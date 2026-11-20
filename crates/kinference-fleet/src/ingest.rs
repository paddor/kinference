//! Per-worker ZMQ ingest listener for vLLM KV-cache events.
//!
//! One [`spawn_worker_listener`] task per worker. Each task owns its sockets,
//! parses vLLM's wire-format events through `dynamo_kv_events::zmq_wire`, and
//! feeds them into the pool's `KvIndexer` (radix tree) and our bookkeeping
//! (`InflightPrefixes`, `FleetMetrics`, `WorkerRegistry`), all reached
//! through the [`WorkerContext`], in `apply_frame`.
//!
//! Modelled after dynamo's `standalone_indexer` listener
//! (`lib/kv-router/src/standalone_indexer/listener.rs`): the SUB socket and
//! the DEALER replay socket live in one task, and gap recovery is synchronous
//! and inline. When a live frame arrives with `seq > expected`, the loop
//! requests a replay, applies every replayed event in order, then applies the
//! live frame. Since gaps are only detected from a frame in hand, the
//! listener never waits for a seq the publisher has already passed.
//!
//! Sequence-cursor semantics come from
//! [`dynamo_kv_events::cursor::CursorState`].
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::{Duration, Instant};

use dynamo_kv_events::cursor::{CursorObservation, CursorState};
use dynamo_kv_events::event::{KvCacheEventData, RouterEvent};
use dynamo_kv_events::zmq_wire::{
    KvCacheSpecKind, KvEventBatch, RawKvEvent, ZmqEventFilterReason, ZmqEventNormalizer,
    convert_event, decode_event_batch,
};
use dynamo_kv_indexer::host::KvIndexerInterface;
use dynamo_router_protocol::worker::WorkerWithDpRank;
use omq_tokio::{KeepAlive, Message, Options, Payload, ReconnectPolicy, Socket, SocketType};

use crate::error::Error;
use crate::metrics::IngestEvent;
use crate::pool::WorkerContext;
use crate::workers::{AttentionKind, IndexedGroup};

const REPLAY_SEND_TIMEOUT: Duration = Duration::from_secs(3);

/// Default wall-clock budget for one `replay_gap` call, when
/// `replay_timeout_ms` is unset.
pub const DEFAULT_REPLAY_TIMEOUT: Duration = Duration::from_millis(60_000);

/// Ingest settings. The two switches come from the environment and the
/// timeout from the top-level `replay_timeout_ms` key; `Config::load` fills
/// all three once at startup.
#[derive(Debug, Clone, Copy)]
pub struct IngestConfig {
    /// `KINFERENCE_DISABLE_INGEST`: a diagnostic switch that skips ZMQ ingest
    /// entirely. Workers are still marked ready, but the radix tree stays
    /// empty.
    pub disabled: bool,
    /// `KINFERENCE_ENABLE_REPLAY`: fetch event history over the DEALER socket
    /// and recover gaps. Off by default because the replay loop contends with
    /// tokenization for tail latency, and because replayed history can be
    /// stale when the engine's KV cache was cleared without a restart,
    /// producing phantom cache hits.
    pub replay: bool,
    /// Wall-clock budget for a single `replay_gap` call; events left
    /// unreplayed are picked up by later gap detection.
    pub replay_timeout: Duration,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            disabled: false,
            replay: false,
            replay_timeout: DEFAULT_REPLAY_TIMEOUT,
        }
    }
}

/// Spawn the ingest listener for a single worker. The listener feeds the
/// pool's `KvIndexer` and inflight prefixes through `ctx`.
pub fn spawn_worker_listener(ctx: WorkerContext, ingest: IngestConfig) {
    let cfg = &ctx.cfg;
    if ingest.disabled {
        tracing::warn!(
            worker_id = cfg.worker_id_str,
            "KV-event ingest disabled by KINFERENCE_DISABLE_INGEST"
        );
        ctx.registry.set_initial_sync_complete(cfg.worker_id_num);
        return;
    }

    // No publish socket declared (e.g. the `InferenceWorkerSet` omits
    // `spec.kvEvents`): these engines don't publish. Mark the worker ready and
    // route on load alone, without a prefix signal.
    if cfg.zmq_endpoint.as_deref().unwrap_or("").is_empty() {
        tracing::debug!(
            worker_id = cfg.worker_id_str,
            "no KV-event endpoint; routing on load alone"
        );
        ctx.registry.set_initial_sync_complete(cfg.worker_id_num);
        return;
    }

    tokio::spawn(async move {
        // Whether the previous attempt failed before it ran for
        // `LISTENER_HEALTHY_AFTER`, so a listener that keeps failing warns
        // once.
        let mut failing = false;
        let mut memory = WorkerMemory::default();
        loop {
            if ctx.cancel.is_cancelled() {
                return;
            }
            let started = Instant::now();
            if let Err(err) = run_listener(&ctx, &ingest, &mut memory).await {
                if started.elapsed() >= LISTENER_HEALTHY_AFTER {
                    failing = false;
                }
                if !failing {
                    tracing::warn!(
                        worker_id = ctx.cfg.worker_id_str,
                        endpoint = ?ctx.cfg.zmq_endpoint,
                        error = &err as &dyn StdError,
                        "KV-event listener failed; reconnecting"
                    );
                }
                failing = true;
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    });
}

/// What ingest remembers about one worker across reconnects. The radix tree
/// keeps the worker's blocks across a reconnect, so this state must too.
struct WorkerMemory {
    /// Whether the tree holds a stored block of this worker. Until then
    /// the tree holds nothing that a `BlockRemoved` event could remove.
    stored_indexed: bool,
    /// The one group whose events ingest indexes, for every data parallel
    /// rank of the worker.
    indexed: Option<IndexedGroup>,
    /// Filters events by attention kind only. `convert_event` gets the
    /// block size with each event. The normalizer holds the kind of each
    /// group, which a `BlockRemoved` event may not name.
    normalizer: ZmqEventNormalizer,
    /// Caps dynamo's own warnings about blocks that it can't convert.
    convert_warnings: Arc<AtomicU32>,
    warned_implausible_block_size: bool,
}

impl Default for WorkerMemory {
    fn default() -> Self {
        Self {
            stored_indexed: false,
            indexed: None,
            normalizer: ZmqEventNormalizer::new(0),
            convert_warnings: Arc::default(),
            warned_implausible_block_size: false,
        }
    }
}

/// Per-listener mutable state: the cursor (for gap detection) and the
/// worker's memory.
struct ListenerState<'a> {
    cursor: CursorState,
    memory: &'a mut WorkerMemory,
    /// Which warnings this listener has logged, so each logs once.
    warned: ListenerWarnings,
}

#[derive(Default)]
struct ListenerWarnings {
    frame_shape: bool,
    decode: bool,
    gap: bool,
    replay: bool,
}

/// Why a `replay_gap` call ended without delivering everything
/// requested. Ported from dynamo's `standalone_indexer::listener`
/// (`ReplayRecoveryFailureReason`).
#[derive(Clone, Copy, Debug)]
enum ReplayFailureReason {
    /// Sentinel arrived before any frame did. Publisher had nothing
    /// in retention for the requested range.
    Empty,
    /// First replayed frame's seq was past `start_seq`. Publisher
    /// already evicted the head of the requested range.
    StartedAfterRequested,
    /// Sequence numbers in the replayed stream weren't contiguous.
    /// Either the publisher dropped some, or HWM truncated us.
    NonContiguous,
    /// Last replayed seq was below `end_seq - 1`. Stream ended (via
    /// sentinel or socket close) before reaching the target.
    EndedBeforeTarget,
    /// Local wall budget exhausted before the publisher's sentinel
    /// arrived. Not in dynamo's enum, which has no such timeout.
    WallBudgetExhausted,
}

impl ReplayFailureReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::StartedAfterRequested => "started_after_requested",
            Self::NonContiguous => "non_contiguous",
            Self::EndedBeforeTarget => "ended_before_target",
            Self::WallBudgetExhausted => "wall_budget_exhausted",
        }
    }
}

/// Per-replay-call tracker, used at exit to decide whether to warn that the
/// replay was incomplete, and why. Ported from dynamo's
/// `ReplayRecoveryProgress`.
struct ReplayProgress {
    start_seq: u64,
    end_seq: u64,
    first_replayed: Option<u64>,
    last_replayed: Option<u64>,
    expected_next: u64,
    replayed: u64,
    non_contiguous: bool,
}

impl ReplayProgress {
    fn new(start_seq: u64, end_seq: u64) -> Self {
        Self {
            start_seq,
            end_seq,
            first_replayed: None,
            last_replayed: None,
            expected_next: start_seq,
            replayed: 0,
            non_contiguous: false,
        }
    }

    fn record(&mut self, seq: u64) {
        if self.first_replayed.is_none() {
            self.first_replayed = Some(seq);
        }
        if seq != self.expected_next {
            self.non_contiguous = true;
        }
        self.expected_next = seq.saturating_add(1);
        self.last_replayed = Some(seq);
        self.replayed += 1;
    }

    fn failure_reason(&self, wall_budget_hit: bool) -> Option<ReplayFailureReason> {
        if self.start_seq >= self.end_seq {
            return None;
        }
        if wall_budget_hit {
            return Some(ReplayFailureReason::WallBudgetExhausted);
        }
        if self.replayed == 0 {
            return Some(ReplayFailureReason::Empty);
        }
        if self
            .first_replayed
            .is_some_and(|first| first > self.start_seq)
        {
            return Some(ReplayFailureReason::StartedAfterRequested);
        }
        if self.non_contiguous {
            return Some(ReplayFailureReason::NonContiguous);
        }
        if self
            .last_replayed
            .is_some_and(|last| last < self.end_seq - 1)
        {
            return Some(ReplayFailureReason::EndedBeforeTarget);
        }
        None
    }
}

/// Run a single listener until it errors or `cancel` fires. All gap recovery
/// happens inside the SUB-recv branch, synchronously, before the triggering
/// frame is applied.
async fn run_listener(
    ctx: &WorkerContext,
    ingest: &IngestConfig,
    memory: &mut WorkerMemory,
) -> Result<(), Error> {
    let cfg = &ctx.cfg;
    let workers = &ctx.registry;
    let cancel = &ctx.cancel;
    let worker_id_num = cfg.worker_id_num;
    let worker_id_str = cfg.worker_id_str.clone();
    let replay_wall_budget = ingest.replay_timeout;

    let live_endpoint = cfg.zmq_endpoint.clone().ok_or(Error::NoZmqEndpoint)?;
    let replay_endpoint = cfg.replay_endpoint.clone();

    let sub = connect_zmq(SocketType::Sub, &live_endpoint).await?;
    // With replay off, live ingest still flows, but history is never fetched
    // and gaps aren't recovered.
    let replay = if !ingest.replay {
        None
    } else {
        match replay_endpoint.as_deref() {
            Some(endpoint) => Some(connect_zmq(SocketType::Dealer, endpoint).await?),
            None => {
                tracing::warn!(
                    worker_id = worker_id_str,
                    "KINFERENCE_ENABLE_REPLAY is set but the worker has no replay endpoint"
                );
                None
            }
        }
    };

    let mut state = ListenerState {
        cursor: CursorState::Initial,
        memory,
        warned: ListenerWarnings::default(),
    };
    // Whether this listener has reported that events are flowing, so the
    // registry write happens once.
    let mut observed_events = false;
    // Liveness accounting for the heartbeat. `frames_seen` counts every
    // message, including ones dropped for shape.
    let mut frames_seen: u64 = 0;
    let mut events_applied: u64 = 0;
    let listener_started = Instant::now();
    let mut warned_idle = false;
    let mut heartbeat = tokio::time::interval(SUB_HEARTBEAT_IVL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick.
    heartbeat.tick().await;

    // Mark ready as soon as the sockets are connected, as dynamo does. The
    // radix tree fills in lazily: the first live frame triggers a bounded
    // `replay_gap(0, got)` for the history (bounded, since only bounded
    // requests get a sentinel). On an idle cluster the tree stays empty until
    // the first event, which is harmless.
    workers.set_initial_sync_complete(worker_id_num);
    tracing::debug!(
        worker_id = worker_id_str,
        endpoint = live_endpoint,
        replay = replay_endpoint.as_deref().unwrap_or("-"),
        "KV-event listener ready"
    );

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            // ZMQ `connect` succeeds with no peer, so report silence, which
            // can mean a wrong address as well as an engine publishing
            // nothing.
            _ = heartbeat.tick() => {
                let idle = listener_started.elapsed();
                // Warn once only, since a healthy fleet serving cached
                // prefixes can also be silent.
                if frames_seen == 0 && !warned_idle && idle >= SUB_IDLE_WARN_AFTER {
                    warned_idle = true;
                    tracing::warn!(
                        worker_id = worker_id_str,
                        endpoint = live_endpoint,
                        "no KV events received yet"
                    );
                } else {
                    tracing::trace!(
                        worker_id = worker_id_str,
                        endpoint = live_endpoint,
                        frames_seen,
                        events_applied,
                        idle_secs = idle.as_secs(),
                        "KV-event listener heartbeat"
                    );
                }
                continue;
            }
            recv = sub.recv() => {
                let msg = recv.map_err(zmq("receive live events"))?;
                tokio::task::consume_budget().await;
                frames_seen += 1;
                let (seq, payload) = match parse_seq_payload(&msg) {
                    Some(p) => p,
                    None => {
                        if !std::mem::replace(&mut state.warned.frame_shape, true) {
                            tracing::warn!(
                                worker_id = worker_id_str,
                                frames = msg.len(),
                                "dropping a KV-event frame with an unexpected shape"
                            );
                        }
                        continue;
                    }
                };
                // A well-formed frame is the only proof the event stream is
                // real.
                events_applied += 1;
                if !observed_events {
                    observed_events = true;
                    workers.note_kv_events_observed(worker_id_num);
                }
                handle_observation(
                    ctx,
                    &mut state,
                    seq,
                    payload,
                    replay.as_ref(),
                    replay_wall_budget,
                )
                .await;
            }
        }
    }
}

/// Apply a single live SUB frame, recovering any sequence gap first. Mirrors
/// dynamo's `ListenerLoop::handle_message` / `handle_gap`.
async fn handle_observation(
    ctx: &WorkerContext,
    state: &mut ListenerState<'_>,
    seq: u64,
    payload: &[u8],
    replay: Option<&Socket>,
    replay_wall_budget: Duration,
) {
    let worker_id_str = &ctx.cfg.worker_id_str;
    let replay_start = match state.cursor.observe(seq) {
        CursorObservation::Initial { got } if got > 0 => {
            // First frame, at a non-zero seq: drain `[0, got)` first.
            tracing::debug!(
                worker_id = worker_id_str,
                start_seq = 0,
                end_seq = got,
                "replaying KV events up to the first live one"
            );
            Some(0)
        }
        CursorObservation::Gap { expected, got } => {
            if !std::mem::replace(&mut state.warned.gap, true) {
                tracing::warn!(
                    worker_id = worker_id_str,
                    expected,
                    got,
                    gap = got.saturating_sub(expected),
                    "gap in event stream; inline replay"
                );
            } else {
                tracing::debug!(
                    worker_id = worker_id_str,
                    expected,
                    got,
                    gap = got.saturating_sub(expected),
                    "gap in event stream; inline replay"
                );
            }
            Some(expected)
        }
        CursorObservation::Stale { got, last_applied } => {
            // Already applied.
            tracing::trace!(
                worker_id = worker_id_str,
                got,
                ?last_applied,
                "dropping a stale KV-event frame"
            );
            return;
        }
        CursorObservation::Initial { .. } | CursorObservation::Contiguous { .. } => None,
    };
    if let (Some(dealer), Some(start)) = (replay, replay_start) {
        replay_gap(ctx, state, dealer, start, seq, replay_wall_budget).await;
        if ctx.cancel.is_cancelled() {
            return;
        }
    }

    apply_frame(ctx, state, seq, payload).await;
    state.cursor = state.cursor.advance_to(seq);
}

/// Drain `[start_seq, end_seq)` from the DEALER replay socket, applying each
/// frame.
///
/// `end_seq` must come from a live frame: for an open-ended request the
/// publisher tails forever and never sends the sentinel.
///
/// Ported from dynamo's `ListenerLoop::replay_gap`.
async fn replay_gap(
    ctx: &WorkerContext,
    state: &mut ListenerState<'_>,
    dealer: &Socket,
    start_seq: u64,
    end_seq: u64,
    replay_wall_budget: Duration,
) {
    let worker_id_str = &ctx.cfg.worker_id_str;
    // Request: [empty_delim, start_seq_be_u64]. The publisher's
    // ROUTER prepends our identity and replies with batches of
    // [identity_or_delim, seq_be_u64, payload]. An empty payload
    // signals end-of-replay.
    let start = start_seq.to_be_bytes();
    let req = Message::multipart_payloads([Payload::new(), Payload::from_slice(&start)]);
    let sent = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return,
        result = tokio::time::timeout(REPLAY_SEND_TIMEOUT, dealer.send(req)) => result,
    };
    if let Err(err) = sent.unwrap_or(Err(omq_tokio::Error::Timeout)) {
        if !std::mem::replace(&mut state.warned.replay, true) {
            tracing::warn!(
                worker_id = worker_id_str,
                start_seq,
                end_seq,
                error = &err as &dyn StdError,
                "replay request failed"
            );
        }
        return;
    }

    let started = std::time::Instant::now();
    let mut progress = ReplayProgress::new(start_seq, end_seq);
    let mut wall_budget_hit = false;
    let mut sentinel_seen = false;

    loop {
        let remaining = replay_wall_budget.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            wall_budget_hit = true;
            break;
        }
        let received = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return,
            result = tokio::time::timeout(remaining, dealer.recv()) => result,
        };
        let msg = match received {
            Ok(Ok(msg)) => msg,
            Err(_) => {
                wall_budget_hit = true;
                break;
            }
            Ok(Err(err)) => {
                if !std::mem::replace(&mut state.warned.replay, true) {
                    tracing::warn!(
                        worker_id = worker_id_str,
                        start_seq,
                        end_seq,
                        replayed = progress.replayed,
                        error = &err as &dyn StdError,
                        "replay receive failed"
                    );
                }
                break;
            }
        };

        // Replay batches are [identity_or_delim, seq_be_u64, payload].
        if msg.len() < 3 {
            tracing::trace!(
                worker_id = worker_id_str,
                frames = msg.len(),
                "dropping a replay frame with an unexpected shape"
            );
            continue;
        }
        let payload = &msg[2];
        if payload.is_empty() {
            sentinel_seen = true;
            break;
        }
        let Ok(seq_bytes) = <[u8; 8]>::try_from(&msg[1]) else {
            tracing::trace!(
                worker_id = worker_id_str,
                len = msg[1].len(),
                "dropping a replay frame with a malformed sequence number"
            );
            continue;
        };
        let seq = u64::from_be_bytes(seq_bytes);

        // The cursor's `Stale` check guards against duplicate replayed
        // events.
        match state.cursor.observe(seq) {
            CursorObservation::Stale { .. } => {
                continue;
            }
            CursorObservation::Initial { .. } | CursorObservation::Contiguous { .. } => {}
            CursorObservation::Gap { .. } => {
                // Ahead of the cursor: accept it; a later observation
                // retries the gap.
            }
        }

        apply_frame(ctx, state, seq, payload).await;
        state.cursor = state.cursor.advance_to(seq);
        progress.record(seq);

        // Yield every 32 events, so the replay loop doesn't starve other
        // tasks such as tokenization. 32 was picked empirically.
        if progress.replayed.is_multiple_of(32) {
            tokio::task::yield_now().await;
        }

        // Caught up to `end_seq`: the rest will arrive live.
        if seq + 1 >= end_seq {
            break;
        }
    }

    let elapsed_ms = started.elapsed().as_millis() as u64;
    match progress.failure_reason(wall_budget_hit) {
        Some(reason) => {
            if !std::mem::replace(&mut state.warned.replay, true) {
                tracing::warn!(
                    worker_id = worker_id_str,
                    start_seq,
                    end_seq,
                    first_replayed = ?progress.first_replayed,
                    last_replayed = ?progress.last_replayed,
                    replayed = progress.replayed,
                    reason = reason.as_str(),
                    sentinel_seen,
                    elapsed_ms,
                    "replay incomplete"
                );
            }
        }
        None => {
            tracing::debug!(
                worker_id = worker_id_str,
                start_seq,
                end_seq,
                replayed = progress.replayed,
                sentinel_seen,
                elapsed_ms,
                "replay complete"
            );
        }
    }
}

/// Extract `(seq, payload)` from a 3-frame ZMQ message
/// `[topic_or_identity, seq_be_u64, payload]`. Returns `None` if the
/// shape is malformed; the caller logs and drops.
fn parse_seq_payload(msg: &Message) -> Option<(u64, &[u8])> {
    if msg.len() != 3 {
        return None;
    }
    let seq_bytes = <[u8; 8]>::try_from(&msg[1]).ok()?;
    Some((u64::from_be_bytes(seq_bytes), &msg[2]))
}

// --- Bookkeeping ------------------------------------------------------

/// Decode one ZMQ payload and apply each event to the radix tree,
/// [`InflightPrefixes::on_stored`] and the ingest metrics. An event that
/// ingest can't index is counted and skipped; no event stops the listener.
async fn apply_frame(ctx: &WorkerContext, state: &mut ListenerState<'_>, seq: u64, payload: &[u8]) {
    let worker_id_str = &ctx.cfg.worker_id_str;
    let batch = match decode_event_batch(payload) {
        Ok(b) => b,
        Err(err) => {
            if !std::mem::replace(&mut state.warned.decode, true) {
                tracing::warn!(
                    worker_id = worker_id_str,
                    seq,
                    error = &err as &dyn StdError,
                    "failed to decode a KV-event batch"
                );
            }
            return;
        }
    };

    apply_batch(ctx, state, seq, batch).await;
}

async fn apply_batch(
    ctx: &WorkerContext,
    state: &mut ListenerState<'_>,
    seq: u64,
    batch: KvEventBatch,
) {
    let worker_id_num = ctx.cfg.worker_id_num;
    let worker_id_str = &ctx.cfg.worker_id_str;
    let effective_dp_rank = batch
        .data_parallel_rank
        .map_or(0u32, |rank| rank.cast_unsigned());

    let worker = WorkerWithDpRank::new(worker_id_num, effective_dp_rank);
    for raw_event in batch.events {
        // The normalizer filters the events of non-main attention groups
        // first, so their blocks never set the worker's block size.
        let raw_event = match state
            .memory
            .normalizer
            .preprocess_with_reason(raw_event, worker)
        {
            Ok(raw_event) => raw_event,
            Err(reason) => {
                // The other reasons name events that the index never holds
                // by design, so only these two count as a skip.
                if matches!(
                    reason,
                    ZmqEventFilterReason::UnknownKind | ZmqEventFilterReason::UnlearnedGroupIdx
                ) {
                    ctx.prom
                        .record_kv_event_skipped(&ctx.pool_name, reason.as_label());
                }
                continue;
            }
        };
        let skip = match &raw_event {
            RawKvEvent::BlockStored {
                block_size,
                group_idx,
                kv_cache_spec_kind,
                ..
            } => {
                let group_idx = group_idx.unwrap_or(0);
                select_group(
                    ctx,
                    state.memory,
                    group_idx,
                    *kv_cache_spec_kind,
                    *block_size,
                )
                .await
            }
            RawKvEvent::BlockRemoved { group_idx, .. } => match state.memory.indexed {
                Some(g) if g.group_idx != group_idx.unwrap_or(0) => Some(SKIP_UNINDEXED_GROUP),
                _ if !state.memory.stored_indexed => Some(SKIP_NOTHING_STORED),
                _ => None,
            },
            _ => None,
        };
        if let Some(reason) = skip {
            ctx.prom.record_kv_event_skipped(&ctx.pool_name, reason);
            continue;
        }
        // Only a `BlockStored` event reads the block size, and `select_group`
        // has set one by then.
        let block_size = state.memory.indexed.map_or(0, |g| g.block_size);
        let Some(placement_event) = convert_event(
            raw_event,
            seq,
            block_size,
            worker,
            &state.memory.convert_warnings,
            None,
            None,
        ) else {
            continue;
        };
        let Some(router_event) = placement_event.into_router_event() else {
            continue;
        };
        log_event(worker_id_str, seq, &router_event);
        let (ev_kind, n_blocks) = match &router_event.event.data {
            KvCacheEventData::Stored(d) => {
                // Notify the pending-prefix tracker before the indexer.
                // Once a dispatch's blocks are all stored it drops out, and
                // the radix tree is the sole signal.
                //
                // Match on `tokens_hash` (Dynamo's XXH3 over the
                // event's token ids), not `block_hash` (the engine's hash):
                // `InflightPrefixes` is populated from
                // `compute_block_hash_for_seq` in `score_workers`, which uses
                // the same XXH3.
                let stored_hashes: Vec<u64> = d.blocks.iter().map(|b| b.tokens_hash.0).collect();
                ctx.inflight_prefixes
                    .on_stored(&ctx.endpoint_key, &stored_hashes);
                state.memory.stored_indexed |= !d.blocks.is_empty();
                (IngestEvent::Stored, d.blocks.len() as u64)
            }
            KvCacheEventData::Removed(d) => (IngestEvent::Removed, d.block_hashes.len() as u64),
            KvCacheEventData::Cleared => {
                clear_worker_blocks(ctx).await;
                continue;
            }
        };
        ctx.prom.record_index_event(ev_kind, n_blocks);
        ctx.indexer.apply_event(router_event).await;
    }
}

/// The `reason` label for a `BlockStored` event of the worker's indexed
/// group at a smaller block size than the worker's. vLLM publishes one when
/// a prompt ends inside a block.
const SKIP_BLOCK_SIZE_MISMATCH: &str = "block_size_mismatch";

/// The `reason` label for an event of a main attention group that ingest
/// doesn't index for the worker.
const SKIP_UNINDEXED_GROUP: &str = "unindexed_attention_group";

/// The `reason` label for a `BlockStored` event whose block size is 0 or
/// above [`MAX_PLAUSIBLE_BLOCK_SIZE`].
const SKIP_IMPLAUSIBLE_BLOCK_SIZE: &str = "implausible_block_size";

/// The `reason` label for a `BlockRemoved` event from a worker that has no
/// indexed block.
const SKIP_NOTHING_STORED: &str = "nothing_stored";

/// KV block sizes are small (SGLang page-size 1, vLLM 16, Gemma 256). A
/// larger size is a misframed message, or a hybrid model's Mamba state block
/// (e.g. 1568 tokens) from a vLLM build that doesn't tag events with
/// `kv_cache_spec_kind`.
const MAX_PLAUSIBLE_BLOCK_SIZE: usize = 1024;

/// What ingest does with a `BlockStored` event of a main attention group.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// Index the event at the worker's block size.
    Index,
    Skip(&'static str),
    /// The event's group and block size become the worker's.
    Select,
}

/// The indexed group is the group with the lowest `group_idx` that has sent
/// a `BlockStored` event, at the largest block size that the group has
/// stated. vLLM names the blocks of every group from one chain of engine
/// hashes, so the tree can hold one group per worker.
fn classify(indexed: Option<IndexedGroup>, group_idx: u32, block_size: u32) -> Verdict {
    let Some(g) = indexed else {
        return Verdict::Select;
    };
    if group_idx > g.group_idx {
        return Verdict::Skip(SKIP_UNINDEXED_GROUP);
    }
    if group_idx < g.group_idx || block_size > g.block_size {
        return Verdict::Select;
    }
    if block_size < g.block_size {
        return Verdict::Skip(SKIP_BLOCK_SIZE_MISMATCH);
    }
    Verdict::Index
}

/// Decide whether ingest indexes a `BlockStored` event of a main attention
/// group, and return the skip reason when it doesn't. A change of the
/// indexed group or of the block size first clears the worker's blocks.
async fn select_group(
    ctx: &WorkerContext,
    memory: &mut WorkerMemory,
    group_idx: u32,
    kind: Option<KvCacheSpecKind>,
    block_size: usize,
) -> Option<&'static str> {
    if block_size == 0 || block_size > MAX_PLAUSIBLE_BLOCK_SIZE {
        if !std::mem::replace(&mut memory.warned_implausible_block_size, true) {
            tracing::warn!(
                workerset = ctx.pool_name,
                worker_id = ctx.cfg.worker_id_str,
                block_size,
                "skipping KV events with an implausible block size"
            );
        }
        return Some(SKIP_IMPLAUSIBLE_BLOCK_SIZE);
    }
    // Lossless: the size is at most `MAX_PLAUSIBLE_BLOCK_SIZE`.
    let block_size = block_size as u32;
    match classify(memory.indexed, group_idx, block_size) {
        Verdict::Index => return None,
        Verdict::Skip(reason) => return Some(reason),
        Verdict::Select => {}
    }

    // The worker's blocks belong to the old group or the old block size.
    if memory.indexed.is_some() {
        clear_worker_blocks(ctx).await;
        memory.stored_indexed = false;
    }
    let group = IndexedGroup {
        group_idx,
        kind: kind.and_then(attention_kind),
        block_size,
    };
    memory.indexed = Some(group);
    ctx.registry.set_indexed_group(ctx.cfg.worker_id_num, group);
    tracing::info!(
        workerset = ctx.pool_name,
        worker_id = ctx.cfg.worker_id_str,
        group_idx,
        kind = group.kind.map(AttentionKind::as_str),
        block_size,
        "indexed group selected"
    );
    None
}

/// Clear every rank of the worker. Removals and events share the indexer's
/// mutation queue, so a subsequent `BlockStored` remains ordered behind this.
async fn clear_worker_blocks(ctx: &WorkerContext) {
    ctx.prom.record_index_event(IngestEvent::Cleared, 0);
    ctx.indexer.remove_worker(ctx.cfg.worker_id_num).await;
}

fn attention_kind(kind: KvCacheSpecKind) -> Option<AttentionKind> {
    match kind {
        KvCacheSpecKind::FullAttention => Some(AttentionKind::Full),
        KvCacheSpecKind::MlaAttention => Some(AttentionKind::Mla),
        KvCacheSpecKind::SinkFullAttention => Some(AttentionKind::SinkFull),
        _ => None,
    }
}

fn log_event(worker_id_str: &str, seq: u64, router_event: &RouterEvent) {
    match &router_event.event.data {
        KvCacheEventData::Stored(data) => {
            let parent_hash = data.parent_hash.map(|h| h.0);
            for (idx, block) in data.blocks.iter().enumerate() {
                tracing::trace!(
                    worker_id = worker_id_str,
                    seq,
                    event = "stored",
                    block_idx = idx,
                    block_hash = block.block_hash.0,
                    tokens_hash = block.tokens_hash.0,
                    parent_hash = ?parent_hash,
                    mm = block.mm_extra_info.is_some(),
                    "block stored"
                );
            }
        }
        KvCacheEventData::Removed(data) => {
            for h in &data.block_hashes {
                tracing::trace!(
                    worker_id = worker_id_str,
                    seq,
                    event = "removed",
                    block_hash = h.0,
                    "block removed"
                );
            }
        }
        KvCacheEventData::Cleared => {
            tracing::trace!(
                worker_id = worker_id_str,
                seq,
                event = "cleared",
                "all blocks cleared"
            );
        }
    }
}

// --- ZMQ socket setup --------------------------------------------------

/// How often the KV-event listener re-checks its own liveness. Cheap; the
/// reporting below is what decides whether anything is said.
const SUB_HEARTBEAT_IVL: Duration = Duration::from_secs(60);
/// How long a listener must have received nothing before it warns, once.
/// KV events fire only on newly stored blocks, so a healthy fleet can be
/// silent, but so is a wrong endpoint.
const SUB_IDLE_WARN_AFTER: Duration = Duration::from_secs(300);
/// A listener that ran this long before failing counts as having recovered,
/// so its next failure warns again.
const LISTENER_HEALTHY_AFTER: Duration = Duration::from_secs(60);
fn zmq(op: &'static str) -> impl Fn(omq_tokio::Error) -> Error {
    move |source| Error::Zmq { op, source }
}

async fn connect_zmq(kind: SocketType, endpoint: &str) -> Result<Socket, Error> {
    let mut options = Options::default()
        .tcp_keepalive(KeepAlive::Enabled {
            idle: Duration::from_secs(60),
            intvl: Duration::from_secs(30),
            cnt: 3,
        })
        .heartbeat_interval(Duration::from_secs(10))
        .heartbeat_timeout(Duration::from_secs(20))
        .reconnect(ReconnectPolicy::Exponential {
            min: Duration::from_millis(100),
            max: Duration::from_secs(5),
        });
    if kind == SocketType::Sub {
        options = options.recv_hwm(100_000);
    }
    let socket = Socket::new(kind, options);
    if kind == SocketType::Sub {
        socket
            .subscribe(b"".as_slice())
            .await
            .map_err(zmq("subscribe to live events"))?;
    }
    socket.connect(endpoint).await.map_err(zmq("connect"))?;
    Ok(socket)
}

#[cfg(test)]
mod tests;
