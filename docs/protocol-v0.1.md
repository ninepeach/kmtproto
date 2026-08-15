# KMTProto Chat Protocol v0.1

## Purpose and scope

KMTProto v0.1 is an independent, lightweight, reliable, and recoverable real-time chat protocol core. It defines protocol semantics, frames, state transitions, reliability, heartbeat, resume, replay, validation, and encoding. It does not define business messages, accounts, rooms, authorization, user interfaces, network servers, or a persistence technology.

The design borrows reliability ideas from Telegram/MTProto but is not an MTProto implementation.

## Wire envelope

```go
type Envelope struct {
    V         uint16          `json:"v"`
    Type      FrameType       `json:"type"`
    ID        string          `json:"id,omitempty"`
    SessionID string          `json:"session_id,omitempty"`
    Seq       uint64          `json:"seq,omitempty"`
    Timestamp int64           `json:"timestamp,omitempty"`
    Payload   json.RawMessage `json:"payload,omitempty"`
}
```

`seq=0` means unset. Only `EVENT` consumes sequence numbers, beginning at 1. Timestamp is diagnostic metadata only; it must never determine ordering, causality, deduplication, or resume position.

The fixed v0.1 frame set is `HELLO`, `WELCOME`, `PING`, `PONG`, `SEND`, `ACK`, `EVENT`, `RESUME`, and `ERROR`.

## Frame matrix

| Frame | Direction | Required identity | Sequence | Replayable |
|---|---|---|---:|---|
| `HELLO` | Client → Server | none | 0 | no |
| `WELCOME` | Server → Client | `session_id` | 0 | no |
| `PING` / `PONG` | both | `session_id`, payload `ping_id` | 0 | no |
| `SEND` | Client → Server | `session_id`, `id` | 0 | no |
| `ACK` | Server → Client | `session_id`, payload `ref_id` | 0 | no |
| `EVENT` | Server → Client | `session_id`, `id` | ≥ 1 | yes |
| `RESUME` | Client → Server | `session_id`, payload `last_seq` | 0 | no |
| `ERROR` | Server → Client | payload `code` | 0 | no |

Envelope and protocol payloads are strictly decoded by default. `SEND.content` and `EVENT.content` are application-owned JSON and remain semantically opaque.

## Sessions and connection generations

A Session is a logical protocol context that can outlive multiple physical transports. It is not a user, account, room, conversation, TCP connection, or WebSocket connection.

Every newly established transport increments `ConnectionGeneration`. Incoming work associated with any older generation returns `ErrStaleConnection` and cannot change active state, heartbeat, session position, replay state, or the active outbound queue.

Client states are `DISCONNECTED`, `CONNECTING`, `CONNECTED`, `HANDSHAKING`, `RESUMING`, `READY`, and `SUSPECT`. Unknown numeric enum values stringify as `UNKNOWN` and never panic.

## SEND, ACK, and idempotency

`SEND` is a reliable logical submission. The client first stores the complete frame in its outbox, sends it, and removes it only after a correlated `ACK`. A timeout retries the exact same frame and message ID.

`ACK` means the server crossed its reliable acceptance boundary and guarantees that a legal retry of `(session_id, msg_id)` will not create a second logical submission. It does not mean delivered, read, externally completed, or answered by a human.

The server atomically changes a missing dedup record to `PROCESSING` via `Claim`. Only the winner may call the application. `Complete` stores the logical ACK; a completed duplicate returns that stored ACK. A processing duplicate waits for the in-process winner where possible and never starts a second application call.

The message ID is always passed to `ApplicationHandler` as `idempotencyKey`. This is essential for the crash window:

```text
application commit → gateway crash → dedup Complete not stored → client retry
```

Protocol dedup plus application idempotency provides at-most-once logical commit within the configured window. The protocol alone cannot provide arbitrary cross-service exactly-once side effects.

The bundled client outbox is in-memory. It survives transport replacement, not client-process termination.

## EVENT sequence and identity

Each Session owns one ordered EVENT stream. `(session_id, seq)` is an ordered position and must forever map to exactly one `event_id`.

- `incoming == last_seq + 1`: accept and deliver.
- `incoming <= last_seq`, same event ID: safe duplicate; ignore.
- `incoming <= last_seq`, different event ID: protocol violation.
- `incoming > last_seq + 1`: gap; enter `RESUMING`, stop delivery, and send `RESUME(last_seq)`.

v0.1 intentionally has no out-of-order buffer.

## Resume and fixed replay boundary

The server handles `RESUME(last_seq=N)` by atomically serializing the following work with live event publication for that Session:

1. Snapshot `replay_to = current_session_seq`.
2. Load original events `N+1..replay_to`.
3. Enqueue `WELCOME(RESUMED)` and the entire replay as one ordered batch.
4. Allow newly published live events to follow the batch.

The resume acknowledgement contains both boundaries:

```json
{
  "mode": "RESUMED",
  "resume_from": 101,
  "replay_to": 105,
  "server_time": 1786821000180
}
```

`replay_to` is a required v0.1 clarification: without it, the client cannot determine when replay is complete and when returning to `READY` is safe.

The client buffers replay frames internally and does not emit any `DeliverEventAction` until every event through `replay_to` is present. A disconnect during partial replay therefore resumes again from the last position already delivered to the application.

If the requested range predates retained replay data, the server returns non-retryable `SYNC_REQUIRED`. Full synchronization is intentionally outside v0.1. An expired Session returns `INVALID_SESSION`.

## Stream concurrency

Every Session has a serial stream actor. Resume snapshot/storage work and live event append work pass through that owner, so storage I/O is serialized without holding a mutex. Replay and live output therefore cannot interleave.

All frames for a connection enter one `OutboundQueue`. `EnqueueBatch` is atomic with respect to other enqueues, and one `SingleWriter` is the only component allowed to call the byte sender. No protocol-state mutex is held while application code, storage, network I/O, or a user callback runs.

## Heartbeat

Application-level `PING`/`PONG` frames are independent of WebSocket control frames. They do not consume a sequence and are never replayed.

An outstanding ping records ID, monotonic send time, and connection generation. A PONG affects liveness only when both ID and generation match. Timeout moves `READY → SUSPECT`; failure through the configured grace period moves `SUSPECT → DISCONNECTED`. A matching PONG during the grace period may restore `READY`; a PONG from a replaced generation never can.

Timeouts use local duration arithmetic, not wire timestamps or `server_time - client_time`.

## Error behavior

Standard codes are `BAD_REQUEST`, `UNSUPPORTED_VERSION`, `UNAUTHORIZED`, `INVALID_SESSION`, `NOT_FOUND`, `RATE_LIMITED`, `SYNC_REQUIRED`, `INTERNAL`, and `PROTOCOL_VIOLATION`.

- `BAD_REQUEST`, `RATE_LIMITED`: connection may remain open.
- `UNSUPPORTED_VERSION`, `PROTOCOL_VIOLATION`: emit ERROR when possible, then close.
- `INVALID_SESSION` during resume: abandon the Session.
- `SYNC_REQUIRED`: stop automatic resume and notify the application.
- `INTERNAL`: implementation policy; retryability is explicit.

An invalid incoming `ERROR` is never answered with another ERROR; it is ignored or closes the connection to prevent an error loop.

## Limits

The codec checks `MaxFrameSize` before JSON decoding and validates `MaxPayloadSize`, `MaxIDLength`, `MaxSessionIDLength`, and `MaxErrorMessageLength`. Production deployments should tune these values and add transport-level read limits as the first defensive boundary.

## TTL constraints

The server rejects invalid configuration. At minimum:

```text
DedupTTL >= ClientRetryTTL
DedupTTL >= SessionResumeTTL
ReplayTTL >= SessionResumeTTL
```

This prevents a resumable Session from silently outliving its required dedup or replay safety window.

## Hard invariants

1. Every resumable Session has its own EVENT sequence.
2. Only EVENT changes sequence position.
3. Sequence is monotonic and never decreases.
4. One `(session_id, seq)` always maps to one `event_id`.
5. Replay preserves original ID, sequence, payload, and timestamp.
6. One `(session_id, msg_id)` creates at most one logical application submission in the idempotency window.
7. The SEND message ID reaches the application as its idempotency key.
8. ACK is sent only after reliable completion is stored.
9. A gap suspends application delivery until fixed-boundary replay completes.
10. An old generation never mutates the active connection state.
11. All connection output passes one serialization point.
12. No lock spans blocking I/O, storage, application code, or user callbacks.
13. Heartbeat does not participate in EVENT sequencing.
14. Timestamps never determine order or deduplication.
15. Resume begins only after the application-confirmed `last_seq`.

## Reliability boundary

v0.1 guarantees connection-level SEND retry, server duplicate suppression, ordered EVENT delivery, gap detection, bounded Session replay, and heartbeat liveness detection.

It does not guarantee global exactly-once delivery, durable client outbox recovery, permanent offline sync, or cross-service exactly-once side effects unless the caller supplies the necessary durable stores and idempotent application boundary.
