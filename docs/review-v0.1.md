# KMTProto v0.1 Protocol Core Review

Status: pre-change review baseline  
Reviewed branch: `main`  
Reviewed commit: `127dd992b6b0c5e5c410395364e0fa2c6efd61aa`  
Reviewed tree: `f879dfc3a2e6c0070c00007db84d2c2ace392290`

This document records the protocol review before hardening changes are made. It
distinguishes wire/protocol correctness defects from concerns that belong to a
future gateway or server runtime.

## Scope boundary

KMTProto is a transport-independent chat protocol library. Its scope is frame
semantics, validation, protocol state, reliable SEND/ACK behavior, idempotency
contracts, ordered EVENT delivery, gap recovery, resume/replay, heartbeat,
errors, deterministic actions, and reference in-memory helpers.

KMTProto is not a WebSocket or TCP server, gateway, database, distributed
session manager, cluster coordinator, message queue, authentication system, or
business router. The in-memory stores, outbound queue, single writer, and
`ServerConnection` are reference/helper implementations. They are not
production runtime requirements of the wire protocol.

The requested review named files such as `frame.go`, `session.go`,
`connection.go`, and `heartbeat.go`. The current implementation deliberately
consolidates some of those concerns into `types.go`, `client.go`, `server.go`,
and `store.go`. A missing filename is not treated as a missing protocol feature;
the behavior and tests are reviewed instead.

## Requirement traceability

| Design requirement | Current implementation | Test coverage before hardening | Gap |
| --- | --- | --- | --- |
| Exactly nine v0.1 frame types | `types.go` declares HELLO, WELCOME, PING, PONG, SEND, ACK, EVENT, RESUME, ERROR | Codec and validator tests cover known and unknown types | Semantics need a single normative table |
| Envelope and typed protocol payloads | `types.go`, `payload.go` | JSON round trips and frame validation | Presence of zero-valued resume fields is not enforced |
| Strict protocol validation with bounded input | `json_codec.go`, `validate.go`, `limits.go` | Malformed JSON, strict fields, and common size limits | Outbound builders do not consistently validate before mutating state |
| Deterministic client state machine | `client.go` with mutex-protected transitions and actions | Happy paths, gap, resume, heartbeat | State-transition matrix is incomplete; invalid PONG can be silently ignored |
| Server-side protocol state | `ServerConnection` fences generation and serializes output | Generation and serialization tests | HELLO/RESUME/READY frame admission is not represented explicitly |
| Connection-generation fencing | `Client.HandleIncoming` and `ServerConnection.Handle` compare generation | Old EVENT/PONG/ERROR tests | Old WELCOME and state-mutation matrix need explicit tests |
| Reliable SEND and ACK boundary | `Server.handleSend` calls Claim, Application, Complete, then enqueues ACK | SEND, duplicate, failure injection | ACK order is correct; needs an explicit ordering invariant test |
| Atomic `(session_id, msg_id)` dedup | `ServerSessionStore.Claim/Complete/Abort`; `MemoryDedupStore` | Duplicate and concurrent duplicate tests | Active PROCESSING entries can expire, permitting a second execution |
| Deterministic duplicate PROCESSING behavior | Server in-flight registry waits for the local leader | Concurrent duplicate test | Claim occurs before flight registration, leaving a nondeterministic window |
| Application idempotency boundary | `msgID` is passed to `ApplicationHandler` | Handler tests observe ID | End-to-end exactly-once limitation and global uniqueness expectation need clearer docs |
| Session-scoped EVENT sequence | `MemoryReplayStore` allocates per-session sequence | Append/replay tests | High-water sequence is lost when all retained events are pruned |
| Duplicate EVENT identity invariant | Client stores `seq -> eventID` and rejects conflicts | Duplicate/conflict tests | Identity map grows without bound |
| Gap detection without silent advancement | Client enters RESUMING and withholds delivery | Gap tests | Correct today; partial-replay leakage needs stronger testing |
| Fixed resume boundary | Server snapshots current seq and replays through it using a stream lane | Resume boundary tests | Replay count is unbounded; zero boundary field presence is ambiguous |
| READY only after full replay | Client buffers replay and releases it at `replay_to` | Resume tests | Replay buffer is unbounded by event count or bytes |
| Application-level heartbeat | Client pending ping tracks ID and generation; PING/PONG use no seq | Timeout, late PONG, stale generation | Invalid-state PONG behavior needs tightening |
| Structured errors and loop protection | `ErrorPayload`, standard codes, invalid ERROR is not answered with ERROR | Error tests | Close/keep/abandon policy is not centralized or fully tested |
| Transport-independent actions | Client and server produce/enqueue frames; `Codec` interface is independent | Mock queue/writer tests | Helper/core distinction and concurrency contracts need documentation |
| Single outbound serialization point | `OutboundQueue`, `SingleWriter`, server stream lane | Serialization/race tests | Stream-lane goroutine can live forever and a panic can strand waiters |
| Deterministic time | `Clock`, `FakeClock`; heartbeat tests do not sleep | Clock and heartbeat tests | Default session ID closure can capture a real clock before config override |

## Normative wire semantics

All frames use wire version `1`. `timestamp` is diagnostic metadata only and is
never used for ordering, deduplication, resume, causality, RTT, or timeout
correctness. Only EVENT consumes sequence numbers.

| Frame | Required | Forbidden / fixed | Valid receiving state | Expected result | Error behavior |
| --- | --- | --- | --- | --- | --- |
| HELLO | valid `HelloPayload`; optional frame ID | empty `session_id`; `seq == 0` | server connection awaiting handshake | WELCOME with `mode=NEW` | unsupported version closes; malformed request is rejected |
| WELCOME | non-empty `session_id`; valid mode and payload | `seq == 0`; NEW has no replay boundary; RESUMED has both boundaries | client HANDSHAKING for NEW; RESUMING for RESUMED | client becomes READY after NEW or after complete replay | unexpected mode/state is a protocol violation |
| PING | non-empty `session_id`; non-empty `ping_id` | `seq == 0` | ready server session | matching PONG | malformed request is rejected; connection may remain open |
| PONG | non-empty `session_id`; matching `ping_id` | `seq == 0` | client READY or SUSPECT | clears only the matching generation's pending ping | stale generation is ignored; invalid state is rejected |
| SEND | non-empty globally unique ID; non-empty `session_id`; content payload | `seq == 0` | ready server session | ACK only after Claim, Application, and Complete | duplicate completed SEND returns the original logical ACK |
| ACK | non-empty `session_id`; non-empty `ref_id` | `seq == 0` | client READY | removes matching pending SEND | unknown ref does not create a commit or change sequence |
| EVENT | non-empty event ID; non-empty `session_id`; `seq >= 1`; content payload | no sequence gaps | client READY or expected replay position in RESUMING | deliver in order; safe duplicate is ignored | same seq/different ID is a protocol violation; a gap starts resume |
| RESUME | non-empty `session_id`; `last_seq` in payload | `seq == 0` | server connection awaiting handshake | WELCOME RESUMED, then fixed replay interval | INVALID_SESSION abandons session; SYNC_REQUIRED requires full sync |
| ERROR | valid standard code; explicit retryability | `seq == 0` | any active protocol state | deterministic keep/close/abandon action | invalid ERROR is logged/ignored or closes; never answered with ERROR |

The detailed validator remains the executable definition of field limits and
payload shape. Business `SEND.content` and `EVENT.content` remain opaque and
allow application-defined JSON.

## Findings

### Critical

#### C-1: an active PROCESSING dedup record can expire

`MemoryDedupStore.Claim` applies TTL expiry to both PROCESSING and COMPLETED
records. If an application call exceeds the TTL, a duplicate can claim the same
`(session_id, msg_id)` and execute the application a second time.

This is a protocol correctness defect because the in-memory reference store
violates the advertised duplicate-suppression contract. PROCESSING must remain
claimed until `Complete` or explicit `Abort`; TTL may expire completed replay
records.

### High

#### H-1: Claim precedes local in-flight registration

The request leader currently calls `Claim` before `registerFlight`. A duplicate
arriving in that interval sees PROCESSING but cannot find the original local
flight, so it receives a retryable error instead of deterministic binding to the
leader. It does not execute the application twice with the current store, but
the window complicates the concurrency contract and failure behavior.

Register the local flight before Claim. A follower waits for that flight. An
already-PROCESSING record with no local leader remains a retryable condition and
must never call the application.

#### H-2: client replay and EVENT identity memory are unbounded

The client buffers the entire replay before delivery and retains every
`seq -> eventID` mapping for the session. A valid but very large replay can grow
memory without a protocol-level bound, and long-lived sessions grow the identity
map forever.

Add simple count/byte replay limits and a recent identity window. A duplicate
older than the retained identity window must be rejected conservatively, never
silently accepted.

#### H-3: server connection admission state is implicit

`ServerConnection` provides generation fencing and serialized output but does
not model awaiting-handshake versus ready states. Consequently, SEND or PING can
reach the low-level handler before HELLO/RESUME, and a second HELLO can be
accepted after readiness when used through the helper.

Add a minimal reference connection state to enforce frame admission without
taking ownership of transports, registries, or connection lifecycle.

#### H-4: replay high-water is lost when all retained events are pruned

`MemoryReplayStore.CurrentSeq` derives the current sequence from the retained
slice. If pruning removes all events, the reported sequence becomes zero and a
future append can reuse a prior sequence number.

Track the per-session high-water separately from retained replay data. Pruning
must never decrement it.

#### H-5: zero-valued resume boundaries are not wire-explicit

`resume_from` and `replay_to` use `omitempty`. A valid empty-session resume has
`replay_to == 0`, which is omitted, and validation cannot distinguish an omitted
field from an explicit zero. A resumed WELCOME must carry both boundary fields,
including zero.

### Medium

#### M-1: PONG can be silently accepted in an invalid state

PONG handling checks generation and pending ping identity, but not that the
client is READY or SUSPECT. In a different current-generation state, a PONG with
no pending ping is silently ignored. Invalid state/frame combinations should
have deterministic errors.

#### M-2: ERROR disposition is distributed across handlers

The standard codes exist, but retryability, keep-open, close, abandon-session,
and full-sync behavior is not represented by one policy table. This makes
client/server behavior harder to audit and test.

#### M-3: stream-lane worker lifetime and panic behavior

Each used session starts a worker goroutine that has no shutdown path. A panic
inside a lane request can terminate the worker before signaling the caller,
causing a permanent wait. The lane is a reference serialization helper, not a
production session manager, but it must still be safe.

A minimal synchronous queued lane can serialize work without a permanent
goroutine and can convert a panic into an error for the waiting caller.

#### M-4: outbound builders validate inconsistently

Some client commands mutate state or outbox data before the complete generated
frame has passed the same validator used for inbound data. Oversized metadata or
IDs can therefore create local state for a frame that cannot be validly sent.

Build and validate first, then mutate protocol state.

#### M-5: invariant/failure tests are incomplete

Existing tests cover the principal happy paths, duplicate SEND, gap recovery,
and stale frames. They do not explicitly prove ACK-after-Complete ordering,
PROCESSING survival beyond TTL, full-prune sequence monotonicity, bounded replay,
all illegal client transitions, old WELCOME fencing, or repeated partial resume.

#### M-6: server replay result count is unbounded

The replay store interface returns a slice, and the server currently requests
the entire fixed interval. Add a simple maximum event count before loading the
slice. This is a safety bound, not a flow-control subsystem.

### Low

#### L-1: default session ID generation captures the initial real clock

`DefaultServerConfig` creates a closure over a real clock. Replacing only
`ServerConfig.Clock` leaves session IDs tied to wall time. Resolve the default ID
generator after the final clock is known.

#### L-2: helper memory queues remain process-local and unbounded

The outbox and outbound queue are intentionally in-memory reference helpers.
Their process-crash and backpressure limitations need explicit documentation.
Replay-specific protocol buffers still require hard bounds; general production
queue policy remains a caller/runtime concern.

### Non-Issue / already correct

- ACK is enqueued only after `Complete` succeeds. On Complete failure, no ACK is
  produced.
- Gap detection does not advance `last_seq` and does not leak post-gap EVENTs to
  the application.
- Replay events retain their original ID, sequence, and payload.
- Current-generation checks happen before client frame state mutation, so old
  EVENT, PONG, and ERROR frames cannot change the new client state.
- PING/PONG do not consume EVENT sequence and are not replayed.
- Wire timestamps are not used for correctness decisions.
- `Codec` remains an interface; the protocol model is not permanently coupled
  to JSON.
- Locks are not held across application callbacks, store calls, transport I/O,
  or user event delivery actions.
- An invalid incoming ERROR is not answered with another ERROR, preventing an
  ERROR loop.

### Out of Scope

The following are runtime or application concerns, not defects in the protocol
core review: WebSocket/TCP/QUIC lifecycle, HTTP APIs, connection registries,
database or Redis stores, distributed locks, multi-node session ownership,
global exactly-once transactions, message routing, authentication, Message
Center, BigCC, NLX, UI, groups, uploads, receipts, typing indicators, encryption,
multi-device synchronization, and cluster coordination.

## Planned minimal hardening

1. Preserve active PROCESSING claims and close the local flight-registration
   window.
2. Make RESUMED boundaries wire-explicit and bound replay by events and bytes.
3. Bound EVENT identity retention while rejecting unverifiable old duplicates.
4. Preserve replay-store sequence high-water through pruning.
5. Add deterministic server connection admission state and central error
   disposition.
6. Make the stream lane panic-safe without permanent worker goroutines.
7. Validate outbound frames before state mutation and finish concurrency GoDoc.
8. Add invariant, transition, concurrency, and failure-injection tests.

These changes add configuration and helper state where necessary but do not add
frame types, transport ownership, storage infrastructure, or business behavior.

## Compatibility notes

The implemented API changes are additive configuration fields and exported
helper state/policy types. Existing frame types and application/store interfaces
remain unchanged. Validation of WELCOME resume-field presence and envelope-ID
absence becomes stricter; this is a wire-correctness tightening for v0.1 rather
than a new protocol feature.

Migration details:

- Zero-valued new replay configuration fields select documented defaults, so
  existing `ClientConfig` and `ServerConfig` construction continues to work.
- Callers using `ServerConnection` must complete HELLO or RESUME before passing
  PING/SEND. Callers that intentionally provide their own admission state may
  continue to use the lower-level `Server.HandleIncoming` frame processor.
- RESUMED WELCOME encoders must include both `resume_from` and `replay_to`, even
  when `replay_to` is zero. NEW WELCOME must omit both fields.
- Only HELLO may carry an optional envelope ID among non-reliable frames. PING,
  PONG, WELCOME, ACK, RESUME, and ERROR use their typed payload correlation
  fields and reject an envelope ID.
- Standard ERROR codes with fixed retryability now reject a contradictory
  `retryable` flag. INTERNAL remains explicitly selectable per failure.

The server continues to pass `msgID` to `ApplicationHandler` as the idempotency
key. Legal SEND IDs are required to be globally unique (ULID recommended), while
the protocol store uses `(session_id, msg_id)` as its deduplication identity.
Applications requiring end-to-end side-effect deduplication must persist and
honor that key. KMTProto alone does not claim global exactly-once execution.

## Resolution record

| Finding | Status before hardening | Resolution/test |
| --- | --- | --- |
| C-1 | Open | Resolved: PROCESSING survives TTL; `TestProcessingDedupClaimDoesNotExpire` |
| H-1 | Open | Resolved: flight precedes Claim; `TestDuplicateBindsToFlightBeforeClaim` |
| H-2 | Open | Resolved: event/byte replay limits and bounded identity window; `TestClientReplayLimitsDoNotAdvanceSequence`, `TestEventIdentityWindowIsBoundedAndConservative` |
| H-3 | Open | Resolved: generation-fenced `ServerConnection` admission state; `TestServerConnectionAdmissionState`, `TestServerConnectionReplacementFencesLateHandshake` |
| H-4 | Open | Resolved: separate sequence high-water and exhaustion guard; `TestReplayHighWaterSurvivesFullPrune` |
| H-5 | Open | Resolved: explicit RESUMED boundary encoding/presence validation; `TestResumedWelcomeCarriesExplicitBounds` |
| M-1 | Open | Resolved: PONG requires READY/SUSPECT; `TestInvalidClientStateTransitionsAndOldWelcome` |
| M-2 | Open | Resolved: `BehaviorForErrorCode`; `TestErrorBehaviorAndRetryabilityValidation` |
| M-3 | Open | Resolved: no permanent lane worker and panic recovery; `TestStreamLaneRecoversFromPanic` |
| M-4 | Open | Resolved: complete outbound validation precedes client mutation; `TestOutboundValidationPrecedesClientMutation` |
| M-5 | Open | Resolved: invariant and failure-window suite in `hardening_test.go` |
| M-6 | Open | Resolved: `ServerConfig.MaxReplayEvents`; `TestServerReplayEventLimitReturnsSyncRequired` |
| L-1 | Open/documentation | Documented: when replacing `Clock`, set `NewSessionID` explicitly or nil so `NewServer` derives it from that clock |
| L-2 | Open/documentation | Documented: helper queues/outbox are process-local; replay-specific memory is bounded |

## Final verification

The hardened tree completed all required checks with Go 1.22:

```text
gofmt -w .                                      PASS
go vet ./...                                    PASS
go test ./...                                   PASS
go test -race ./...                             PASS
go test -fuzz=FuzzJSONCodec -fuzztime=10s .     PASS (99,024 executions)
```

No real network, wall-clock sleep, scheduler-timing assertion, database,
WebSocket runtime, or business dependency was added.
