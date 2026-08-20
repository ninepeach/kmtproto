# KMTProto v0.2 Final Protocol Review

Status: **READY TO FREEZE**

Review date: 2026-08-19

Reviewed branch: `agent/capability-negotiation-foundation-v0.2`

Reviewed HEAD: `0c3a38951d056228b165ee247c473ddae0baac3e`

Local `main`: `48955c003420542248d54e7c51788908e911396e`

Reviewed code: the uncommitted Phase 0–5 working tree on top of that HEAD

## 1. Overall assessment

KMTProto has a strong protocol core, and most of the intended v0.2 behavior is
implemented deterministically:

- the package remains transport-, business-, and storage-implementation
  independent;
- SEND crosses Claim, Application acceptance, and Complete before ACK;
- duplicate PROCESSING and COMPLETED SENDs do not re-execute the Application;
- EVENT identity, ordering, gap detection, fixed Replay, and conservative
  duplicate handling are explicit;
- capability negotiation selects a deterministic intersection and stores an
  immutable Session result;
- State Objects have independent, object-scoped monotonic versions;
- State frames do not consume or alter EVENT sequence;
- Resume with State gates all Application delivery until both EVENT Replay and
  the State snapshot are complete;
- generation fencing and mutex-protected state mutation are consistently used.

The initial review found two Critical and three High issues. The focused
finalization changes resolve all five without adding features: v0.2 now has one
Wire Version 2 specification, same-connection Gap Resume is admitted,
Replay/State materialization has byte bounds, and stream callback re-entry
fails deterministically instead of self-deadlocking. The full Go verification
matrix passes against the exact working-tree candidate.

### Resolution record

| Finding | Resolution |
|---|---|
| C-01 wire/version ambiguity | **Resolved**: `WireVersionV2=2` is the only accepted baseline; `docs/protocol-v0.2.md` is normative; README/package/history docs point to it |
| C-02 READY Gap Resume rejected | **Resolved**: same-session RESUME transitions `ServerAdmission` READY -> RESUMING -> READY; end-to-end test covers ClientProtocol gap through the reference gate |
| H-01 unbounded server Replay bytes | **Resolved**: `ServerConfig.MaxReplayBytes` and `ReplayLimits` bound store materialization; overflow returns `SYNC_REQUIRED` |
| H-02 late State response limits | **Resolved**: query snapshots accumulate with count/byte bounds; Resume providers receive `StateSnapshotLimits` before materialization; failures are protocol ERRORs |
| H-03 re-entrant stream deadlock | **Resolved**: active callback re-entry returns `ErrStreamCallbackActive`; deterministic retry and re-entry tests cover the contract |

### Scope boundary result

| Boundary | Result | Evidence |
|---|---|---|
| Transport independence | PASS | no socket, `net.Conn`, HTTP, WebSocket, TCP, or QUIC dependency; Actions, `OutboundQueue`, `ByteSender`, and `Codec` form the boundary |
| Business independence | PASS | SEND/EVENT content and State data remain opaque JSON; no message, task, read-receipt, presence, or workflow implementation |
| Storage independence | PASS | dedup, Session, Replay, and State are interfaces; memory stores are documented process-local helpers |
| Authentication implementation | OUT OF SCOPE | no identity/authentication mechanism is embedded |
| Authorization policy | OUT OF SCOPE | namespace and object authorization belongs to the caller/Application |
| Database semantics | OUT OF SCOPE | interfaces define atomic protocol contracts without selecting a database or distributed transaction model |

## 2. Completed features

### 2.1 Implemented frame model

The implementation recognizes twelve Frame Types under the single
`WireVersionV2` baseline:

| Frame | Direction | Required wire fields | Fixed/forbidden fields | State/admission behavior |
|---|---|---|---|---|
| `HELLO` | Client -> Server | payload object | empty `session_id`, `seq=0`; envelope ID optional | Server awaiting handshake; negotiate and return NEW WELCOME |
| `WELCOME` | Server -> Client | `session_id`, valid `mode` | envelope ID empty, `seq=0` | NEW only in HANDSHAKING; RESUMED only in RESUMING |
| `PING` | Client -> Server | `session_id`, `ping_id` | envelope ID empty, `seq=0` | ready Session; return matching PONG |
| `PONG` | Server -> Client | `session_id`, `ping_id` | envelope ID empty, `seq=0` | Client READY/SUSPECT; only matching current-generation ping affects health |
| `SEND` | Client -> Server | envelope `id`, `session_id`, valid content | `seq=0` | ready Session; reliable Claim/Application/Complete path |
| `ACK` | Server -> Client | `session_id`, payload `ref_id` | envelope ID empty, `seq=0` | Client READY/SUSPECT; remove matching outbox entry |
| `EVENT` | Server -> Client | envelope `id`, `session_id`, `seq>=1`, valid content | only replayable Frame | Client READY or exact next Replay position |
| `RESUME` | Client -> Server | `session_id`, payload object containing `last_seq` value | envelope ID empty, `seq=0` | awaiting handshake or same-session READY Gap recovery; explicit RESUMED WELCOME required |
| `STATE_QUERY` | Client -> Server | envelope `id`, `session_id`, namespace, non-empty bounded object IDs | `seq=0` | READY and negotiated `state-sync` only |
| `STATE_SNAPSHOT` | Server -> Client | envelope `id`, `session_id`, explicit non-null states list | `seq=0` | matching READY query or expected Resume State phase |
| `STATE_UPDATE` | Server -> Client | envelope `id`, `session_id`, valid State Object | `seq=0` | READY and negotiated `state-sync` only |
| `ERROR` | Either direction | known code and code-consistent retryable flag | envelope ID empty, `seq=0` | code-specific disposition; invalid ERROR never creates ERROR-about-ERROR |

Envelope and typed payload validation reject unknown frame types, unsupported
wire versions, invalid identifiers, unknown fields in strict mode, malformed
content, illegal sequence use, oversized frames/payloads, invalid capability
syntax, and invalid State Objects.

### 2.2 Capability negotiation

Capability names are lower-case ASCII identifiers beginning with a letter;
digits are allowed after the first character and single `.` or `-` separators
are allowed internally. Names, offer counts, and version lists are bounded.
Versions are positive and unique.

The server selects the highest common version for every offered capability and
returns accepted capabilities in canonical name order:

```text
client offers intersect server registry = immutable Session capabilities
```

An unknown optional capability is omitted. An unsupported required capability
returns `UNSUPPORTED_FEATURE`, creates no Session, and closes. Malformed or
duplicate capability declarations return `INVALID_CAPABILITY`.

The accepted result is copied into `SessionState`, `ClientProtocol` state, and
`ServerAdmission` state. Public query methods return defensive copies, and
RESUME reuses the Session result rather than renegotiating it.

### 2.3 Reliable SEND/ACK

The implemented order is:

```text
local flight registration
  -> atomic Claim(session_id, msg_id)
  -> ApplicationHandler(msg_id as idempotency key)
  -> Complete(stored logical ACK)
  -> enqueue ACK
```

The local flight is installed before Claim, closing the in-process
Claim/register race. A PROCESSING duplicate waits for the original flight or
returns a retryable indeterminate result; it never executes the Application.
A COMPLETED duplicate returns the stored logical ACK.

An unresolved PROCESSING claim does not expire merely because TTL elapsed.
Crash consistency still requires the Application to persist and honor the
globally unique message ID when end-to-end side-effect deduplication is needed.

ACK means reliable protocol acceptance after `ApplicationHandler` returned nil
and the dedup record completed. It does **not** mean recipient delivery, user
read, final business/workflow success, an external side effect completed, or a
human response.

### 2.4 EVENT stream

- `seq` is scoped to one logical Session EVENT stream;
- only EVENT consumes sequence;
- sequence begins at 1, increases monotonically, and never resets after Replay
  pruning;
- `(session_id, seq)` maps to exactly one event ID within the retained identity
  window;
- same sequence and same ID is an idempotent duplicate;
- same sequence and different ID is a protocol conflict;
- an identity older than the retained verification window fails
  conservatively;
- a gap never advances `last_seq` and immediately suspends delivery;
- Replay uses a fixed `replay_to` and preserves original ID, sequence, and
  payload;
- partial Replay remains buffered and is discarded on interrupted recovery.

### 2.5 State synchronization

`StateObject` contains `namespace`, `object_id`, `version`, and opaque JSON
`data`. State identity is `(namespace, object_id)` and is independent of
Session EVENT position.

- version zero and exhausted `MaxUint64` are invalid;
- a higher version replaces current State;
- a semantically equal value at the same version is an idempotent duplicate;
- an older version or same-version different value is rejected and cannot
  replace retained State;
- complete snapshots are staged before the Client cache is replaced;
- client State cache object and byte ceilings are enforced;
- State frames require the immutable `state-sync` Session capability;
- every State frame has `seq=0` and never enters EVENT Replay.

Exact-ID query snapshots provide independently authoritative point results;
they do not promise a cross-object transaction. Resume uses the separate
`StateSnapshotProvider` contract for one internally consistent namespace
snapshot.

### 2.6 Resume with State

Plain RESUME retains the v0.1 event-only behavior. Optional State Resume is:

1. validate Session and negotiated `state-sync` capability;
2. capture fixed EVENT `replay_to`;
3. obtain and validate the contiguous EVENT Replay;
4. obtain one bounded State snapshot for the requested canonical namespaces;
5. enqueue RESUMED WELCOME, EVENT Replay, then STATE_SNAPSHOT as one batch;
6. Client buffers EVENTs and applies no Application actions until the State
   snapshot is complete;
7. Client emits EVENT actions first, State actions second, then READY.

Snapshot failure produces no WELCOME or partial EVENT Replay. Interrupted
recovery leaves `last_seq` unchanged, so another generation retries from the
last confirmed EVENT position.

## 3. Protocol invariants

The following invariants are implemented and should be copied into the final
normative v0.2 specification:

1. Wire Version 2 is the only accepted v0.2 baseline; Wire Version 1 is rejected
   with `UNSUPPORTED_VERSION`.
2. A logical Session may outlive transport generations.
3. Stale generations cannot mutate active `ClientProtocol` or `ServerAdmission` state.
4. Negotiated capabilities are immutable for the logical Session.
5. An unsupported required capability never creates a Session.
6. `(session_id, msg_id)` is the protocol SEND dedup identity.
7. `msg_id` is passed unchanged to the Application as its idempotency key.
8. ACK is emitted only after Complete succeeds.
9. A PROCESSING or COMPLETED duplicate never executes the Application again.
10. PROCESSING is not reclaimed by ordinary TTL expiry.
11. KMTProto does not claim global exactly-once business side effects.
12. Only EVENT consumes sequence.
13. EVENT sequence is Session-scoped, monotonic, and never decreases.
14. `(session_id, seq)` identifies exactly one event ID.
15. A gap never advances `last_seq` or leaks delivery.
16. Replay has a fixed upper boundary and preserves EVENT identity.
17. Partial Replay is never delivered to the Application.
18. State identity and version are independent from EVENT sequence.
19. State versions are monotonic per object; stale State cannot overwrite newer
    State.
20. State snapshots are validated and staged before Client cache mutation.
21. State frames never affect EVENT sequence, gap detection, or Replay.
22. Resume with State reaches READY only after EVENT and State phases complete.
23. PING/PONG never affect EVENT or State ordering.
24. Timestamp is diagnostic metadata only.
25. Invalid ERROR input never produces another ERROR.

## 4. Findings and resolutions

### Critical

#### C-01 — No single normative v0.2 wire/version contract

**Resolved.** The text below records the original finding.

The implementation adds capabilities, State frames, and Resume State fields
under `WireVersionV1`. `docs/protocol-v0.2-design.md` is explicitly a historical
proposal for Wire Version 2, universal Envelope correlation, `RESUME_OK`, and a
different handshake. `docs/protocol-v0.2-hardening.md` describes the additive
Wire Version 1 implementation. `README.md` still declares the implementation
as v0.1, and `docs/state-sync-v0.2-design.md` still says State synchronization is
not implemented.

Consequences:

- peers cannot identify a frozen v0.2 baseline from the Envelope version;
- it is unclear whether Wire Version 2 is rejected, future, or normative;
- correlation and Resume semantics differ between design and code;
- compatibility claims cannot be tested against one canonical document.

Freeze requirement: choose the existing additive Wire Version 1 extension or
the proposed Wire Version 2 baseline, then publish one normative
`protocol-v0.2.md`, update README/package documentation, and add golden
compatibility fixtures. This is a version-contract decision, not a request to
add features.

#### C-02 — EVENT-gap RESUME conflicts with `ServerAdmission` admission

**Resolved.** The text below records the original finding.

`ClientProtocol.handleEventLocked` handles `incoming.seq > last_seq+1` by entering
RESUMING and immediately returning a RESUME frame on the active connection.
`ServerAdmission.Handle`, however, allows RESUME only while
`AWAITING_HANDSHAKE`; its READY allow-list is PING, SEND, and STATE_QUERY.
Therefore a caller using the reference admission gate will reject the Client's
documented gap recovery with `PROTOCOL_VIOLATION` and close the connection.

The tests cover Client gap generation and new-connection Server Resume
admission separately, but do not connect these two paths.

Freeze requirement: make one existing semantic authoritative—either admit
RESUME from READY for same-connection gap recovery or require reconnect before
the Client emits RESUME—and add an end-to-end state-transition test through
`ServerAdmission`.

### High

#### H-01 — Server Replay has no byte ceiling

**Resolved.** The text below records the original finding.

`ServerConfig` bounds Replay by event count only. `ReplayStore.Replay` returns
the complete slice before the Server validates individual Frames, and the
Server never accumulates a total byte count. With the default 4096 events and a
near-1-MiB Frame ceiling, one valid Resume can require several GiB of copied
Replay data.

The Client has `MaxReplayBytes`; the Server does not. This weakens the stated
bounded-Replay and pre-processing-limit guarantees.

Freeze requirement: define and enforce a server Replay byte ceiling, with a
deterministic `SYNC_REQUIRED` result and a boundary test.

#### H-02 — State response limits are enforced after potentially large work

**Resolved.** The text below records the original finding.

READY STATE_QUERY calls `StateStore.Get` for every requested ID, accumulates
complete State Objects, marshals the entire snapshot, and only then validates
Frame/payload size. A bounded count of individually valid objects can still
produce a response tens of MiB larger than the configured Frame ceiling. The
failure is returned as a local handler error rather than a defined ERROR frame.

Resume State has a deterministic `STATE_UNAVAILABLE` mapping, but it still
depends on the provider returning the full slice before encoded-size
validation.

Freeze requirement: document provider/Store size obligations and make
oversized query/snapshot failure deterministic before enqueue and before
avoidable large encoding/allocation.

#### H-03 — Same-Session stream callbacks can deadlock when re-entered

**Resolved.** The text below records the original finding.

The Session `streamLane` deliberately serializes Replay, live EVENT, and live
State publication. It invokes user-provided `EventAppender.Append` and
`StateSnapshotProvider.Snapshot` while logically owning that lane. The lane
mutex is not held, but a synchronous callback that calls `PublishEvent` or
`PublishStateUpdate` for the same Session enqueues behind itself and waits
forever.

No public concurrency contract forbids re-entry. The current panic recovery
test does not cover it.

Freeze requirement: either prevent re-entrant waiting or explicitly make
same-Session callback re-entry invalid and test/detect it deterministically.
This is a concurrency-contract issue, not a request for a runtime framework.

### Medium

#### M-01 — Strict JSON does not reject duplicate member names

Go's standard JSON decoder deterministically keeps the later matching field,
even with `DisallowUnknownFields`. Duplicate Envelope or protocol-payload keys
can therefore be accepted. Different intermediaries may interpret such input
differently.

Freeze requirement: define duplicate-member behavior and add a Codec test. For
a strict stable wire format, rejection is the safer rule.

#### M-02 — Direct validation can accept whitespace-padded null payloads

`decodePayload` rejects only a RawMessage exactly equal to `null`. When called
through the public Envelope validation path, whitespace-padded null decodes
successfully into a zero-value struct. HELLO and RESUME can then pass because
their zero-value fields are otherwise valid.

Freeze requirement: trim whitespace before the null check and add direct
`ValidateFrame` plus Codec coverage.

#### M-03 — Missing invariant tests

The suite is broad, but freeze coverage is missing for:

- ClientProtocol gap RESUME through a READY `ServerAdmission`;
- server Replay total-byte exhaustion;
- oversized multi-object State response disposition;
- duplicate JSON members and whitespace-null payloads;
- all-or-nothing Client cache behavior when object N in a multi-object
  snapshot fails;
- concurrent READY State queries and query-response races;
- golden v0.1/v0.2 compatibility fixtures for the chosen wire-version policy.

### Low / helper concerns

- `streamLane`, in-memory Session records, Replay event-ID sets, pending
  outbox, and pending State-query maps can grow for the process lifetime.
- `OutboundQueue` is intentionally unbounded.
- memory helpers provide process-local concurrency safety only.

These are not wire-protocol defects. They are acceptable for reference helpers
if the final documentation continues to state their lifecycle, memory, and
non-distributed limits. They must not be advertised as a production gateway,
database, connection registry, or cluster runtime.

### Non-issues / out of scope

- No WebSocket/TCP/HTTP implementation is needed for protocol freeze.
- No database, Redis, distributed lock, Session cluster, or multi-node owner is
  needed.
- Authentication implementation and namespace/object authorization policy
  remain caller concerns.
- EVENT and State do not require a cross-store transaction; Applications that
  require atomic business consistency must provide it.
- State version jumps are valid and are not EVENT gaps.
- Missing exact-query State objects are omitted; deletion/tombstone semantics
  are not silently inferred.
- ACK Replay, PONG, ERROR, and State frames correctly remain outside EVENT
  Replay.

## 5. Test and concurrency review

### Existing coverage

| Area | Assessment |
|---|---|
| Codec | round trip, unknown Envelope field, frame-size bound, deterministic failure classes, arbitrary-byte fuzz target |
| Validation | complete v0.2 frame matrix, missing-field cases, limits, identifiers, unknown Frame behavior |
| Capability | serialization, intersection/highest version, invalid/duplicate input, required feature failure, immutable lifecycle, Resume preservation |
| SEND/ACK | concurrent duplicate, flight-before-Claim, ACK-after-Complete, indeterminate failure windows, ACK-loss recovery |
| EVENT | duplicate/conflict, bounded identity cache, gap detection, Replay boundary, high-water after pruning, partial Replay discard |
| State | object validation, semantic equality, newer/stale/conflict rules, capability gating, no EVENT-sequence effect |
| Resume with State | event-only regression, Replay/snapshot gating, stale snapshot, provider failure, retry, generation fencing, live-update serialization |
| Concurrency | duplicate SEND, connection replacement, stream panic recovery, outbound batch non-interleaving, race-detector suite |

Client methods are mutex-protected and return Actions instead of executing
transport/Application callbacks. Server flight and Session-lane mutexes are
released before Application and Store calls. `ServerAdmission` releases its
mutex before invoking `ServerProtocol`. Memory helpers and `OutboundQueue` document
process-local concurrent safety. `SingleWriter` explicitly requires one active
Run call.

No Critical/High deadlock concern remains after deterministic same-lane
callback re-entry rejection. The exact candidate passes race-detector
validation.

### Validation evidence

The exact focused-finalization working tree was validated with the official
Go 1.22.12 Linux AMD64 toolchain after verifying its published SHA-256:

- `gofmt -w .`: PASS; no formatting changes required;
- `git diff --check`: PASS;
- `go vet ./...`: PASS;
- `go test ./...`: PASS;
- `go test -race ./...`: PASS;
- `go test -run=^$ -fuzz=FuzzJSONCodec -fuzztime=10s .`: PASS, 501,599 executions;
- `go run ./examples/basic`: PASS;
- active Go references to `WireVersionV1`: none;
- `WireVersionV2` declaration: unique.

## 6. Documentation review

| Required topic | Current source | Result |
|---|---|---|
| Base reliable protocol | `docs/protocol-v0.1.md` | complete for v0.1 |
| v0.2 architecture/version | `docs/protocol-v0.2.md` | normative Wire Version 2 contract |
| Capability model | normative protocol, GoDoc, tests | implemented and consolidated |
| State model | normative protocol, GoDoc, tests | implemented and consolidated |
| Frame definitions | `docs/protocol-v0.2.md` | authoritative implemented matrix |
| Error model and limits | `docs/protocol-v0.2-hardening.md` | current and useful, but partial |
| Resume/State ordering | `docs/protocol-v0.2.md` | implemented and consolidated into the normative contract |
| Extension rules | `docs/protocol-v0.2.md` | capability-gated extension policy documented |
| Package overview | README and `doc.go` | updated for v0.2 Wire Version 2 and State/capability scope |

Documentation now has one implemented, normative v0.2 contract. Historical
design and roadmap documents are explicitly non-normative.

## 7. Breaking changes and compatibility

### Public Go API

Most Phase 0–5 API changes are additive:

- Capability types, registry, and Session query APIs;
- State types, validation/merge APIs, Store interfaces, Client query APIs, and
  State actions;
- optional Client/Server configuration fields and limits;
- State Frame Types and payloads;
- additional protocol error codes;
- `ProtocolError.Cause` and error unwrapping.

The base `ClientProtocol` workflow remains intact. Before first external use,
the `NewServerProtocol` positional dependencies were replaced by the structured
`ServerDependencies` parameter and the low-level entry was renamed to
`ProcessFrame`; no wire or protocol behavior changed.
The final safety pass intentionally changes the `ReplayStore.Replay` and
`StateSnapshotProvider.Snapshot` interfaces to receive materialization limits.
Custom implementations must accept and enforce those bounds.

### Wire compatibility

v0.2 is intentionally wire-breaking: all Frames use version 2 and version 1 is
rejected. There is no dual-protocol or migration layer because the project has
no production consumer compatibility burden. Core SEND, EVENT, Resume, and
heartbeat meanings remain continuous, but wire interoperability with v0.1 is
not claimed.

## 8. Freeze recommendation

**Yes.** Critical protocol findings are 0 and High protocol findings are 0 in
the current working tree. The architecture, wire semantics, state machines,
reliability boundary, ordering, State separation, Resume gating, and resource
limits are suitable for a v0.2 freeze, and the complete local verification
matrix passes.

Medium strict-JSON hardening items remain documented follow-up work; they do
not change the resolved v0.2 architecture or reliability semantics.

No new Frame Type, business feature, transport, database, authentication
system, or distributed runtime is needed.
