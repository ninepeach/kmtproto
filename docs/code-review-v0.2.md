# KMTProto v0.2 Code Logic Review

Review date: 2026-08-20

Reviewed branch: `main`

Reviewed commit: `9c3a3f87d9e462a78f32d3cfda3f6acf4c8b2b2f`

## 1. Overall assessment

KMTProto remains a well-separated, transport-independent protocol library. The
core package has no non-standard dependencies, no transport implementation, no
business routing, and no persistence assumption. Client actions, reference
queues, memory stores, and the reference server admission gate are separated
from caller-owned I/O and application execution.

The principal v0.2 mechanisms are structurally sound:

- wire v2 is the sole encoded and accepted baseline;
- capability negotiation is canonical and stored immutably;
- ACK is emitted only after `Complete`;
- PROCESSING claims do not expire in the memory dedup store;
- EVENT sequence, replay high-water, gap detection, and partial-replay gating
  are isolated from State and heartbeat;
- Resume fixes a replay boundary and validates a contiguous store result;
- State replacement is monotonic and applies snapshots atomically to the
  Client cache;
- connection-generation result application is fenced;
- replay/live stream callbacks are not invoked under the stream-lane mutex.

However, at the reviewed commit the implementation was **not ready to freeze**. This review found no
Critical issue, but found six High correctness issues. They are protocol
semantic or concurrency defects that can occur without changing the wire
format. Passing tests and race detection do not cover these paths.

| Severity | Count | Freeze impact |
|---|---:|---|
| Critical | 0 | None found |
| High | 6 | Must be resolved before freeze |
| Medium | 9 | Resolve or explicitly accept with a precise contract |
| Low | 3 | Follow-up hardening/documentation |

Recommendation at reviewed commit: **Needs fixes**. See the post-review
Resolution Record in section 9 for the corrected working tree.

## 2. Architecture and boundary review

### Confirmed

- The package imports only the Go standard library.
- `Client` produces protocol Actions and owns no transport.
- `Server` consumes typed Frames and writes to a caller-owned `OutboundQueue`;
  it owns no listener, connection registry, or reconnect loop.
- `ApplicationHandler`, `ReplayStore`, `EventAppender`, `StateStore`,
  `StateSnapshotProvider`, and `SessionRepository` are interfaces rather than
  database assumptions.
- `MemoryDedupStore`, `MemoryReplayStore`, `MemorySessionRepository`,
  `OutboundQueue`, `SingleWriter`, and `ServerConnection` are reference/helper
  implementations, not required wire components.
- State data, SEND content, EVENT content, and event types remain
  application-opaque.

### Boundary clarification needed

`Server.HandleIncoming` does not enforce connection admission state; only
`ServerConnection.Handle` does. This is a valid separation, but the public API
should state unambiguously that callers bypassing `ServerConnection` MUST
provide equivalent per-connection admission and serialization. Otherwise a
caller can invoke repeated HELLO or out-of-state Frames directly against the
processor.

## 3. Correctness findings

### Critical

No Critical finding was identified. In accordance with the review-only task,
no Go code was changed.

### High

#### H-01: State Frame gates ignore the negotiated capability version

The specification requires built-in State Frames to use `state-sync` version
1 (`docs/protocol-v0.2.md:103-106`). Negotiation correctly selects an exact,
highest common version, and the public test deliberately negotiates version 2
(`capability_test.go:135-203`). All State gates then call only
`Capabilities.Enabled(CapabilityStateSync)`:

- Client Resume with State: `client.go:237-245`;
- Client query/snapshot/update paths: `client.go:291-301`, `442-450`,
  `480-482`, and `521-526`;
- Server query, live update, and Resume paths: `server.go:190-200`,
  `293-317`, and `456-470`.

Consequently a Session negotiated as `state-sync@2` uses the v1 State Frame
grammar and semantics. This defeats capability versioning and makes an
incompatible future capability version unsafe.

Required correction: every built-in State operation must require the exact
supported version (currently 1), or the specification must explicitly define
version compatibility. Add a test that negotiates only version 2 and proves
v1 State operations are rejected.

#### H-02: `INVALID_SESSION` does not abandon a READY Client Session

The canonical error disposition marks `INVALID_SESSION` as
`AbandonSession=true` (`error.go:34-50`), and the specification says it abandons
the logical Session (`docs/protocol-v0.2.md:224-234`). The Client performs the
abandon/reset only when its current state is RESUMING
(`client.go:727-763`).

The Server can validly return `INVALID_SESSION` for READY PING, SEND, or
STATE_QUERY requests (`server.go:190-197`, `366-373`, and `382-389`). In those
cases the Client emits a `ProtocolErrorAction` but remains READY with the
expired Session ID, retained outbox, capabilities, sequence, and State cache.
Subsequent requests continue using a Session the server has already rejected.

Required correction: apply the `AbandonSession` disposition independent of the
current non-disconnected state, while preserving any explicitly documented
caller recovery actions. Add READY-path tests for PING, SEND, and STATE_QUERY.

#### H-03: an ERROR for another Session can mutate the current Client

The Client's Session correlation check exempts every ERROR
(`client.go:405-419`). A current-generation ERROR with a non-empty, different
`session_id` can therefore:

- delete a pending State query by `ref_id`;
- close the current connection;
- trigger full-sync behavior; or
- abandon protocol state once H-02 is corrected.

Generation fencing does not address cross-Session correlation within the same
generation. A sessionless ERROR is necessary during HELLO, but a non-empty
ERROR Session ID must either match the active Session or be rejected/ignored
conservatively.

Required correction: distinguish sessionless handshake errors from
Session-scoped errors and validate correlation before applying the error
disposition. Add wrong-Session ERROR tests in READY and RESUMING.

#### H-04: valid in-flight EVENTs after Gap detection are treated as violations

On a READY Gap, the Client enters RESUMING, preserves `last_seq`, and emits
RESUME (`client.go:663-724`). Before the peer can process that RESUME and return
RESUMED WELCOME, already in-flight later EVENTs may legitimately arrive on the
ordered transport. While `replayTo==0`, the Client returns
`PROTOCOL_VIOLATION` for every such EVENT (`client.go:677-684`).

This turns a normal recovery race into an integration-dependent failure. It is
also inconsistent with the intended Gap rule: do not deliver or advance, then
recover from the confirmed `last_seq`. These pre-WELCOME EVENTs should be
dropped/gated without changing `last_seq`; events received after an explicit
fixed Replay boundary can remain strictly validated.

Required correction: define and test the pre-WELCOME Gap substate. In-flight
EVENTs must neither reach the Application nor abort a valid recovery attempt.

#### H-05: same-SEND callback re-entry can deadlock on its own flight

The SEND flight is installed before `DedupStore.Claim`, which correctly closes
the original Claim/register race (`server.go:390-395`). A duplicate waits for
that flight to finish (`server.go:436-453`). Unlike `streamLane`, this mechanism
has no callback-re-entry guard.

If an injected `ServerSessionStore.Claim` or `ApplicationHandler.HandleSend`
synchronously re-enters `Server.HandleIncoming` for the same
`(session_id,msg_id)`, the nested call sees the existing flight and waits on the
outer call. The outer callback cannot return until the nested call returns, so
the flight can never close. With a non-cancelled context this is an unbounded
self-deadlock.

The stream lane already states and implements a fail-fast re-entry policy
(`server.go:72-76`, `714-771`); SEND needs an equivalent explicit contract or a
non-waiting indeterminate result for callback re-entry.

Required correction: make same-flight callback re-entry fail deterministically
without waiting. Add Store- and Application-re-entry tests with bounded
contexts and prove that the original execution can complete.

#### H-06: a reused SEND ID is not bound to the original request content

The dedup identity and record contain only `(session_id,msg_id)`, state, and the
stored ACK (`types.go:70-80`, `store.go:33-41`). A COMPLETED duplicate always
receives that ACK (`server.go:397-405`), even if the retried SEND has different
content. The Client rejects duplicate IDs only while they remain in the pending
outbox; after ACK removes the entry, the same ID can be enqueued again with
different content (`client.go:323-343`, `648-660`).

The Application is correctly not executed twice, but the second, different
logical request is silently acknowledged as if it were the original. The wire
contract says a retry reuses the same ID; it should also require the same
logical request and fail deterministic identity conflicts.

Required correction: bind a claim to an immutable request fingerprint (or
otherwise retain enough identity metadata) and reject same-ID/different-content
retries. At minimum the normative contract and Client API must prevent post-ACK
ID reuse. Add completed and concurrent conflicting-duplicate tests.

### Medium

#### M-01: required JSON field presence is not consistently enforced

- RESUME decodes `last_seq` but never checks that the field is present
  (`validate.go:239-263`), despite the specification requiring explicit
  `last_seq`. `{}` is accepted as `last_seq=0`.
- ERROR validates the decoded boolean value but not presence of `retryable`
  (`validate.go:346-370`). For fixed-false codes, omission is indistinguishable
  from an explicit canonical `false`, despite the wire rule that every ERROR
  contains the field.

Add field-presence checks and wire-level tests using literal JSON rather than
typed payload constructors.

#### M-02: injected replay/dedup output is structurally, not semantically, correlated

`PublishEvent` validates requested input, calls `EventAppender.Append`, then
validates only the returned Envelope shape before enqueue
(`server.go:256-290`). It does not assert that the returned Event preserves the
requested Session ID, event ID, type, or content. Similarly, a custom dedup
store's COMPLETED ACK is enqueued without checking that its Session and
`ref_id` match the duplicate SEND (`server.go:397-405`, `450-451`).

`ReplayStore` and State providers have detailed GoDoc contracts and post-call
correlation checks; `EventAppender` does not (`store.go:88-98`). A faulty custom
dependency can therefore emit a valid Frame for the wrong logical operation.

Define semantic return contracts and validate correlation before enqueue.

#### M-03: application error automatically makes a claim retryable

Any non-nil `ApplicationHandler` error causes unconditional `Abort`
(`server.go:416-419`). If an Application performed a side effect and then
returned an error with an indeterminate outcome, the next retry may execute it
again. Durable application idempotency limits the damage, but the safe meaning
of returning an error is not stated.

Document that a returned error authorizes Abort only when the Application can
prove no commit occurred, or introduce an explicit outcome distinction. Do not
automatically reclaim an indeterminate application result.

#### M-04: point State snapshot and live update output are not serialized

Resume State snapshots and live State updates share `streamLane`, but
`handleStateQuery` reads and enqueues outside that lane (`server.go:190-253`). A
query can read version 5, pause, then a version 6 live update can be enqueued
before the older query snapshot. The Client accepts v6 and rejects the later v5
snapshot as `INVALID_STATE_VERSION`, leaving the query pending
(`client.go:442-477`, `535-574`). State does not regress, but a valid concurrent
query can fail or starve.

Define a deterministic policy: serialize the snapshot enqueue with live State
output, or treat older query snapshot items as superseded while still
completing correlation.

#### M-05: generated Session ID collisions overwrite the memory repository

`MemorySessionRepository.Create` unconditionally assigns by Session ID
(`store.go:188-195`). `handleHello` does not check for a pre-existing generated
ID before Create (`server.go:345-359`). The default random generator makes a
collision extremely unlikely, but `NewSessionID` is public and injectable. A
duplicate can replace an active Session's expiry and capabilities.

The repository contract should require collision rejection, and the reference
repository should implement it.

#### M-06: configured TTL invariants are disconnected from the injected store

`NewServer` validates relationships among `ServerConfig` TTL values
(`server.go:125-133`), but `ServerSessionStore` does not expose or promise its
retention window. A caller can inject `MemoryDedupStore` with a shorter TTL than
`ServerConfig.DedupTTL`; the constructor still accepts it and completed duplicate
suppression can expire while the Session remains resumable.

This cannot be mechanically enforced for arbitrary storage, but the store
contract must require retention consistent with the configured retry/resume
windows. Reference construction should make mismatches harder.

#### M-07: strict JSON still accepts duplicate object member names

`DisallowUnknownFields` rejects unknown fields, but Go's JSON decoder accepts
duplicate members and keeps the last value (`json_codec.go:36-57`, `64-78`). A
Frame such as one containing two `type` or two `last_seq` members therefore has
ambiguous producer semantics but a deterministic last-wins interpretation.
Strict protocol mode should reject duplicate member names at the Envelope and
typed-payload levels, while SEND/EVENT content and State data may remain opaque
according to their documented policy.

Also, `decodePayload` recognizes only the exact bytes `null`, not whitespace-
padded null (`json_codec.go:64-66`). JSONCodec decoding typically normalizes a
RawMessage, but direct public `ValidateFrame` calls can reach this inconsistency.

#### M-08: injected Clock calls occur while protocol/store locks are held

Client operations call `Clock.Now` while holding `Client.mu` (for example
`client.go:293-320`, `364-401`), and `MemoryDedupStore` calls its Clock while
holding the store mutex (`store.go:129-161`). Clock is an injected interface, so
the absolute documentation statement that no lock is held across user
callbacks (`docs/protocol-v0.2.md:256-261`) is not literally true. A blocking or
re-entrant Clock can deadlock the object.

Either constrain Clock explicitly to a non-blocking, non-re-entrant value
provider, or sample it outside the lock and pass the value into the transition.

#### M-09: maximum EVENT sequence cannot be represented by WELCOME validation

WELCOME replay bounds use `p.ReplayTo+1 < p.ResumeFrom`
(`validate.go:143-147`). When `ReplayTo==math.MaxUint64`, the addition wraps to
zero and a valid final replay range is rejected. Server Resume otherwise
contains explicit sequence-exhaustion handling (`server.go:487-495`).

Use overflow-safe comparisons and add a boundary test for replaying the final
EVENT before sequence exhaustion.

### Low

#### L-01: reference maps have unbounded lifetime growth

`Server.lanes` is never retired, and `MemoryReplayStore.ids` retains every event
ID even after events are pruned (`server.go:714-722`, `store.go:208-225`,
`295-309`). This is not a wire correctness failure and the components are
reference helpers, but long-lived process-local use can grow memory by Session
and EVENT count.

#### L-02: default ID generator closures can retain the original real Clock

`DefaultServerConfig` builds ID generator closures from its initial RealClock
(`server.go:43-57`). Replacing only `config.Clock` later does not replace those
closures because they are already non-nil (`server.go:134-139`). Tests normally
override IDs explicitly, but this is surprising for deterministic integration
and makes timestamp and generated-ID clocks disagree.

#### L-03: Action emission is the delivery boundary, not action execution

The Client advances `last_seq` and commits State before returning delivery
Actions (`client.go:698-724`, `535-574`). If caller action execution fails or the
process crashes after transition but before handling the Actions, KMTProto does
not retry that local delivery. This is consistent with a process-local protocol
machine, but should remain explicit so consumers do not infer crash-safe local
delivery.

## 4. Concurrency findings

### Confirmed safe paths

- Client mutable state is serialized by one mutex.
- Session capabilities expose defensive lists and have no mutation API.
- Memory stores and OutboundQueue protect process-local data.
- ServerConnection applies async results only when both generation and queue
  still match (`server.go:928-960`).
- Replay and live EVENT/STATE publication share a per-Session stream lane.
- The stream lane releases its mutex around callbacks, fails fast on same-lane
  callback re-entry, recovers a callback panic, and continues draining.
- Outbound batches are atomically enqueued, preventing replay/live interleave at
  that serialization point.
- No internal goroutine is spawned by Client or Server; writer goroutine
  lifecycle remains caller-owned.

### Concurrency defects/risks

- H-05 is a real wait cycle in same-SEND callback re-entry; `go test -race`
  cannot detect logical deadlock.
- M-04 is an ordering race across two independently safe output paths.
- M-08 contradicts the documented callback/lock boundary for injected Clocks.
- Concurrent HELLO calls admitted against one `ServerConnection` are memory-safe
  but can leave an orphan Session: the second call closes admission while the
  first already-running handler may still create protocol Session state. A
  single reader/caller-serialization requirement should be explicit if this
  race is intentionally delegated to the transport adapter.

## 5. Protocol invariant review

| Invariant | Result | Notes |
|---|---|---|
| One authoritative v0.2 wire version | PASS | `WireVersionV2=2`; other versions deterministically reject |
| Unknown/malformed Frames do not panic | PASS with Medium gaps | Bounded codec and fuzz pass; duplicate JSON members and required-field presence remain |
| Negotiated capabilities are immutable | PASS | Defensive Session capability snapshots |
| Feature use matches negotiated capability version | **FAIL** | H-01 |
| Invalid transitions are deterministic | PASS with High gap | General matrix is strong; H-02 and H-04 are incorrect dispositions |
| Stale generation cannot mutate current Client/connection state | PASS | Client entry fence and ServerConnection result fence |
| ACK occurs only after Complete | PASS | Failure tests and direct flow prove boundary |
| Duplicate SEND never repeats Application while PROCESSING/COMPLETED | PASS | Flight plus atomic Claim; re-entry deadlocks rather than repeats |
| Same SEND identity denotes one immutable logical request | **FAIL** | H-06 |
| Only EVENT consumes sequence | PASS | All non-EVENT validators require `seq=0` |
| EVENT sequence is monotonic and Session-scoped | PASS | Memory appender and Replay checks preserve high-water/contiguity |
| Gap never advances `last_seq` | PASS | H-04 affects recovery availability, not sequence safety |
| Partial Replay is not delivered | PASS | Client buffers until EVENT and optional State phases finish |
| Replay preserves identity and fixed boundary | PASS | Store results validated for shape/session/contiguous sequence |
| State version is independent of EVENT sequence | PASS | Separate State merge/cache; State Frames require `seq=0` |
| Stale/conflicting State cannot overwrite newer State | PASS | All-or-nothing temporary map commit |
| READY follows complete Resume recovery | PASS | EVENT-only and EVENT+State gates are explicit |
| Heartbeat cannot alter EVENT/State ordering | PASS | Local duration and matching generation/ping ID only |
| Error disposition is Session-correct | **FAIL** | H-02 and H-03 |
| No lock across injected/user callbacks | **FAIL** | Stream/Application/Store paths mostly pass; M-08 Clock exception |

## 6. Test quality review

The suite is substantially stronger than simple happy-path coverage. It tests
codec bounds and fuzz input, capability immutability, atomic Claim, pre-Complete
failure windows, ACK loss, processing TTL, Event identity retention, replay
bounds/high-water, partial Replay, generation fencing, State snapshot limits,
State merge semantics, Resume-with-State ordering, stream re-entry, and race
safety.

Measured statement coverage is 78.6%. The raw percentage is acceptable for a
protocol core, but weakly exercised functions align with actual review risk:
`Client.handleErrorLocked` is 58.5% covered and `Server.waitForOriginal` is
18.2% covered.

Missing invariant tests, in priority order:

1. exact `state-sync` capability version gating;
2. READY `INVALID_SESSION` abandonment and wrong-Session ERROR isolation;
3. in-flight EVENTs between Gap detection and RESUMED WELCOME;
4. synchronous same-SEND re-entry from Claim and Application callbacks;
5. same SEND ID with different content in PROCESSING and COMPLETED states;
6. explicit RESUME `last_seq` and ERROR `retryable` presence;
7. EventAppender and dedup ACK correlation failures;
8. query snapshot/live update inversion;
9. generated Session ID collision;
10. maximum EVENT sequence/replay boundary overflow;
11. duplicate JSON object members in strict mode.

The fuzz target exercises arbitrary bytes through JSONCodec and proves bounded
non-panicking behavior for that path. It does not fuzz state-machine action
sequences, injected-store adversarial results, or callback re-entry.

## 7. Validation performed

Using Go 1.22.12 at the reviewed commit:

| Validation | Result |
|---|---|
| `go vet ./...` | PASS |
| `go test ./...` | PASS |
| `go test -race ./...` | PASS |
| `go test -run=^$ -fuzz=FuzzJSONCodec -fuzztime=10s .` | PASS (344,524 executions reported) |
| `go test -cover .` | PASS, 78.6% statement coverage |

Race and fuzz success are valuable but do not invalidate the semantic wait,
correlation, disposition, and ordering findings above.

## 8. Freeze recommendation

**Freeze status at reviewed commit: Needs fixes.**

KMTProto v0.2 should not be frozen at this commit. Resolve all six High
findings with minimal, wire-compatible corrections and invariant tests. Then
resolve or explicitly accept the Medium findings in the normative protocol and
interface contracts, rerun vet/test/race/fuzz, and perform a focused delta
review.

Current result:

- Critical correctness findings: **0**
- High correctness findings: **6**
- Architecture boundary: **approved**
- Protocol freeze: **not approved**

## 9. Post-review resolution record

The following corrections were applied after the review without adding a Frame
Type, transport, business feature, or persistence implementation.

### High findings

| Finding | Resolution | Regression proof |
|---|---|---|
| H-01 capability version gate | Resolved: all built-in State operations require exact `state-sync@1`; a negotiated version 2 remains visible but cannot invoke v1 semantics | `TestStateFramesRequireExactCapabilityVersion` |
| H-02 INVALID_SESSION disposition | Resolved: Client abandonment is state-independent; READY ServerConnection PING/SEND paths also return to awaiting handshake | `TestInvalidSessionAlwaysAbandonsClientSession`, `TestInvalidSessionAbandonsServerConnection` |
| H-03 wrong-Session ERROR | Resolved: non-empty ERROR Session IDs are correlated before any Client mutation | `TestWrongSessionErrorCannotMutateClient` |
| H-04 pre-WELCOME Gap EVENT | Resolved: only same-connection Gap recovery discards superseded in-flight EVENTs without delivery or sequence advance; reconnect Resume remains strict | `TestGapRecoveryIgnoresPreWelcomeInflightEvents` |
| H-05 same-SEND re-entry | Resolved: SEND flights mark injected callbacks; same-identity callback re-entry receives a retryable indeterminate ERROR instead of waiting on itself | `TestSameSendApplicationReentryFailsFast`, `TestSameSendStoreReentryFailsFast` |
| H-06 conflicting SEND reuse | Resolved: Claim atomically binds a `SendFingerprint`; PROCESSING and COMPLETED same-ID/different-content input is rejected | `TestProcessingSendRejectsConflictingContent`, `TestSendIDCannotBeReusedWithDifferentContent` |

### Medium and Low findings

| Finding | Resolution |
|---|---|
| M-01 required JSON presence | Resolved: RESUME requires an explicit `last_seq`; ERROR requires an explicit `retryable` |
| M-02 injected result correlation | Resolved: EventAppender output and stored ACKs are structurally and semantically correlated before enqueue |
| M-03 Application error Abort | Resolved conservatively: an Application error leaves PROCESSING indeterminate; it is not automatically aborted |
| M-04 query/update inversion | Resolved: point State query output uses the same Session stream lane as live State and Resume output |
| M-05 Session ID collision | Resolved: repository Create atomically rejects `ErrSessionExists`; the Server returns deterministic INTERNAL |
| M-06 configured/store TTL gap | Resolved by mandatory Store retention contract and optional `DedupRetentionReporter` constructor verification; the memory store reports its TTL |
| M-07 duplicate JSON members | Resolved in strict mode for Envelope and typed payload objects; opaque SEND/EVENT content and State data remain opaque |
| M-08 Clock under lock | Accepted with explicit contract: Clock is a prompt, concurrency-safe, non-re-entrant value provider, not a general callback |
| M-09 replay bound overflow | Resolved with subtraction-safe bound validation and a MaxUint64 regression test |
| L-01 helper map growth | Partially resolved: idle stream lanes are reference-counted and retired; replay event-ID retention remains an explicit process-local helper tradeoff |
| L-02 default generator Clock | Resolved: default ID generators are bound by `NewServer` after the final configured Clock is known |
| L-03 local Action execution | Accepted/out of scope: action emission remains the documented process-local delivery boundary |

### Additional safety corrections

- completed in-flight duplicates receive the stored ACK even during a
  post-Complete injected callback;
- custom dedup ACK Session/ref-ID mismatch is rejected;
- State query callback re-entry retains the existing fail-fast stream-lane
  contract;
- idle Session stream lanes are removed without weakening concurrent
  serialization;
- MemoryDedupStore normalizes a nil Clock and exposes retention for constructor
  validation.

### Compatibility impact

One public interface change is intentional and required for H-06:

```go
Claim(sessionID, msgID string, fingerprint SendFingerprint) (...)
```

Custom `ServerSessionStore` implementations must persist and return the
fingerprint in `DedupRecord`. `DedupRecord` therefore adds `Fingerprint`.
This is a source-level breaking change for custom Store implementations, but it
does not change the v0.2 wire format. Migration is mechanical: store the
provided fingerprint atomically with a new PROCESSING claim and preserve it
through Complete.

`DefaultServerConfig` now leaves ID generator functions unset until
`NewServer`, so they bind to the final configured Clock. Callers that invoked a
default generator directly before constructing a Server must instead use
`DefaultSessionIDGenerator`/`DefaultFrameIDGenerator` explicitly.

### Post-fix recommendation

All Critical and High findings are resolved in the corrected working tree.
The focused correction set is approved for v0.2 freeze review; it does not
expand protocol scope or change the wire schema.

Post-fix result:

- Critical correctness findings: **0**
- High correctness findings: **0**
- Architecture boundary: **approved**
- Protocol freeze: **approved after applying this correction set**

### Post-fix validation

Using Go 1.22.12:

| Validation | Result |
|---|---|
| `gofmt -w .` and `git diff --check` | PASS |
| `go vet ./...` | PASS |
| `go test ./...` | PASS |
| `go test -race ./...` | PASS |
| `go test -run=^$ -fuzz=FuzzJSONCodec -fuzztime=10s .` | PASS (737,658 executions reported) |
| `go test -cover .` | PASS, 80.5% statement coverage |
| `go run ./examples/basic` | PASS |
