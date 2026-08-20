# KMTProto v0.2 Freeze Review

Status: **READY TO FREEZE**

Review date: 2026-08-20

Reviewed branch: `agent/capability-negotiation-foundation-v0.2`

Reviewed HEAD: `0c3a38951d056228b165ee247c473ddae0baac3e`

Local `main`: `48955c003420542248d54e7c51788908e911396e`

Reviewed candidate: the uncommitted v0.2 working tree on top of that HEAD.

## 1. Protocol status

KMTProto v0.2 is ready for final human freeze review as a
transport-independent chat synchronization protocol. The reviewed candidate
has one wire baseline, deterministic capability and connection admission,
reliable SEND acceptance, one ordered EVENT stream per Session, bounded
Resume/Replay, application-level heartbeat, and capability-gated generic State
synchronization.

No transport, business workflow, authentication policy, authorization policy,
database, distributed ownership, or multi-node consistency behavior has been
added to the protocol core. Memory stores, `OutboundQueue`, `SingleWriter`, and
`ServerAdmission` remain process-local reference helpers.

The implementation does not reproduce every idea in the historical design and
roadmap. It intentionally retains Frame-specific correlation and
`WELCOME(RESUMED)` instead of the proposal's universal `reply_to`, HELLO-first
reconnect, and `RESUME_OK`. These deviations are explicit at the top of both
historical documents. [Protocol v0.2](protocol-v0.2.md) is the single normative
implemented specification, so the historical alternatives do not create a
second valid wire interpretation.

No additional `protocol-v0.2-spec.md` is needed: `protocol-v0.2.md` already
contains the final rules rather than design discussion.

## 2. Fixed issues verification

| Finding from `review-v0.2-final.md` | Verification | Status |
|---|---|---|
| C-01: wire/version ambiguity | `WireVersionV2` is uniquely defined as `2`; all outbound Frames use it; all other versions deterministically produce closing `UNSUPPORTED_VERSION`; the normative specification declares no downgrade mode | **Resolved** |
| C-02: READY Gap Resume admission | ClientProtocol enters RESUMING without advancing `last_seq`; `ServerAdmission` accepts a same-Session RESUME from READY, fences the recovery state, and returns to READY only on explicit success | **Resolved** |
| H-01: unbounded server Replay bytes | Server and ReplayStore receive both event and encoded-byte limits; overflow maps to `SYNC_REQUIRED` before a successful Resume batch | **Resolved** |
| H-02: late State materialization limits | query snapshots use bounded incremental accumulation; namespace snapshot providers receive `StateSnapshotLimits` before materialization; oversized/failing snapshots have deterministic ERROR behavior | **Resolved** |
| H-03: same-Session callback re-entry deadlock | injected callback execution occurs without the lane mutex held; same-lane entry during a callback fails immediately with `ErrStreamCallbackActive` and is covered for EVENT and State providers | **Resolved** |

Freeze-blocking findings after verification:

- Critical: **0**
- High: **0**

## 3. Invariant checklist

### Version

| Invariant | Evidence | Result |
|---|---|---|
| One authoritative v0.2 wire version | `WireVersionV2 = 2`; no active `WireVersionV1` reference | PASS |
| Version validation is deterministic | `ValidateEnvelope`; `TestWireVersionV2IsTheOnlyAcceptedBaseline` | PASS |
| Error response to an older peer still uses v2 | server version-baseline test | PASS |

### Capability negotiation

| Invariant | Evidence | Result |
|---|---|---|
| HELLO advertises bounded capability offers | `HelloPayload`, `ValidateFrame`, serialization tests | PASS |
| WELCOME returns the highest common versions in canonical order | `CapabilityRegistry.Negotiate`; intersection test | PASS |
| Unsupported optional offers are omitted | negotiation lifecycle test | PASS |
| Unsupported required offers fail before Session creation | required-capability test | PASS |
| Negotiated capabilities are immutable and reused by Resume | `SessionCapabilities`; defensive-copy and lifecycle tests | PASS |
| State feature checks use Session capability state | `ClientProtocol`, `ServerProtocol`, and `ServerAdmission` capability gates | PASS |

### Reliable SEND and ACK

| Invariant | Evidence | Result |
|---|---|---|
| Dedup identity is `(session_id, msg_id)` | `ServerSessionStore`, flight key, memory store | PASS |
| ACK follows Claim, Application, and Complete | `handleSend`; ACK-boundary failure test | PASS |
| PROCESSING duplicate never executes Application again | flight-before-Claim and concurrent duplicate tests | PASS |
| COMPLETED duplicate returns the stored logical ACK | duplicate and ACK-loss recovery tests | PASS |
| PROCESSING is not reclaimed by ordinary TTL expiry | memory dedup implementation and TTL test | PASS |
| ACK means protocol acceptance, not read/business completion | normative specification and Application contract | PASS |

### EVENT stream

| Invariant | Evidence | Result |
|---|---|---|
| Only EVENT consumes Session sequence | Frame validator and `TestOnlyEventChangesLastSequence` | PASS |
| Sequence starts at 1, is monotonic, and survives pruning | replay store append/high-water tests | PASS |
| `(session_id, seq)` retains stable event identity | duplicate/conflict and identity-window tests | PASS |
| Gap detection never advances `last_seq` | ClientProtocol gap test and READY ServerAdmission recovery test | PASS |
| Replay has fixed bounds and preserves original Frames | boundary/identity tests | PASS |
| Partial Replay is not delivered | buffered Replay and interrupted recovery tests | PASS |
| Replay event and byte ceilings fail deterministically | Client and Server limit tests | PASS |

### State synchronization

| Invariant | Evidence | Result |
|---|---|---|
| State identity is `(namespace, object_id)` | `StateIdentity` and validation tests | PASS |
| State version is positive, monotonic per object, and independent of EVENT seq | State merge and sequence-independence tests | PASS |
| Stale and same-version conflicting State cannot replace retained State | pure merge and wire update tests | PASS |
| Same-version semantically equal JSON is an idempotent duplicate | semantic-equality test | PASS |
| Every State Frame requires `state-sync` | Client and Server gates plus wire tests | PASS |
| State Frames use `seq=0` and never enter EVENT Replay | frame validation and separation tests | PASS |
| Snapshot object and encoded-byte bounds are enforced | snapshot validation and accumulation tests | PASS |

### Resume and heartbeat

| Invariant | Evidence | Result |
|---|---|---|
| Event-only Resume behavior remains available | event-only Resume regression | PASS |
| State Resume orders WELCOME, complete EVENT Replay, then one snapshot | Resume/State integration tests and outbound batch | PASS |
| Client reaches READY only after all requested recovery phases | gated-delivery test | PASS |
| Snapshot failure or stale State cannot advance EVENT position | provider failure, stale snapshot, and retry tests | PASS |
| PING/PONG never affect EVENT sequence or State versions | validators, heartbeat state machine, ordering test | PASS |
| A late or stale-generation PONG cannot revive a replacement connection | generation-fencing heartbeat test | PASS |

## 4. Concurrency and lifecycle review

`ClientProtocol` serializes state mutation with one mutex and returns Actions for later
execution; it performs no network I/O and invokes no user callback while the
mutex is held.

`ServerAdmission` validates and snapshots admission state under its mutex,
then releases it before invoking `ServerProtocol`. A generation/outbound identity check
fences the later result, so an old handler cannot mutate a replacement
connection.

`ServerProtocol` releases its flight-map mutex before Dedup Store and Application
calls. The per-Session stream lane serializes Replay, live EVENT, and live
State output, but releases its mutex before Session Repository, Replay Store,
Event Appender, and State Snapshot Provider calls. Callback-active detection
prevents synchronous same-lane re-entry from waiting on itself.

`OutboundQueue.EnqueueBatch` makes each Resume batch non-interleavable.
`SingleWriter` documents that exactly one `Run` call may be active. No protocol
object starts a background goroutine, so protocol-core goroutine ownership and
termination remain with the caller.

The process-local lane map, pending outbox/query maps, and unbounded reference
outbound queue remain lifecycle/memory considerations for a caller runtime.
They are documented helper limitations, not distributed-runtime or wire
protocol guarantees.

Race-detector validation of the complete package passes.

## 5. Test coverage and remaining gaps

Existing deterministic coverage includes:

- capability serialization, intersection, invalid offers, required feature
  failure, immutable Session lifecycle, and Resume preservation;
- v2 acceptance and older-version rejection;
- Claim/flight ordering, concurrent duplicate SEND, ACK-after-Complete, crash
  windows, and stored ACK recovery;
- EVENT duplicate/conflict, sequence monotonicity, identity retention, gap
  recovery, fixed Replay, high-water retention, and Replay bounds;
- State Object validation, semantic equality, newer/stale/conflicting merges,
  capability admission, State Frames, query limits, and EVENT separation;
- event-only Resume, Resume with State, delivery gating, State failure, retry,
  generation fencing, and Replay/live publication serialization;
- callback panic recovery, callback re-entry, connection replacement, and
  outbound batch non-interleaving;
- malformed/oversized codec input and arbitrary-byte codec fuzzing.

The following non-blocking invariant tests remain useful:

1. define and test an explicit duplicate-JSON-member policy for strict mode;
2. reject whitespace-padded `null` through direct `ValidateFrame` calls, not
   only through normal Codec-decoded wire input;
3. explicitly prove all-or-nothing Client cache mutation when object N in a
   multi-object snapshot fails;
4. add deterministic concurrent READY State query/response correlation tests;
5. add exact golden JSON fixtures for the frozen v2 Frame shapes.

These are test/strict-input hardening gaps. Inspection and race testing found
no corresponding Critical or High protocol-state failure in the reviewed Go
implementation.

## 6. Documentation consistency

| Document | Role | Result |
|---|---|---|
| `protocol-v0.2.md` | single normative implemented specification | complete and consistent with code |
| `protocol-v0.2-design.md` | superseded historical proposal | clearly marked non-normative and points to the specification |
| `protocol-v0.2-implementation-plan.md` | historical implementation roadmap | clearly records implemented phases and deviations |
| `protocol-v0.2-hardening.md` | safety-decision record | consistent with normative limits and error policy |
| `review-v0.2-final.md` | original findings and resolution record | all Critical/High findings resolved |
| README and package GoDoc | public boundary and usage overview | identify Wire Version 2 and the normative specification |

No active Go code refers to `WireVersionV1`. Historical documents may mention
v0.1 when explaining ancestry, rejected alternatives, or compatibility; those
references do not define active v0.2 behavior.

## 7. Validation evidence

The exact reviewed working tree was validated with the official Go 1.22.12
Linux AMD64 toolchain. The downloaded archive matched the published SHA-256
`4fa4f869b0f7fc6bb1eb2660e74657fbf04cdd290b5aef905585c86051b34d43`.

| Command | Result |
|---|---|
| `gofmt -l .` | PASS; no files reported |
| `git diff --check` | PASS |
| `go vet ./...` | PASS |
| `go test ./...` | PASS |
| `go test -race ./...` | PASS |
| `go test -run=^$ -fuzz=FuzzJSONCodec -fuzztime=10s .` | PASS; 618,286 executions, 181 new interesting inputs |
| `go run ./examples/basic` | PASS |

## 8. Remaining risks

1. Strict mode currently relies on Go JSON decoding behavior for duplicate
   object member names. This is deterministic within this implementation but
   should be made explicitly normative before claiming cross-language strict
   JSON interoperability.
2. The client outbox, Session/replay memory helpers, State cache, and outbound
   helper are process-local; they do not provide process-crash durability or
   production backpressure.
3. Atomic business consistency between an EVENT and a State replacement is an
   Application/storage concern; the protocol intentionally provides no
   cross-model transaction.
4. `ReplayStore`, `StateStore`, and `StateSnapshotProvider` implementations
   must honor their atomicity, defensive-copy, concurrency, and
   materialization-limit contracts. KMTProto does not make these guarantees
   across multiple processes.
5. v0.2 intentionally rejects v0.1 wire traffic. Custom `ReplayStore` and
   `StateSnapshotProvider` implementations must adopt the v0.2 limit-bearing
   method signatures.

None of these risks requires a new Frame Type, transport, database, business
feature, or protocol redesign.

## 9. Freeze recommendation

**Yes — KMTProto v0.2 is ready to freeze as a stable protocol version after
the reviewed working tree is committed and receives final human review.**

The freeze candidate has:

- Critical findings: **0**;
- High findings: **0**;
- one authoritative Wire Version 2 contract;
- deterministic capability, SEND/ACK, EVENT, State, Resume, heartbeat, error,
  and generation-fencing semantics;
- bounded Replay and State materialization;
- documented concurrency and runtime boundaries;
- passing vet, unit, race, fuzz, and example validation.

Do not expand the protocol before freeze. The remaining Medium strict-JSON and
test-fixture items may be resolved as focused hardening work without adding
features or changing the protocol architecture.
