# KMTProto v0.2 — Implementation Plan

Status: **PLANNING / NO IMPLEMENTATION**
Design source: `docs/protocol-v0.2-design.md`
Target: KMTProto v0.2 implementation roadmap

This document converts the approved v0.2 protocol proposal into an incremental,
testable implementation sequence. It does not implement any Frame Type, change
Go code, or modify the v0.1 implementation.

Normative protocol behavior remains defined by
`docs/protocol-v0.2-design.md`. If this plan and the design disagree, the design
wins and the plan must be updated before implementation continues.

## 1. Overview

KMTProto v0.1 is a reliable messaging protocol with ordered EVENT recovery.
KMTProto v0.2 evolves it into a synchronization protocol by adding:

- one Wire Version 2 baseline;
- mandatory HELLO/WELCOME negotiation on every transport;
- a versioned Capability Registry;
- immutable negotiated capabilities and directional limits;
- universal Envelope request/response correlation;
- an explicit Resume acknowledgement and fixed Replay boundary;
- generic, server-authoritative State Objects;
- bounded State query, snapshot, and live update primitives.

The implementation must preserve three independent reliability domains:

| Domain | Identity/order | Recovery |
|---|---|---|
| Reliable SEND | `(session_id, message_id)` | Retry the same message ID |
| EVENT | Session-scoped contiguous `seq` | Fixed-boundary Replay |
| STATE | `(namespace, object_id, version)` | Authoritative snapshot and version merge |

Only EVENT consumes `seq`. State versions are per object and never participate
in EVENT Replay. Client State mutation intent continues to use reliable SEND;
v0.2 does not create a second client-write or ACK protocol.

### 1.1 Delivery strategy

Implementation should be delivered as small pull requests with a green main
branch after every merge. The recommended sequence is:

```text
Phase 0  v2 foundation and capability framework
   -> Phase 1  HELLO/WELCOME negotiation
   -> Phase 2  negotiated connection/session state
   -> Phase 3  State Object and version core
   -> Phase 4  State Frames
   -> Phase 5  EVENT/STATE coexistence
   -> Phase 6  Resume integration
   -> Phase 7  Error evolution
   -> Phase 8  hardening, interoperability, and release gate
```

Phase 7 error work may begin after Phase 1 where needed, but its final behavior
cannot be frozen until State admission and Resume behavior are implemented.

### 1.2 No large rewrite

The current package already has the right architectural primitives:

- typed Envelope and payloads;
- strict bounded JSON Codec;
- a mutex-protected Client state machine that returns Actions;
- a transport-independent Server;
- generation fencing;
- single outbound serialization;
- atomic SEND Claim/Complete contracts;
- Replay Store high-water and fixed Replay lanes;
- FakeClock and deterministic tests.

v0.2 should extend these components rather than replace them with a new runtime,
actor framework, transport stack, or storage system.

### 1.3 One intentional compatibility break

There is no production consumer, public client, or compatibility requirement.
The first implementation phase therefore performs one explicit cutover from the
v1 wire model to the v2 base model. It must not leave a permanent dual-protocol
path.

After that foundation merge, later phases should be additive within v0.2:

- do not reinterpret a released Capability version;
- add optional behavior through a new Capability or Capability version;
- keep accepted capability semantics immutable for one connection generation;
- use configuration/option structs to avoid repeatedly changing constructor
  signatures;
- keep structural validation independent from connection-context admission;
- keep every intermediate commit buildable and testable.

## 2. Design assumptions

The roadmap assumes the following decisions from the v0.2 design.

### 2.1 Version and compatibility

- `WireVersionV2 = 2` is the only accepted wire version in v0.2.
- v0.1 is frozen documentation and historical implementation behavior, not a
  runtime compatibility mode.
- there is no downgrade, migration handshake, or mixed v1/v2 Session.
- stored v0.1 Sessions, pending frames, and Replay data are not automatically
  reused by a v0.2 process.

### 2.2 Handshake

- every physical transport begins with HELLO;
- a new-Session HELLO has no Session ID;
- a reconnecting HELLO carries the existing Session ID;
- WELCOME selects version, capabilities, limits, and either `NEW` or
  `RESUME_REQUIRED` mode;
- `RESUME_OK`, not WELCOME, establishes EVENT Replay boundaries;
- transforms such as compression or encryption cannot activate before
  WELCOME.

### 2.3 Correlation

- every v0.2 frame has an Envelope ID;
- direct responses use Envelope `reply_to`;
- payload-specific `ref_id` and `ping_id` correlation is removed;
- frame IDs are assumed globally unique ULIDs unless design review chooses a
  narrower scope before Phase 0 begins.

### 2.4 Capability scope

- capability names use lowercase normalized ASCII and the design's naming
  rule;
- deterministic negotiation selects the highest mutually supported version;
- unsupported required capabilities fail the handshake;
- unsupported optional capabilities are omitted;
- a frame gated by an unaccepted capability is a protocol violation;
- the accepted set and effective limits are immutable for the connection
  generation.

The Capability Registry must be capable of describing names such as:

- `state-sync`;
- `presence`;
- `compression.zstd`;
- `encryption.example`.

Only `state-sync` is part of the initial implementation scope. A registry entry
does not itself implement presence, compression, or encryption. Those features
require separate reviewed Capability specifications, payload rules, activation
boundaries, limits, and tests.

### 2.5 State model

- State identity is `(namespace, object_id)`;
- version is a positive, per-object, monotonically increasing integer;
- a State Object is a complete replacement, not a patch;
- the authoritative side assigns versions after commit;
- clients do not send authoritative STATE_UPDATE frames;
- exact-ID queries are the v0.2 baseline;
- namespace-wide snapshots, pagination, and subscriptions are deferred;
- deletion is a versioned tombstone;
- State data is opaque application JSON.

### 2.6 Resume selection

The implementation uses Resume option C:

1. negotiate HELLO/WELCOME;
2. recover the EVENT stream through a fixed `replay_to`;
3. enter READY only after complete EVENT Replay;
4. issue explicit, bounded STATE_QUERY requests for application-selected IDs;
5. merge snapshots and later updates by State version.

State snapshots are not embedded in RESUME and are not EVENT Replay items.

### 2.7 Concurrency and I/O

- Client and Server state mutation remains serialized;
- no lock spans network I/O, storage I/O, Application callbacks, or user
  callbacks;
- all output for one connection passes through one writer;
- the caller supplies correct connection generations;
- in-memory helpers are process-local and never imply distributed safety;
- tests use FakeClock and deterministic drivers, not `time.Sleep` or real
  networks.

## 3. Implementation phases

### Phase 0 — v0.2 Foundation

Complexity: **Large**. This is the single intentional wire/API cutover.
Dependency: approved v0.2 design and resolution of Phase 0 blocking questions.

Phase 0 should be split into reviewable internal steps but merged only when the
base v2 path is coherent and all existing reliability tests have v2
equivalents.

#### 0A. Freeze schemas and test vectors

Before production code changes:

- resolve global versus Session-scoped Frame ID uniqueness;
- freeze Envelope v2 fields and correlation rules;
- freeze HELLO/WELCOME and RESUME_OK JSON schemas;
- freeze Capability Offer and Selection schemas;
- define default and maximum negotiated limits;
- publish valid and invalid JSON fixtures for every base frame;
- identify which v0.1 tests remain semantic invariants and which assert an
  intentionally obsolete wire shape.

Exit gate:

- every v2 base frame has required, optional, and forbidden fields documented;
- every direct response has exactly one correlation rule;
- no unresolved question can change the Envelope or handshake grammar.

#### 0B. Envelope and base validation cutover

Planned changes:

- introduce Wire Version 2 as the accepted version;
- add required Envelope `reply_to` support;
- require IDs on every v2 frame;
- remove payload-level correlation fields from ACK, PONG, and ERROR;
- retain `seq` exclusively for EVENT;
- keep Codec interface unchanged;
- update strict JSON validation and encoded/decoded limits;
- inject an ID generator through Client/Server configuration so tests never
  depend on random IDs.

Validation must be separated into two layers:

1. **structural validation** — wire version, Frame schema, field sizes, JSON,
   and context-free invariants; safe for Codec Encode/Decode;
2. **protocol admission** — connection state, generation, Session ownership,
   accepted Capability, negotiated directional limits, and request
   correlation; performed by Client/Server state machines.

The Codec must not require mutable Session state. The SingleWriter may rely on
frames being admission-validated before enqueue, but still performs structural
Encode validation.

#### 0C. Capability framework

Recommended protocol-facing concepts:

- `CapabilityName` — normalized name;
- `CapabilityOffer` — name, supported versions, required flag, parameters;
- `CapabilitySelection` — name, selected version, selected parameters;
- `CapabilitySpec` — supported versions, parameter validation/selection, and
  any Frame admission metadata;
- `CapabilityRegistry` — immutable lookup of server-supported specs;
- `NegotiatedContext` — immutable selected version, capability set, and limits
  for one connection generation.

Capability naming validation:

- match `[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*`;
- reject empty, duplicate, non-ASCII, and oversized names;
- reject empty, zero, duplicate, or excessive version lists;
- canonicalize accepted selections by name without silently normalizing invalid
  input.

Capability storage:

- Client offers live in immutable Client configuration or handshake options;
- server-supported specifications live in a registry constructed before the
  Server begins handling frames;
- the registry is process-local, read-only after construction, and safe for
  concurrent negotiation;
- negotiated selections live in the active connection-generation context;
- only resume-critical Capability requirements may be retained in the logical
  Session record;
- Capability data is not business State and does not require a database
  implementation.

Negotiation algorithm:

1. structurally validate and deduplicate client offers;
2. look up each name in the server registry;
3. calculate the version intersection;
4. select the highest mutually supported version;
5. invoke that Capability version's bounded parameter selector;
6. fail on any unsupported required offer;
7. omit unsupported optional offers;
8. sort accepted results canonically;
9. compute effective directional limits;
10. freeze the result for the generation.

The parameter selector must be deterministic and must not perform network I/O,
Application callbacks, or blocking storage work.

Phase 0 tests:

- Envelope IDs and `reply_to` validation;
- response/request correlation matrix;
- Capability name and version validation;
- canonical ordering;
- deterministic highest-version intersection;
- required versus optional unsupported behavior;
- duplicate offers and duplicate versions;
- bounded parameters and limits;
- concurrent registry reads;
- malformed JSON and fuzz inputs never panic.

Phase 0 exit gate:

- the v2 base model is the only wire model accepted;
- the registry is deterministic and immutable;
- all retained v0.1 reliability invariants pass under the v2 Envelope;
- `go vet`, unit tests, race tests, and Codec fuzz tests pass.

### Phase 1 — HELLO/WELCOME Upgrade

Complexity: **Large**.
Dependency: Phase 0.

Goal: every connection negotiates version, capabilities, and limits before any
Session traffic.

#### Payload model

HELLO should carry:

- protocol version;
- optional bounded client metadata;
- ordered Capability Offers;
- client receive limits.

WELCOME should carry:

- selected version;
- `NEW` or `RESUME_REQUIRED` mode;
- accepted Capability Selections;
- effective client-to-server and server-to-client limits.

Session ID remains in the Envelope. WELCOME correlates to HELLO through
`reply_to`.

#### Client handshake flow

Replace the separate v0.1 “new StartSession versus immediate Resume” entry with
one handshake operation:

```text
CONNECTED
  -> HELLO(no session_id)              for a new Session
  -> HELLO(existing session_id)        for a resumable Session
  -> HANDSHAKING
```

On WELCOME:

- verify current generation and HELLO correlation;
- verify the selected version and accepted set are subsets of the offer;
- freeze negotiated limits and selections;
- `NEW` installs the new Session ID and enters READY;
- `RESUME_REQUIRED` keeps the existing Session ID, produces RESUME, and enters
  RESUMING;
- any other mode/state/correlation is a protocol violation.

The client should automatically produce the RESUME Action after
`RESUME_REQUIRED`; callers should not be able to send normal Session traffic in
the gap.

#### Server handshake flow

ServerConnection remains a minimal admission helper:

```text
AWAITING_HELLO -> NEGOTIATING -> READY
                            \-> AWAITING_RESUME -> READY
                            \-> CLOSED
```

An implementation may keep only externally visible `AWAITING_HANDSHAKE`,
`READY`, and `CLOSED`, but it must internally fence concurrent handshake work so
only one HELLO can win.

The server must:

- reject any non-ERROR frame before HELLO;
- negotiate without creating a new Session first;
- create a Session only after successful required-Capability negotiation;
- look up an existing Session for a reconnecting HELLO;
- verify resume-critical Capability compatibility;
- enqueue WELCOME only after Session admission succeeds;
- never enable Capability-specific frames before WELCOME is queued;
- preserve generation fencing if Session lookup completes late.

#### Code impact

- `payload.go`: HELLO/WELCOME v2 payloads and modes;
- `validate.go`: structural schemas and forbidden fields;
- `client.go`: unified handshake command and WELCOME transition;
- `server.go`: negotiation and HELLO-first admission;
- `types.go`: any server connection admission-state additions;
- `action.go`: negotiated/handshake result Action if exposed;
- `limits.go`: directional offer/selection types.

#### Phase 1 tests

- exact Capability intersection;
- unsupported optional Capability is omitted;
- unsupported required Capability closes with `UNSUPPORTED_FEATURE`;
- empty Capability list succeeds when no server Capability is mandatory;
- duplicate and malformed offers fail;
- incompatible wire version closes with `UNSUPPORTED_VERSION`;
- server cannot accept an unoffered Capability;
- client rejects an invalid accepted version or parameter;
- `NEW` and `RESUME_REQUIRED` state transitions;
- SEND, PING, RESUME, and State Frames before WELCOME are rejected;
- two concurrent HELLO frames create at most one Session;
- old-generation WELCOME cannot install capabilities or limits.

Exit gate: every active Session connection has one immutable negotiated context
before entering READY or RESUMING.

### Phase 2 — Session Capability State

Complexity: **Medium**.
Dependency: Phase 1.

Goal: make the lifetime and ownership of negotiation results explicit.

#### Protocol state versus Application state

| Data | Owner | Lifetime |
|---|---|---|
| selected wire version | protocol connection | connection generation |
| accepted capabilities and versions | protocol connection | connection generation |
| effective directional limits | protocol connection | connection generation |
| outstanding request correlations | protocol connection | connection generation |
| resume-critical Capability requirements | logical Session protocol metadata | Session resume TTL |
| EVENT `last_seq` and replay boundary | logical Session/client protocol state | Session |
| State cache versions and tombstones | client synchronization state | cache policy |
| namespace meaning and visibility | Application | Application policy |
| business State data persistence | Application/State Store | Application policy |

Negotiated limits are connection properties and MUST NOT be copied into durable
Session semantics. A reconnecting client may negotiate smaller limits; Replay
and snapshots must respect the new generation's limits.

Only capabilities that alter resumable Session semantics should be retained as
Session requirements. For the initial release, `state-sync` does not need to be
mandatory for Resume of the EVENT stream unless the Session was explicitly
created with it as a required Session Capability.

#### Planned data model

Introduce one immutable `NegotiatedContext` snapshot containing:

- selected wire version;
- accepted Capability Selections indexed by name;
- canonical accepted Capability list for wire/debug output;
- directional effective limits;
- connection generation.

Maps, slices, parameter JSON, and limits must be defensively copied. Public
accessors return values or copies, never mutable internal references.

Evolve SessionRepository conceptually from existence-only operations to Session
metadata lookup:

```text
Create(SessionRecord) error
Lookup(session_id, now) (SessionRecord, found, error)
```

The minimal SessionRecord contains Session ID, expiration, and any
resume-critical Capability requirements. It does not contain user identity,
authorization policy, business State, transport ownership, or a connection
registry.

#### Phase 2 tests

- immutable negotiated results after WELCOME;
- defensive-copy tests for slices, maps, and RawMessage parameters;
- smaller limits on reconnect are applied to the new generation only;
- required Session Capability mismatch prevents Resume;
- optional Capability changes do not mutate historical Session semantics;
- stale-generation async lookup cannot overwrite current negotiated state;
- concurrent accessors pass the race detector.

Exit gate: every protocol field has one documented owner and lifetime; no
Application State is stored in the protocol Session record.

### Phase 3 — State Synchronization Core

Complexity: **Large**.
Dependency: Phase 2. Frame implementation is not required yet.

Goal: implement and test State identity, replacement, version, equality, cache,
and storage contracts independently of wire Frames.

#### State Object model

The design's wire name is `id`; Go APIs may use `ObjectID` to avoid ambiguity
with Envelope ID:

```text
StateObject
  Namespace string
  ObjectID  string
  Version   uint64
  Deleted   bool
  Data      json.RawMessage
```

Validation:

- namespace and object ID are valid UTF-8, non-empty, and bounded;
- version is positive;
- non-deleted objects contain exactly one valid JSON value;
- tombstones have no data;
- encoded object and data sizes are bounded;
- version must never wrap.

#### State version merge

Implement one pure deterministic merge function used by both snapshots and
updates:

| Incoming versus cached | Result |
|---|---|
| no cached object | install |
| newer version | replace |
| older version | stale; ignore |
| equal version and equal object | duplicate; ignore |
| equal version and different object | protocol conflict |

A jump in State version is legal and does not trigger Gap Detection. The merge
function never reads timestamps or EVENT sequence.

The State equality algorithm is a Phase 3 blocker. Raw JSON bytes are not
sufficient. Choose and document either RFC 8785 canonical JSON or a precise
decoded semantic comparison before implementation. Tests must include object
key order, whitespace, numeric representation, `null`, arrays, and Unicode.

#### Client State cache

The initial cache is a bounded protocol helper, not durable storage:

- keyed by `(namespace, object_id)`;
- stores the most recent value or tombstone and version;
- applies a complete snapshot atomically only after every object validates;
- exposes deterministic Actions rather than invoking callbacks under lock;
- has explicit maximum object count and total byte limits;
- clears or marks entries STALE according to disconnect/Session abandonment
  policy;
- keeps enough identity/version information to reject conflicts in the
  configured verification window.

Cache phase remains independent from connection state:

```text
UNKNOWN -> SYNCING -> CURRENT
              |          |
              +-> STALE <-+
```

#### Protocol-facing State Store contract

No database implementation is added. Define only the atomic behavior needed by
the protocol boundary:

```text
Get(namespace, object_id) -> committed StateObject or missing
GetMany(exact selectors) -> bounded committed results and missing identities
CompareAndSet(namespace, object_id, expected_version, replacement)
    -> committed StateObject or conflict
```

`GetMany` must state whether it provides independent point reads or one
cross-object snapshot barrier. The v0.2 exact-ID baseline requires a complete
bounded response but does not claim a cross-object transaction.

`CompareAndSet` is an Application/storage boundary for assigning authoritative
versions; STATE_UPDATE itself is server-to-client only. Implementations must
preserve version high-water across deletion. They must document concurrency and
copy ownership, but KMTProto does not provide a database, transaction manager,
or multi-node guarantee.

#### Snapshot generation

Snapshot generation should:

1. validate and canonicalize exact-ID selectors;
2. enforce selector, object, and byte limits before/while loading;
3. call GetMany without holding protocol-state locks;
4. generation-fence the result on return;
5. ensure each requested identity appears exactly once as object or missing;
6. validate every returned State Object;
7. sort results deterministically;
8. produce one immutable snapshot result.

#### Phase 3 tests

- valid object and tombstone validation;
- version zero and exhaustion rejection;
- newer, stale, duplicate, and conflict merge cases;
- equivalent JSON under the chosen equality rule;
- complete snapshot validation before cache mutation;
- cache object/byte bounds;
- State `version` never changes EVENT `last_seq`;
- concurrent CompareAndSet has exactly one winner for one expected version;
- concurrent reads return defensive copies;
- property tests prove cached version never decreases.

Exit gate: State Core can be tested without Envelope, Client connection, Server,
or network helpers.

### Phase 4 — State Frames

Complexity: **Large**.
Dependency: Phases 1–3.

Goal: add the `state-sync` Capability version 1 and its three Frames without
changing EVENT semantics.

#### STATE_QUERY

Direction: Client -> Server.

Payload:

- one or more exact-ID selectors;
- each selector contains a namespace and one or more object IDs.

Validation and admission:

- Envelope ID and Session ID required;
- `reply_to` absent and `seq == 0`;
- `state-sync` version 1 accepted;
- current generation and READY state required;
- no EVENT Replay in progress;
- selectors and total IDs are unique, non-empty, and bounded;
- payload and expected response fit negotiated limits.

Success: exactly one correlated STATE_SNAPSHOT. Failure: one correlated ERROR.
The query creates no durable subscription.

#### STATE_SNAPSHOT

Direction: Server -> Client.

Payload:

- complete State Objects;
- explicit missing identities.

Validation and admission:

- Envelope ID, Session ID, and `reply_to` required;
- `seq == 0`;
- `reply_to` matches one outstanding query in the current generation;
- every requested identity appears exactly once as object or missing;
- no unrequested or duplicate identity appears;
- the complete payload is within object and byte limits;
- all objects validate before any cache mutation.

Success: merge the snapshot atomically under State version rules, clear the
pending query, and produce State result Actions. A response from an old
generation or unknown query cannot mutate the cache.

#### STATE_UPDATE

Direction: Server -> Client only.

Payload: exactly one complete committed State Object.

Validation and admission:

- Envelope ID and Session ID required;
- `reply_to` absent and `seq == 0`;
- `state-sync` version 1 accepted;
- current generation and READY state required;
- object and frame are within negotiated limits.

Success: apply the common merge function. Older update is ignored; equal
identical update is a safe duplicate; equal conflicting update is a protocol
violation; newer update replaces the cache.

There is no client STATE_UPDATE and no STATE_ACK. Client mutation intent remains
SEND, and missed live State is repaired by a future query.

#### Actions and public commands

Likely additions:

- a Client command to issue an exact-ID State query;
- an Action reporting a completed snapshot;
- an Action reporting an installed State replacement or tombstone;
- a Server method to publish an already committed State Object;
- a server-side query handler boundary that loads exact IDs after admission.

Actions contain immutable copies. They never execute Application callbacks.

#### Phase 4 tests

- structural validation for all three Frames;
- direction and state admission;
- Capability-required admission;
- query/snapshot correlation;
- unrequested, duplicate, or missing result identity;
- stale update rejection and equal-version conflict;
- oversized query, object, and snapshot;
- no partial cache application;
- old-generation snapshot/update ignored or rejected without mutation;
- server cannot accept client STATE_UPDATE;
- State Frames never enter Replay Store.

Exit gate: exact-ID State synchronization works deterministically on one READY
connection and remains fully independent of EVENT sequence.

### Phase 5 — EVENT and STATE Integration

Complexity: **Medium to Large**.
Dependency: Phase 4.

Goal: prove that EVENT history and STATE current-value synchronization coexist
without sharing ordering state.

Integration rules:

- EVENT remains append-only, Session ordered, and replayable;
- STATE remains replaceable, object-versioned, and non-replayable;
- State Frames always have `seq == 0`;
- `last_seq` changes only after validated EVENT delivery;
- State version changes only through State merge;
- the same outbound queue serializes Frames, but wire order is not an atomic
  EVENT/STATE transaction guarantee;
- an Application may commit EVENT and State together in its own storage, but
  KMTProto does not require or implement that transaction;
- a STATE_UPDATE racing with an older STATE_SNAPSHOT is resolved by State
  version, not arrival order;
- heartbeat and ERROR handling cannot advance either ordering domain.

The existing Session stream lane continues to serialize Replay with live EVENT
publication. STATE_UPDATE does not enter that EVENT lane solely to acquire an
EVENT order. It still uses the connection's single outbound queue. If an
Application requires a chosen publication order between one EVENT and one State
replacement, it coordinates its own publication calls without treating that
order as an atomic protocol guarantee.

Integration tests:

- only EVENT changes `last_seq`;
- only committed State Objects change cached State versions;
- EVENT gap while State traffic exists never leaks an EVENT;
- State update does not fill or hide an EVENT gap;
- snapshot/update race selects the higher State version;
- ACK, EVENT, and STATE_UPDATE concurrent enqueue remains frame-serialized;
- same EVENT sequence conflict remains fatal regardless of State traffic;
- same State version conflict remains fatal regardless of EVENT traffic;
- disconnect leaves delivered EVENT position unchanged and marks State cache
  according to the documented policy.

Exit gate: property tests can vary EVENT and State operation interleavings while
proving both monotonic domains independently.

### Phase 6 — Resume Integration

Complexity: **Large**.
Dependency: Phases 1, 2, 4, and 5.

Goal: implement the complete v0.2 reconnect flow without embedding State into
EVENT Replay.

#### Considered strategies

| Strategy | Result | Decision |
|---|---|---|
| A: Resume only EVENT | safe but State may remain stale indefinitely | insufficient as complete v0.2 guidance |
| B: EVENT plus automatic State snapshot | unbounded and couples authorization/visibility to Resume | reject |
| C: EVENT Replay then client-selected State query | bounded and application-directed | adopt |

#### Resume flow

```text
new transport / new generation
  -> HELLO(existing session_id, offered capabilities, receive limits)
  -> WELCOME(RESUME_REQUIRED, accepted capabilities, limits)
  -> RESUME(last_seq)
  -> RESUME_OK(resume_from, replay_to)
  -> validate and buffer EVENT resume_from..replay_to
  -> atomically deliver complete Replay
  -> READY
  -> STATE_QUERY(exact application-selected identities)
  -> STATE_SNAPSHOT
```

RESUME_OK rules:

- Envelope ID, Session ID, and `reply_to` required;
- `reply_to` matches the current RESUME request;
- fixed `resume_from` and `replay_to` required, including an empty range;
- no EVENT is accepted as Replay before RESUME_OK;
- live EVENT cannot interleave before `replay_to`;
- partial Replay is never delivered;
- State query is rejected until EVENT Replay completes and READY is restored.

State cache behavior across disconnect must be explicit:

- cached values may remain readable as STALE application data;
- they are not silently declared CURRENT after Resume;
- explicit State queries move selected identities through SYNCING to CURRENT;
- a current-generation STATE_UPDATE may refresh an identity after READY;
- Session abandonment clears Session-bound protocol cache metadata.

Phase 6 tests:

- new transport always requires HELLO before RESUME;
- WELCOME/RESUME/RESUME_OK correlation and state transitions;
- fixed Replay boundary and original EVENT identity;
- empty Replay reaches READY deterministically;
- partial Replay disconnect and second Resume;
- State query rejected before Replay completion;
- State query emitted only after READY;
- snapshot/update races after Resume;
- smaller negotiated Replay/snapshot limits on reconnect;
- missing required Session Capability rejects Resume;
- old WELCOME, RESUME_OK, EVENT, snapshot, update, PONG, and ERROR cannot mutate
  the current generation.

Exit gate: reconnect deterministically restores EVENT continuity first and
enables bounded State refresh second.

### Phase 7 — Error Evolution

Complexity: **Medium**.
Dependency: Phases 1, 4, and 6.

Goal: make v0.2 error codes, retryability, correlation, and connection
disposition unambiguous.

Adopt `UNSUPPORTED_FEATURE` for an unsupported required Capability during
negotiation. It is non-retryable for that HELLO and closes the connection.

The following proposed codes should not automatically become wire codes because
the approved design already provides more precise behavior:

| Candidate | Recommended v0.2 treatment |
|---|---|
| `CAPABILITY_REQUIRED` | local API error before send; peer use of an unaccepted Capability is `PROTOCOL_VIOLATION` |
| `INVALID_STATE_VERSION` | local validation/merge error; conflicting authoritative wire State is `PROTOCOL_VIOLATION` |
| `STATE_NOT_FOUND` | represent exact query misses in STATE_SNAPSHOT `missing`; otherwise use generic `NOT_FOUND` |

Adding one of these wire codes requires amending the design before code, not an
implementation-only decision.

v0.2 ERROR correlation uses Envelope `reply_to`; payload `ref_id` is removed.
An optional `retry_after_ms` is meaningful only for retryable failures such as
RATE_LIMITED.

Required behavior:

| Code | Retryable | Default disposition |
|---|---:|---|
| `BAD_REQUEST` | no | reject operation; connection may stay open |
| `UNSUPPORTED_VERSION` | no | fail HELLO and close |
| `UNSUPPORTED_FEATURE` | no | fail required Capability negotiation and close |
| `UNAUTHORIZED` | no | Application policy; handshake failure normally closes |
| `INVALID_SESSION` | no | abandon resumable Session context |
| `NOT_FOUND` | no | reject referenced resource; keep connection open |
| `RATE_LIMITED` | yes | keep connection open; honor retry hint |
| `SYNC_REQUIRED` | no automatic Resume | stop Replay and notify Application full sync is needed |
| `INTERNAL` | explicit per failure | never imply SEND commit success |
| `PROTOCOL_VIOLATION` | no | close connection |

No ERROR may trigger ERROR-about-ERROR. Failure after Application success but
before SEND Complete remains an indeterminate commit state and must not be
converted into an ACK or automatic duplicate execution.

Phase 7 tests:

- code/retryability/disposition table;
- ERROR `reply_to` validation;
- pre-Session errors without Session ID;
- negotiation failure closes before Session creation;
- unaccepted State Frame causes protocol violation;
- invalid ERROR never creates another ERROR;
- error from stale generation cannot close the current connection;
- Complete failure never emits ACK;
- snapshot missing is not treated as a connection error.

Exit gate: every validation and state-machine failure has one documented wire
or local error and deterministic connection behavior.

### Phase 8 — Testing, Hardening, and Release Gate

Complexity: **Large**.
Dependency: all previous phases.

Phase 8 is not a feature phase. It verifies that incremental work composes into
one reliable v0.2 protocol.

Required validation layers:

1. **Wire fixtures** — canonical valid and invalid JSON for all frames.
2. **Unit tests** — validators, registry, limits, State merge, error behavior.
3. **State-transition tests** — all legal and illegal Client/Server transitions.
4. **Invariant/property tests** — independent EVENT and State monotonicity.
5. **Concurrency/race tests** — handshake, queries, updates, Resume, SEND.
6. **Failure injection** — storage/callback/enqueue failures and generation
   replacement.
7. **Fuzz tests** — Envelope, payload, Capability parameters, State Objects,
   selectors, and snapshots.
8. **Example/interoperability tests** — one in-memory deterministic v2 flow,
   not a WebSocket server.

Release commands:

```text
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go test -run=^$ -fuzz=Fuzz -fuzztime=10s ./...
go run ./examples/basic
```

Fuzz targets may require package-specific commands. CI should run stable unit,
race, and fixture tests; bounded fuzz smoke tests may be separate if necessary.

Release exit criteria:

- Wire Version 2 is the only active grammar;
- capability negotiation is deterministic;
- no unaccepted Capability can mutate protocol state;
- SEND/ACK commit ordering remains unchanged;
- EVENT Replay remains bounded, fixed, and identity-preserving;
- State cache version never decreases;
- no partial EVENT Replay or State snapshot reaches the Application;
- heartbeat cannot mutate EVENT or State ordering;
- stale generations cannot mutate negotiated, heartbeat, EVENT, or State state;
- all public concurrency contracts are documented;
- no lock spans blocking I/O or callbacks;
- vet, tests, race tests, fuzz smoke, example, and CI pass.

## 4. File/package impact

Keep one `kmtproto` package for v0.2. Do not create transport, database, or
business subpackages. Add focused files only where they isolate a stable
protocol concept.

| File | Planned impact |
|---|---|
| `types.go` | Wire Version 2, Envelope `ReplyTo`, v2 Frame model, connection/admission enums |
| `payload.go` | v2 HELLO/WELCOME, RESUME_OK, State, and revised Error payloads |
| `capability.go` (new) | names, offers, selections, immutable registry, negotiation |
| `limits.go` | negotiation offers, directional effective limits, State/Capability bounds |
| `validate.go` | v2 structural validation and frame-schema dispatch |
| `admission.go` (optional new) | context-aware state/Capability/correlation admission if `validate.go` becomes unclear |
| `codec.go` | Codec interface should remain unchanged |
| `json_codec.go` | Wire v2 strict decoding and structural Encode/Decode bounds |
| `client.go` | unified HELLO, negotiated context, reply correlation, Resume flow, State commands/cache integration |
| `server.go` | HELLO-first negotiation, capability-aware admission, RESUME_OK, State query/publication |
| `session.go` (new if useful) | protocol-only Session metadata and ownership/lifetime helpers |
| `state.go` (new) | StateObject, identity, validation, equality, version merge |
| `state_cache.go` (new) | bounded client cache and atomic snapshot application |
| `store.go` | evolved SessionRepository and protocol-facing StateStore contracts |
| `error.go` | v2 codes, disposition, local versus wire errors |
| `action.go` | negotiation and State result Actions |
| `outbound.go` | preserve queue/writer model; use v2 structural validation |
| `id.go` | injectable Frame ID generation and global uniqueness contract |
| `clock.go` | no semantic change; continue deterministic time injection |
| `doc.go` | package scope updated from messaging to synchronization |
| `examples/basic/main.go` | final deterministic v2 example after protocol phases pass |

Test files should follow concepts rather than accumulate all cases in one file:

- `capability_test.go`;
- `handshake_test.go`;
- `state_test.go`;
- `state_cache_test.go`;
- `resume_v2_test.go`;
- existing Client, Server, Codec, queue, race, and hardening tests updated for v2.

Reference helpers such as Memory stores, OutboundQueue, SingleWriter, and
ServerConnection remain optional process-local helpers. Do not add cluster,
persistence, routing, or network lifecycle responsibilities to them.

## 5. Public API changes

v0.2 intentionally changes the public API once at the foundation boundary.
Exact Go signatures should be frozen in Phase 0 before implementation.

Expected breaking changes:

- Envelope gains `ReplyTo` and requires IDs on all Frames;
- `WireVersionV1` is replaced as the active version by Wire Version 2;
- ACK, PONG, and ERROR payload correlation fields disappear;
- HELLO/WELCOME payloads and WELCOME modes change;
- RESUME receives an explicit RESUME_OK response;
- Client's separate v0.1 StartSession/Resume entry should become one HELLO-first
  handshake command;
- ClientConfig gains client metadata, Capability Offers, receive limits, Frame
  ID generation, and State cache/query limits;
- ServerConfig gains supported capabilities, directional limit policy, and
  Frame ID generation;
- Server construction needs State query dependencies without repeatedly
  lengthening positional arguments;
- SessionRepository evolves from existence checks to protocol Session metadata;
- Client gains State query/accessor commands;
- Server gains committed State publication;
- Actions gain immutable negotiation and State results.

To limit future churn:

- prefer `ClientConfig`, `ServerConfig`, and a `ServerDependencies` struct over
  new positional constructor parameters;
- validate and deep-copy all caller-owned slices, maps, and RawMessage values;
- expose negotiated context through read-only accessors;
- keep Codec interface stable;
- keep ApplicationHandler SEND contract and message ID idempotency key stable;
- keep ReplayStore and EventAppender semantics stable unless Wire v2 identity
  requires a clearly documented signature change;
- avoid exporting intermediate scaffolding that is not intended to survive the
  v0.2 release.

Once v0.2.0 is released, an existing Capability version is immutable. Breaking
extension changes require a new Capability version; breaking base grammar
changes require a later wire version.

## 6. Data model changes

| Concern | v0.1 | v0.2 |
|---|---|---|
| Wire version | 1 | 2 only |
| Frame ID | SEND/EVENT-focused | required on every Frame |
| Correlation | payload `ref_id`/`ping_id` | Envelope `reply_to` |
| HELLO | new Session only, client name | every connection, version/capabilities/limits |
| WELCOME | NEW or overloaded RESUMED | NEW or RESUME_REQUIRED negotiation result |
| Resume acknowledgement | WELCOME(RESUMED) | RESUME_OK |
| Capability state | none | immutable per generation |
| Session metadata | ID and TTL existence | ID, TTL, resume-critical Capability requirements |
| Limits | local configuration | local ceiling plus negotiated directional limits |
| EVENT | Session `seq` | semantic behavior preserved |
| State | absent | object identity, version, tombstone, complete data |
| Error correlation | Error payload RefID | Envelope `reply_to` |

State structures must distinguish:

- Envelope Frame ID;
- State Object ID;
- EVENT ID;
- SEND message ID;
- request `reply_to` correlation.

Naming in Go should make accidental substitution difficult even where the wire
uses the generic JSON key `id`.

## 7. Migration considerations

There is no runtime v0.1-to-v0.2 migration layer.

Implementation and release considerations still exist:

- complete the v2 cutover on a feature branch; do not merge a branch that
  accepts an incoherent mix of v1 and v2;
- preserve v0.1 documentation and tag/history for reference;
- update examples and README claims only when the v2 release gate passes;
- treat old in-memory Sessions and outboxes as non-resumable after process
  upgrade;
- any future durable deployment must explicitly version stored Session,
  deduplication, Replay, and State records;
- do not infer Capability acceptance from the presence of a Frame;
- do not silently translate v0.1 ACK/PONG/ERROR correlation into v0.2;
- do not reuse v0.1 Session IDs across the version boundary unless deployment
  policy can prove all stored metadata is v2-compatible.

Backward compatibility **within v0.2** is maintained through:

- immutable wire version 2 base semantics;
- explicit Capability versions;
- deterministic optional Capability omission;
- new Capability versions for breaking extension changes;
- additive configuration fields with stable defaults;
- golden JSON fixtures and interoperability tests.

Recommended release progression:

```text
implementation branch -> v0.2 alpha fixtures -> API freeze -> release candidate
-> race/fuzz/interoperability gate -> v0.2.0
```

## 8. Test plan

### 8.1 Capability tests

- valid, empty, duplicate, unknown, optional, and required offers;
- version intersection and mismatch;
- canonical accepted ordering;
- deterministic parameter negotiation;
- size/count limits;
- unaccepted Feature Frame admission;
- immutable selection and defensive copies;
- concurrent registry negotiation.

### 8.2 Handshake tests

- new Session HELLO/WELCOME;
- existing Session HELLO/WELCOME(RESUME_REQUIRED);
- unsupported wire version;
- unsupported required Capability;
- invalid server selection;
- correlation mismatch;
- HELLO sent twice;
- non-HELLO first frame;
- late negotiation completion from old generation.

### 8.3 Reliable SEND regression tests

- retry uses the same SEND ID;
- duplicate PROCESSING never calls Application twice;
- duplicate COMPLETED returns the original ACK;
- Claim -> Application -> Complete -> ACK ordering;
- failure at each existing injection point;
- v2 ACK `reply_to` correlation;
- Capability and State traffic cannot weaken SEND deduplication.

### 8.4 EVENT tests

- sequence begins at 1 and is Session scoped;
- duplicate identity versus conflicting identity;
- gap never advances `last_seq`;
- Replay fixed boundary and high-water retention;
- partial Replay never delivers;
- RESUME_OK correlation;
- State Frames never enter Replay or consume sequence.

### 8.5 State tests

- object, tombstone, namespace, ID, data, and size validation;
- monotonic versions and legal jumps;
- stale rejection;
- equal-version duplicate and conflict;
- canonical data equality;
- exact-ID selector validation;
- snapshot completeness and atomic cache application;
- missing identities;
- State update versus snapshot race;
- cache bounds and tombstone retention;
- version exhaustion.

### 8.6 Resume tests

- HELLO always precedes Resume;
- EVENT Replay completes before State query;
- State is not embedded in Replay;
- State remains STALE until explicitly refreshed;
- repeated disconnect during Replay;
- changed capabilities and smaller limits on reconnect;
- Session-required Capability mismatch;
- all stale-generation Frame classes are fenced.

### 8.7 Concurrency tests

- concurrent capability negotiation reads;
- two HELLO frames on one connection;
- PING plus SEND plus State query;
- multiple outstanding exact-ID queries with distinct correlations;
- concurrent STATE_UPDATE and STATE_SNAPSHOT;
- concurrent State Store CompareAndSet;
- Resume plus live EVENT plus State publication;
- connection replacement while old callbacks finish;
- OutboundQueue serialization across ACK/EVENT/State/Error.

### 8.8 Property and fuzz tests

Properties:

- EVENT `last_seq` never decreases;
- only EVENT changes `last_seq`;
- cached State version never decreases;
- State traffic never changes EVENT sequence;
- EVENT traffic never assigns State version;
- same `(session, seq)` never maps to two event IDs;
- same `(namespace, object_id, version)` never maps to two semantic values;
- accepted capabilities never change within a generation;
- stale generations never mutate current state;
- no snapshot or Replay is partially delivered.

Fuzz:

- Envelope and all typed payloads;
- Capability names, version lists, and parameter JSON;
- directional limits;
- State Objects and selectors;
- snapshot completeness/duplicate identities;
- equal-version semantic JSON comparison;
- malformed and oversized encoded/decompressed inputs.

Requirements: never panic, deadlock, regress sequence/version, or allocate
without configured bounds.

## 9. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| partial v1/v2 correlation cutover | ACK/PONG/ERROR matched incorrectly | one Phase 0 wire cutover plus golden fixtures |
| mixing structural validation with Session admission | Codec depends on mutable connection state | two-layer validator design |
| mutable Capability result | behavior changes mid-generation or races | immutable snapshots and defensive copies |
| Capability name treated as implementation | presence/compression/encryption implied but absent | registry entry requires an explicit versioned spec |
| Session stores negotiated limits | reconnect cannot safely negotiate smaller limits | limits remain generation scoped |
| Session repository absorbs Application identity | protocol/business boundary is lost | minimal SessionRecord only |
| State version compared with EVENT seq | false gaps or lost current values | separate types, merge paths, and invariant tests |
| raw JSON equality | false conflict or missed conflict | freeze canonical semantic comparison before Phase 3 |
| snapshot partially applied | client cache becomes internally inconsistent | validate full result, then atomically merge |
| update races with snapshot | older snapshot overwrites live State | common version merge for both paths |
| namespace-wide query added early | unbounded memory and ambiguous snapshot cut | exact-ID baseline only |
| automatic State in Resume | unbounded recovery and authorization coupling | adopt explicit post-Replay query |
| State Store interface implies database | project scope expands | interface contract only; helpers remain reference-only |
| State cache grows forever | client memory exhaustion | object/byte/tombstone bounds |
| old generation async completion | current negotiated or State state corrupted | generation fence before every mutation |
| locks span Store/Application calls | deadlock and latency amplification | compute actions under lock; perform I/O after unlock |
| new error codes duplicate existing semantics | inconsistent retry and connection behavior | adopt only UNSUPPORTED_FEATURE without design amendment |
| constructor churn across phases | repeated public API breaks | freeze config/dependency structs in Phase 0 |

The highest-risk implementation areas are Phase 0 correlation/validation,
Phase 1 handshake admission, Phase 3 State equality and atomic snapshot merge,
and Phase 6 generation-fenced Resume.

## 10. Open questions

### Blocking before Phase 0

1. Is every Frame ID globally unique? Recommendation: yes, generated as ULID.
2. What exact public Client handshake API replaces StartSession/Resume?
   Recommendation: one HELLO-first command that uses the Client's current
   Session ID.
3. Which capabilities are resume-critical Session requirements?
   Recommendation: each CapabilitySpec declares this; do not persist all
   accepted optional capabilities automatically.
4. Does the initial Server constructor move to a ServerDependencies struct?
   Recommendation: yes, to avoid repeated positional breaking changes.

### Blocking before Phase 3

5. What is the canonical State data equality algorithm?
6. What object-count, byte, and tombstone/version verification bounds are the
   v0.2 defaults?
7. Does GetMany promise independent point reads or a cross-object snapshot
   barrier? Recommendation: independent exact-ID point reads for baseline v0.2.

### Non-blocking or deferred

8. Namespace-wide State snapshots, snapshot cursors, and pagination: defer to a
   future Capability version.
9. Live State subscriptions: keep routing Application-owned; design a future
   Capability only if consumers require explicit subscription semantics.
10. Presence: model as opaque State/application semantics unless a distinct
    protocol extension is justified.
11. Compression and encryption: registry-ready but not implemented until their
    framing and activation specifications are independently reviewed.
12. STATE_UPDATE while SUSPECT: baseline recommendation is reject/defer State
    traffic outside READY.
13. Durable State cache and durable client outbox: remain outside protocol core.
14. Multi-node State Store or Session ownership: remain caller/runtime concerns.

## 11. Dependency order and complexity summary

| Phase | Depends on | Complexity | Main risk |
|---|---|---:|---|
| 0. Foundation | design decisions | Large | coherent wire/API cutover |
| 1. HELLO/WELCOME | 0 | Large | negotiation and handshake admission |
| 2. Session capability state | 1 | Medium | state ownership/lifetime |
| 3. State core | 2 | Large | equality, versions, atomic snapshot |
| 4. State Frames | 1–3 | Large | admission, correlation, limits |
| 5. EVENT/STATE integration | 4 | Medium–Large | independent monotonic domains |
| 6. Resume integration | 1, 2, 4, 5 | Large | replay boundary and generation races |
| 7. Error evolution | 1, 4, 6 | Medium | retry/disposition consistency |
| 8. Hardening/release | all | Large | concurrency and composition failures |

Overall complexity is **Extra Large**, driven by correctness and test surface,
not by business features or infrastructure. The work should be estimated and
reviewed per phase rather than scheduled as one rewrite.

## 12. Minimal-risk evolution summary

The minimum-risk path from v0.1 reliable messaging to v0.2 synchronization is:

1. make one deliberate Wire Version 2 Envelope/correlation cutover;
2. establish deterministic Capability negotiation before optional Frames;
3. give every negotiated value one clear owner and lifetime;
4. implement State identity/version behavior as a pure independently tested
   core;
5. add bounded exact-ID State Frames only after that core is stable;
6. prove EVENT sequence and State version remain independent;
7. recover EVENT first and explicitly query State second;
8. finish with failure, concurrency, race, property, and fuzz gates.

This evolves KMTProto into a synchronization protocol without turning it into a
transport server, database, authentication system, conflict-resolution engine,
or business workflow runtime.
