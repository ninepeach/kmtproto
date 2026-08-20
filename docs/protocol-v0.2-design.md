# KMTProto v0.2 — Protocol Evolution Design Proposal

Status: **HISTORICAL PROPOSAL / SUPERSEDED**

Target: KMTProto v0.2 design review

Baseline: a new protocol generation with no v0.1 compatibility mode

Implementation note: the implemented protocol selected a single Wire Version
2 baseline while retaining the existing Frame-specific correlation and
WELCOME(RESUMED) model. The authoritative implemented contract is
`docs/protocol-v0.2.md`; this proposal is retained only as design history.

This document originally proposed KMTProto v0.2 as a transport-independent chat
synchronization protocol. It is intentionally a design artifact: the frame
names, schemas, transitions, limits, and interfaces below describe that larger
cutover and are not a statement that every item is implemented.

Normative terms such as MUST, MUST NOT, SHOULD, and MAY describe the proposed
v0.2 contract.

## 1. Motivation

KMTProto v0.1 establishes a small, reliable protocol core:

- `SEND`/`ACK` provide retryable logical submission;
- `EVENT` provides a Session-scoped ordered history stream;
- `RESUME` and Replay repair EVENT gaps after connection loss;
- `PING`/`PONG` provide application-level liveness;
- generation fencing prevents stale connections from mutating current state.

Two concerns remain outside that baseline.

First, a modern protocol needs an explicit way to agree on optional behavior.
Adding fields or Frame Types without negotiation makes unsupported behavior
ambiguous and encourages implicit downgrade.

Second, ordered history does not answer the question “what is the current
value?” Reconstructing message status, delivery state, presence, task state, or
workflow state by replaying all past events is inefficient and may be impossible
after history retention expires.

v0.2 therefore unifies two additions:

1. capability and limit negotiation during every transport handshake; and
2. business-blind current-state synchronization alongside, but never inside,
   the EVENT stream.

The core distinction is normative:

| Model | Question answered | Identity/order | Recovery |
|---|---|---|---|
| Reliable SEND | Was this logical submission accepted? | `(session_id, message_id)` | Retry the same ID |
| EVENT | What happened, and in which order? | Session-scoped `seq` plus immutable event ID | Fixed-boundary replay |
| STATE | What is the authoritative value now? | `(namespace, object_id, version)` | Bounded snapshot |

## 2. Goals

KMTProto v0.2 aims to provide:

- one deterministic wire grammar for the next protocol generation;
- explicit version, capability, parameter, and limit negotiation;
- a required HELLO/WELCOME handshake on every physical transport;
- reliable SEND retry with the existing idempotency boundary;
- a per-Session, ordered, replayable EVENT stream;
- bounded and deterministic Resume/Replay;
- heartbeat semantics isolated from message and event ordering;
- generic, server-authoritative State Objects;
- deterministic handling of stale, duplicate, and conflicting State versions;
- explicit recovery ordering between EVENT and STATE;
- strict validation and bounded resource use;
- transport-, storage-, and business-independent protocol semantics;
- state machines that can be tested without real networks or wall-clock sleeps.

## 3. Non-goals

KMTProto v0.2 does not define or implement:

- WebSocket, TCP, QUIC, HTTP, or any other transport lifecycle;
- a chat server, Gateway, connection registry, router, or connection pool;
- a database, Redis adapter, durable outbox, cache, or storage engine;
- distributed Session ownership, distributed locking, consensus, or cluster
  coordination;
- authentication implementation or application authorization policy;
- message, read-receipt, typing, presence, task, or workflow business logic;
- CRDTs, vector clocks, field-level merging, or conflict resolution;
- UI behavior, Message Center, BigCC, NLX, or application workflows;
- global exactly-once side effects or cross-service transactions;
- permanent offline synchronization, history APIs, State pagination, or a
  general flow-control subsystem;
- a v0.1 compatibility, migration, downgrade, or dual-protocol mode.

In-memory stores, queues, and connection helpers may be supplied later as
reference implementations. They are not requirements of the wire protocol and
MUST NOT be described as production or multi-process infrastructure.

## 4. Protocol Architecture

v0.2 has four semantic layers plus a cross-layer error mechanism:

| Layer | Responsibility | Proposed frames |
|---|---|---|
| Connection | Negotiation and liveness | `HELLO`, `WELCOME`, `PING`, `PONG` |
| Reliable Message | Retryable client submission | `SEND`, `ACK` |
| Event Stream | Ordered history and recovery | `EVENT`, `RESUME`, `RESUME_OK` |
| State Synchronization | Current-value query and publication | `STATE_QUERY`, `STATE_SNAPSHOT`, `STATE_UPDATE` |
| Error | Structured failure and disposition | `ERROR` |

The layers share one Session and one serialized outbound path, but their
ordering domains do not merge:

- only EVENT consumes `seq`;
- SEND idempotency is keyed by message ID, not EVENT position;
- State versions are per object, not per Session stream;
- PING/PONG never affects EVENT or STATE ordering;
- ERROR is never replayed.

### 4.1 Session and connection

A Session is a logical synchronization context that can outlive multiple
physical transports. It is not a user, account, room, conversation, or network
connection.

Every transport replacement creates a new connection generation. The caller
MUST associate each incoming frame and asynchronous completion with its
generation. Once a newer generation is active, work from an older generation
MUST NOT change:

- connection or heartbeat state;
- negotiated capabilities or limits;
- Session identity;
- EVENT `last_seq` or replay state;
- State cache contents or State synchronization phase;
- the active outbound queue.

KMTProto validates generation labels; it does not own transports or a
production connection registry.

### 4.2 Connection state

The client connection states remain conceptually:

```text
DISCONNECTED -> CONNECTING -> CONNECTED -> HANDSHAKING
HANDSHAKING  -> READY                    (new Session)
HANDSHAKING  -> RESUMING                 (existing Session)
RESUMING     -> READY                    (complete EVENT replay)
READY        -> RESUMING                 (EVENT gap)
READY        -> SUSPECT -> READY          (matching PONG)
READY/SUSPECT/RESUMING -> DISCONNECTED    (disconnect or fatal error)
```

State cache health is orthogonal and SHOULD be modeled independently:

```text
UNKNOWN -> SYNCING -> CURRENT
              |          |
              +-> STALE <-+
```

`CURRENT` does not make the transport READY, and READY does not imply that every
application State Object is cached.

### 4.3 New Session handshake

For a new Session:

```text
CONNECTED
  -> send HELLO(version, capabilities, receive limits; no session_id)
  -> HANDSHAKING
  -> receive WELCOME(mode=NEW, session_id, selected capabilities, limits)
  -> READY
```

### 4.4 Existing Session handshake and recovery

Every replacement transport begins with HELLO, including one that will resume
an existing Session:

```text
CONNECTED
  -> send HELLO(existing session_id, version, capabilities, receive limits)
  -> HANDSHAKING
  -> receive WELCOME(mode=RESUME_REQUIRED, negotiated result)
  -> send RESUME(last_seq)
  -> RESUMING
  -> receive RESUME_OK(resume_from, replay_to)
  -> receive the complete fixed EVENT replay
  -> atomically deliver replay and enter READY
  -> optionally issue STATE_QUERY
```

`RESUME_OK` is proposed instead of overloading WELCOME as both capability
handshake and replay-boundary acknowledgement. This produces one meaning per
frame and makes invalid state/frame combinations deterministic.

## 5. Version Strategy

### 5.1 Recommendation: Wire Version 2

v0.2 SHOULD use `v = 2` and become the only supported grammar in the first
v0.2 implementation.

Keeping `v = 1` and relying only on capabilities is not recommended. A
capability can enable an optional feature, but it cannot safely redefine base
requirements such as:

- every connection beginning with HELLO;
- universal request/response correlation;
- new WELCOME and Resume semantics;
- frame admission based on the negotiated capability set;
- strict handling of newly proposed State frames.

Wire version and capability solve different problems:

- **wire version** selects the mandatory envelope grammar, core frames, and
  state-machine semantics;
- **capability** selects an optional extension, its version, and its parameters
  within that grammar.

### 5.2 Version selection

The v0.2 HELLO carries `protocol_version: 2`. The server either selects exactly
that version or returns `UNSUPPORTED_VERSION` and closes the connection.

No automatic downgrade is allowed. A future multi-version implementation could
offer an ordered version set, but that is outside this baseline and MUST NOT be
inferred from v0.2.

Because there are no production consumers or public clients, this clean break
is preferable to carrying a migration layer that would itself need long-term
testing and security support.

## 6. HELLO/WELCOME Capability Negotiation

### 6.1 HELLO

Direction: Client -> Server. HELLO is the first frame on every transport.

Conceptual payload:

```json
{
  "protocol_version": 2,
  "client": {
    "name": "example-client",
    "version": "0.2.0"
  },
  "capabilities": [
    {
      "name": "state-sync",
      "versions": [1],
      "required": true,
      "parameters": {}
    },
    {
      "name": "compression.zstd",
      "versions": [1],
      "required": false,
      "parameters": {}
    }
  ],
  "receive_limits": {
    "max_frame_bytes": 1048576,
    "max_state_snapshot_bytes": 524288
  }
}
```

Envelope rules:

- `v == 2` and `type == HELLO`;
- `id` is required;
- `reply_to` is absent;
- `seq == 0`;
- `session_id` is empty for a new Session and contains the existing Session ID
  when reconnecting;
- payload is required and strictly validated.

HELLO is always encoded in the base JSON format. Compression, encryption, or
other transforms cannot activate until after a successful WELCOME.

### 6.2 WELCOME

Direction: Server -> Client.

Conceptual payload:

```json
{
  "selected_version": 2,
  "mode": "NEW",
  "accepted_capabilities": [
    {
      "name": "state-sync",
      "version": 1,
      "parameters": {
        "query_mode": "exact-id"
      }
    }
  ],
  "limits": {
    "client_to_server": {
      "max_frame_bytes": 1048576,
      "max_state_query_ids": 256
    },
    "server_to_client": {
      "max_frame_bytes": 1048576,
      "max_state_snapshot_bytes": 524288
    }
  }
}
```

Envelope rules:

- `v == 2` and `type == WELCOME`;
- `id`, `reply_to`, and `session_id` are required;
- `reply_to` equals the HELLO ID;
- `seq == 0`;
- `mode` is `NEW` or `RESUME_REQUIRED`;
- the selected version equals 2;
- accepted capabilities and effective limits are authoritative and immutable
  for this connection generation.

`NEW` creates a Session and enters READY. `RESUME_REQUIRED` recognizes the
existing Session but does not declare it synchronized; the client MUST send
RESUME before normal protocol traffic.

### 6.3 Capability names and offers

Capability names MUST:

- use lowercase ASCII;
- match `[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*`;
- be unique within an offer or selection;
- remain within a negotiated maximum length.

Each offered capability contains a non-empty, duplicate-free set of positive
integer extension versions. A server selects the highest mutually supported
version. Capability-specific parameter negotiation is defined by that
capability's specification; the server's selected parameters in WELCOME are
authoritative.

The accepted capability list MUST be emitted in canonical name order. The
server MUST NOT accept a capability that the client did not offer.

### 6.4 Unsupported behavior

- Required capability with no compatible version: return
  `UNSUPPORTED_FEATURE`, create no new Session, and close.
- Optional capability with no compatible version: omit it from WELCOME and
  continue.
- Frame or field requiring a capability that was not accepted:
  `PROTOCOL_VIOLATION` and close.
- Unknown optional offer: ignore it after validating its structural bounds.
- Unknown required offer: `UNSUPPORTED_FEATURE` and close.

There is no silent fallback from required to optional and no capability change
mid-connection. A resumed Session MAY persist a set of required
Session-semantic capabilities; if a new connection cannot renegotiate them,
Resume fails instead of silently changing Session meaning.

### 6.5 Limit negotiation

HELLO advertises what the client can receive. WELCOME returns effective limits
for each direction. An effective limit MUST NOT exceed the receiving endpoint's
advertised or configured maximum.

Limits are hard ceilings, not promises that an endpoint will allocate that
amount. Missing optional limit fields use protocol defaults. A receiver always
enforces its own smaller safety bound if local policy is stricter.

## 7. Envelope Design

The proposed v0.2 Envelope remains intentionally small:

```json
{
  "v": 2,
  "type": "ACK",
  "id": "01K...",
  "reply_to": "01J...",
  "session_id": "s_123",
  "seq": 0,
  "timestamp": 1786821000180,
  "payload": {}
}
```

Conceptual fields:

| Field | Meaning |
|---|---|
| `v` | Mandatory wire version |
| `type` | Mandatory Frame Type |
| `id` | Mandatory unique frame identity |
| `reply_to` | Correlates a direct response to one request |
| `session_id` | Logical Session context |
| `seq` | EVENT position only; zero/unset otherwise |
| `timestamp` | Optional diagnostic/display metadata |
| `payload` | Frame-specific typed payload |

### 7.1 Correlation

Every v0.2 frame has an ID. A direct response MUST set `reply_to`:

| Response | Request |
|---|---|
| `WELCOME` | `HELLO` |
| `PONG` | `PING` |
| `ACK` | `SEND` |
| `RESUME_OK` | `RESUME` |
| `STATE_SNAPSHOT` | `STATE_QUERY` |
| attributable `ERROR` | offending/request frame |

This replaces v0.1's inconsistent payload-level correlation fields such as
`AckPayload.ref_id` and `PingPayload.ping_id`. A response frame has one
unambiguous correlation mechanism.

Unsolicited `EVENT` and `STATE_UPDATE` omit `reply_to`. An ERROR that cannot be
attributed to one frame MAY omit it.

### 7.2 Field invariants

- only EVENT has `seq >= 1`;
- all non-EVENT frames have `seq == 0`;
- timestamp never participates in ordering, idempotency, capability selection,
  State conflict resolution, Resume, or timeout correctness;
- `session_id` is absent only for a new-Session HELLO and errors emitted before
  a Session is established;
- Envelope and protocol payload fields are strict by default;
- application-owned `SEND.content`, `EVENT.content`, and `StateObject.data`
  remain semantically opaque.

v0.2 SHOULD NOT add a generic top-level extension metadata map. Capability
parameters belong in HELLO/WELCOME, and capability-specific data belongs in a
typed frame payload. An unrestricted metadata bag weakens validation and
creates hidden business coupling.

## 8. Frame Model

The proposed v0.2 frame matrix is:

| Frame | Direction | Required context | Allowed receiver state | Success/effect |
|---|---|---|---|---|
| `HELLO` | C -> S | version, capabilities, receive limits | awaiting HELLO | negotiate |
| `WELCOME` | S -> C | correlation, Session, selected result | HANDSHAKING | READY or require Resume |
| `PING` | either | Session, ID | READY or SUSPECT policy | matching `PONG` |
| `PONG` | either | Session, correlation | READY or SUSPECT | current-generation liveness only |
| `SEND` | C -> S | Session, ID, content | READY | reliable application submission |
| `ACK` | S -> C | Session, correlation | READY or SUSPECT | remove matching outbox entry |
| `EVENT` | S -> C | Session, ID, `seq>=1`, content | READY or expected replay position | ordered delivery |
| `RESUME` | C -> S | Session, ID, `last_seq` | resume admission or RESUMING after gap | establish replay |
| `RESUME_OK` | S -> C | correlation, fixed bounds | RESUMING | admit bounded replay |
| `STATE_QUERY` | C -> S | State capability, selectors | READY | one snapshot |
| `STATE_SNAPSHOT` | S -> C | State capability, correlation, objects | matching query | authoritative query result |
| `STATE_UPDATE` | S -> C | State capability, one object | READY | authoritative live replacement |
| `ERROR` | either | code and disposition | state-specific | deterministic failure action |

Unexpected frames MUST produce a deterministic state error. They are never
silently accepted. An invalid incoming ERROR is logged and ignored or causes a
close; it MUST NOT trigger an ERROR-about-ERROR loop.

### 8.1 Reliable SEND/ACK

v0.2 preserves the v0.1 correctness boundary:

```text
atomic Claim(session_id, message_id)
  -> Application(message_id as idempotency key)
  -> Complete(stored logical ACK)
  -> enqueue ACK(reply_to=message_id)
```

- retries reuse the exact same SEND ID;
- a completed duplicate returns the original logical ACK and never re-executes
  the Application;
- a PROCESSING duplicate never executes the Application again;
- unresolved PROCESSING is an indeterminate commit state and MUST NOT be
  reclaimed merely because time elapsed;
- Complete failure MUST NOT produce a success ACK;
- protocol deduplication does not claim cross-service or global exactly-once
  effects;
- an Application needing end-to-end deduplication MUST durably persist and
  honor the SEND ID as its idempotency key.

ACK correlation moves to Envelope `reply_to`; its payload contains only any
future ACK result defined by the reliable-message layer.

### 8.2 Heartbeat

PING/PONG is application-level heartbeat, independent of transport control
frames. Each PING's Envelope ID is its ping identity; PONG correlates with
`reply_to`.

A PONG affects health only if the ID and connection generation match an
outstanding PING. It is accepted only in the defined READY/SUSPECT liveness
states. PING/PONG does not consume EVENT sequence, enter Replay, update
`last_seq`, or change State versions.

### 8.3 Outbound serialization

All server output for one active connection—WELCOME, PONG, ACK, RESUME_OK,
EVENT, State frames, and ERROR—passes through one outbound serialization point.
Multiple goroutines MUST NOT write directly to the transport.

## 9. EVENT Model

EVENT means **something happened**. It is append-only, replayable, and ordered.

Normative invariants:

1. Each Session owns one independent EVENT sequence.
2. Sequence begins at 1 and increases monotonically by one.
3. Only EVENT consumes sequence.
4. `(session_id, seq)` identifies exactly one immutable event ID.
5. A replayed EVENT preserves its original ID, sequence, type, and content.
6. `last_seq` never decreases.
7. Timestamp never establishes EVENT order.

Client handling:

- `incoming == last_seq + 1`: accept and deliver;
- `incoming <= last_seq` with the same retained event ID: safe duplicate;
- `incoming <= last_seq` with a different event ID: protocol violation;
- identity older than the verification retention window: fail conservatively,
  never silently accept;
- `incoming > last_seq + 1`: enter RESUMING, stop EVENT delivery, and request
  Resume from the last already delivered sequence.

v0.2 does not add an out-of-order EVENT buffer.

### 9.1 Fixed Resume boundary

For `RESUME(last_seq=N)`, the server serializes Resume with live publication:

1. snapshot `replay_to = current_session_seq`;
2. establish `resume_from = N + 1`;
3. validate retention and replay limits;
4. emit `RESUME_OK(resume_from, replay_to)`;
5. emit exactly the original events `resume_from..replay_to`;
6. emit live events created during Replay only after that range.

The client buffers the partial Replay within negotiated event/byte limits and
does not deliver any of it to the Application until the complete fixed range is
present and validated. A disconnect halfway through therefore resumes from the
last sequence previously delivered to the Application, not from a partially
buffered position.

If Replay retention cannot satisfy the range, the server returns
`SYNC_REQUIRED`. Replay high-water is independent of retained history: pruning
all stored events MUST NOT reset the next Session sequence to 1.

## 10. STATE Model

STATE means **what the authoritative value is now**. It is a replacement
snapshot, not append-only history.

### 10.1 Capability

State frames are legal only when the `state-sync` capability is accepted. The
capability version owns the State payload grammar. The first recommended
capability version supports exact-object queries and bounded single-frame
snapshots; namespace-wide scans and pagination are deferred unless explicitly
specified.

### 10.2 State Object

Conceptual model:

```json
{
  "namespace": "message",
  "id": "msg001",
  "version": 5,
  "deleted": false,
  "data": {
    "status": "read"
  }
}
```

Fields:

| Field | Rule |
|---|---|
| `namespace` | Required, opaque application namespace |
| `id` | Required object identity within namespace |
| `version` | Required positive, object-scoped monotonic integer |
| `deleted` | Optional tombstone indicator, default false |
| `data` | Required complete replacement when not deleted; absent/null for tombstone |

State identity is `(namespace, id)`. The Envelope Session is an authorization
and delivery context, not automatically part of object identity. An
application exposing Session-specific representations MUST encode that
distinction in its identity or namespace contract.

State data is always a complete replacement. v0.2 does not define JSON Patch,
field merge, last-write-wins-by-time, or client-authored version increments.

### 10.3 STATE_QUERY

Direction: Client -> Server.

Conceptual payload:

```json
{
  "selectors": [
    {"namespace": "message", "ids": ["msg001", "msg002"]},
    {"namespace": "task", "ids": ["pickup001"]}
  ]
}
```

Rules:

- `id`, `session_id`, payload, and accepted `state-sync` capability required;
- `reply_to` absent and `seq == 0`;
- at least one selector and one exact object ID;
- no duplicate selector or duplicate ID;
- names, IDs, selector count, total ID count, and encoded size are bounded;
- accepted only in READY, with no EVENT replay in progress;
- authorization is evaluated by the Application before revealing existence or
  data.

Expected result is exactly one correlated STATE_SNAPSHOT or one ERROR.
STATE_QUERY is one-shot and does not create a durable subscription.

### 10.4 STATE_SNAPSHOT

Direction: Server -> Client.

Conceptual payload:

```json
{
  "objects": [
    {
      "namespace": "message",
      "id": "msg001",
      "version": 5,
      "deleted": false,
      "data": {"status": "read"}
    }
  ],
  "missing": [
    {"namespace": "message", "id": "msg002"}
  ]
}
```

Rules:

- `id`, `reply_to`, and `session_id` required; `seq == 0`;
- `reply_to` matches exactly one outstanding query on the active generation;
- every returned or missing identity was requested exactly once;
- returned identities are unique and have valid State Objects;
- result object count and encoded bytes are within negotiated limits;
- the complete snapshot is validated before any object is applied;
- applying the snapshot follows the version rules in Section 11.

An exact-ID snapshot gives a point result for each object, but does not promise
one cross-object transaction unless the State Store contract explicitly
provides a snapshot barrier. Missing is data, not a protocol failure.

### 10.5 STATE_UPDATE

Direction: Server -> Client.

Conceptual payload:

```json
{
  "object": {
    "namespace": "task",
    "id": "pickup001",
    "version": 10,
    "deleted": false,
    "data": {"status": "completed"}
  }
}
```

Rules:

- `id` and `session_id` required; `reply_to` absent; `seq == 0`;
- contains exactly one complete committed State Object;
- accepted only for the current generation in READY;
- object and frame are within negotiated limits;
- stale, duplicate, conflict, and newer handling follows Section 11;
- never enters EVENT Replay and never changes EVENT `last_seq`.

There is intentionally no client-to-server STATE_UPDATE. Client mutation intent
uses reliable SEND; only the authoritative side assigns a committed version and
publishes a State replacement. There is also no STATE_ACK: missed State updates
are repaired by a later query/snapshot.

### 10.6 Deletion and tombstones

Deletion is a versioned replacement with `deleted=true`. The client removes the
live value but retains the tombstone identity/version for the configured
verification window. A store MUST retain the object's version high-water long
enough to prevent object recreation from reusing an earlier version.

## 11. State Version Semantics

### 11.1 Version domain

State version is an unsigned positive integer scoped to one
`(namespace, object_id)`:

- the first committed representation uses version 1;
- each committed replacement uses a strictly greater version;
- versions MAY jump; contiguity is not required;
- a version is assigned only after the replacement is authoritatively
  committed;
- version zero is invalid;
- version exhaustion fails closed; it MUST NOT wrap.

State version is independent from EVENT `seq`, SEND ID, timestamp, Session, and
connection generation.

### 11.2 Client merge rules

Given cached version `C` and incoming version `I` for the same object:

| Condition | Required behavior |
|---|---|
| no cached object | validate and install incoming object |
| `I > C` | replace the cached object |
| `I < C` | stale delivery; ignore without regression |
| `I == C`, semantically identical object | safe duplicate; ignore |
| `I == C`, different object | `PROTOCOL_VIOLATION`; do not choose a winner |

No version gap is inferred from `I > C + 1`; State is a current replacement,
not a history stream. `last_state_version` is not global and there is no State
Replay cursor.

Semantic equality requires a canonical representation for State data. The
implementation phase MUST choose and document a deterministic canonical JSON
algorithm or compare the decoded JSON value under a precisely specified rule.
Raw byte equality alone is insufficient because insignificant JSON formatting
may differ.

### 11.3 Concurrent updates

KMTProto does not resolve concurrent business writes. The authoritative
Application/State Store serializes them and assigns versions. A minimal storage
contract requires atomic compare-and-set semantics such as:

```text
Get(namespace, id) -> StateObject or missing
CompareAndSet(namespace, id, expectedVersion, replacement) -> committed object
GetMany(exact selectors) -> bounded snapshot result
```

If two writers both observe version 5, only one compare-and-set may commit the
next authoritative replacement. The loser receives an application/storage
conflict and decides whether to retry business logic. The protocol does not
apply last-arrival-wins and never compares timestamps.

Storage implementations must maintain committed version high-water through
retention and deletion. Interfaces are contracts only; KMTProto does not
provide a database or distributed transaction coordinator.

## 12. Resume Integration

Four recovery strategies were considered.

| Option | Behavior | Advantage | Problem |
|---|---|---|---|
| A | Recover EVENT only | Smallest protocol | State may remain indefinitely stale |
| B | Automatically replay EVENT and all State | One apparent recovery flow | Unbounded, couples unrelated namespaces, leaks visibility policy into Resume |
| C | Recover EVENT, then explicitly query selected State | Bounded, authorized, application-directed | Requires one additional request |
| D | Recover EVENT, then include a client-selected namespace snapshot in the same Resume | One deterministic recovery gate with bounded selectors | Requires a namespace snapshot provider and strict bounds |

### 12.1 Implemented baseline: Option D

v0.2 uses an optional, client-required State phase in RESUME:

1. negotiate v2 and `state-sync` in HELLO/WELCOME;
2. send `RESUME(last_seq, state_sync.namespaces)` when State refresh is required;
3. acknowledge the fixed EVENT boundary and exact namespaces in
   `WELCOME(RESUMED)`;
4. validate and buffer the complete EVENT Replay without application delivery;
5. receive exactly one bounded STATE_SNAPSHOT after the Replay boundary;
6. atomically apply the snapshot under object version rules;
7. release EVENT actions, State actions, and finally READY;
8. merge later STATE_UPDATE frames using the same version rules.

Omitting `state_sync` preserves the v0.1 EVENT-only Resume wire shape and
behavior. Requesting State Sync is a requirement: the server either echoes the
canonical namespace list and supplies the snapshot or returns ERROR. EVENT
Replay identity and `replay_to` are unchanged, and State frames never consume
EVENT sequence.

The server serializes the Resume batch as `WELCOME`, replay EVENT frames, then
STATE_SNAPSHOT. Live EVENT and STATE_UPDATE publication follows that batch.
The client emits no application delivery actions until both requested recovery
phases are complete. A stale or conflicting snapshot fails conservatively and
never advances `last_seq`.

### 12.2 EVENT and STATE consistency

An Application may produce an EVENT such as `message.read` and a State Object
such as `message/msg001.status=read`. v0.2 does not require them to share a
transaction, number, or wire ordering:

- EVENT is authoritative history within its Session stream;
- STATE is authoritative current value within its object version domain;
- EVENT `seq` and State `version` MUST NOT be copied into or compared with each
  other;
- KMTProto does not promise that clients observe the pair atomically;
- an Application requiring atomic business consistency must commit its event
  record and State replacement under its own storage contract before
  publication.

The single outbound writer preserves the chosen emission order on one
connection, but that order is not a cross-model transaction guarantee.

### 12.3 Namespace-wide snapshots

Exact-ID STATE_QUERY remains the normal READY-state query mechanism. Resume may
instead request a bounded, explicit namespace list. The provider returns one
logical snapshot behind the Session serialization lane, subject to
`MaxStateSyncNamespaces`, `MaxStateSnapshotObjects`, payload, and client cache
limits. Snapshot omission does not imply deletion in v0.2; deletion/tombstone
semantics remain out of scope.

## 13. Error Model

Proposed v0.2 ERROR payload:

```json
{
  "code": "RATE_LIMITED",
  "message": "query rate exceeded",
  "retryable": true,
  "retry_after_ms": 1000
}
```

Correlation uses Envelope `reply_to`; the v0.1 payload-level `ref_id` is not
retained. `retry_after_ms` is optional and meaningful only when retryable.

| Code | Retryable | Default disposition |
|---|---:|---|
| `BAD_REQUEST` | no | reject frame; connection may remain open |
| `UNSUPPORTED_VERSION` | no | fail handshake and close |
| `UNSUPPORTED_FEATURE` | no | fail required negotiation and close |
| `INVALID_CAPABILITY` | no | reject malformed, duplicate, oversized, or non-canonical capability data; connection may remain open |
| `INVALID_STATE_VERSION` | no | reject a structurally invalid, stale, or same-version conflicting State replacement without changing cached State |
| `UNAUTHORIZED` | no | reject operation; during handshake normally close |
| `INVALID_SESSION` | no | abandon Session and close/resynchronize as caller policy |
| `NOT_FOUND` | no | reject referenced resource; connection remains open |
| `RATE_LIMITED` | yes | connection remains open; obey retry hint |
| `SYNC_REQUIRED` | no automatic Resume | stop Replay and request application full sync |
| `STATE_SYNC_REQUIRED` | no | reject a stale/conflicting Resume snapshot and close without advancing EVENT position |
| `STATE_UNAVAILABLE` | yes | close the current connection; retain Session position so the caller may retry Resume |
| `INTERNAL` | explicitly declared | implementation policy; never imply commit success |
| `PROTOCOL_VIOLATION` | no | close active protocol connection |

The minimal model deliberately does not add aliases for failures already
represented by the base protocol:

- `INVALID_FRAME` is `BAD_REQUEST`; the diagnostic message retains the precise
  structural reason;
- `STATE_NOT_FOUND` is represented by omission from an exact-query snapshot,
  or by generic `NOT_FOUND` when an operation itself references an absent
  protocol resource;
- `STATE_SYNC_FAILED` is represented by `STATE_UNAVAILABLE` during Resume.

`INVALID_STATE_VERSION` is a protocol-level rejection for a zero/exhausted
version and for a stale or same-version conflicting authoritative State
replacement. It is not an Application compare-and-set or business conflict
code.

An invalid ERROR never causes another ERROR. Implementations log and ignore it
or close according to local safety policy.

## 14. Limits

v0.2 defines configurable defaults and negotiated hard ceilings for at least:

- encoded frame bytes;
- decoded/decompressed frame bytes;
- protocol payload bytes;
- ID, Session ID, client name, EVENT type, namespace, object ID, and error
  message lengths;
- capability count, capability-name length, versions per capability, and
  capability-parameter bytes;
- pending reliable SEND count and bytes;
- Replay event count and bytes;
- EVENT identity-retention window;
- State Object data bytes and complete encoded State Object bytes;
- selectors, IDs per selector, and total IDs per query;
- snapshot object count and bytes;
- concurrent outstanding State queries;
- retained State identity/tombstone verification window.

Validation occurs before state mutation, callback, or large allocation. If
compression or encryption is later negotiated, limits apply to both wire form
and decoded plaintext so a small encoded frame cannot expand without bound.

Exceeding a request-scoped limit normally returns BAD_REQUEST or RATE_LIMITED.
Exceeding a safety bound during server-driven Replay or State publication fails
closed and requires resynchronization rather than partially advancing client
state.

These limits are safety bounds, not a pagination, backpressure, streaming, or
flow-control protocol.

## 15. Security Boundary

KMTProto provides validation and protocol admission; it does not implement
identity or authorization.

Protocol requirements:

- never silently downgrade the wire version or a required capability;
- reject use of unaccepted capability frames or parameters;
- cap capability lists and parameter sizes before negotiation work;
- bound encoded and decoded payloads before allocation;
- apply generation fencing to all async results and State frames;
- keep accepted capabilities and limits immutable for one generation;
- avoid logging opaque SEND/EVENT/State data by default;
- do not use timestamps as proof of freshness, authorization, or conflict
  resolution;
- do not activate compression/encryption before WELCOME and its capability
  specification's activation boundary;
- prevent ERROR loops and close on protocol violations that make state
  ambiguous.

Application responsibilities:

- authenticate the peer and map it to an authority domain;
- authorize namespace, selector, object visibility, mutation intent, and State
  update routing;
- avoid exposing object existence through inconsistent NOT_FOUND/UNAUTHORIZED
  behavior;
- enforce any tenant, user, or Session-specific representation rules;
- protect and persist business data.

Names such as `presence`, `typing`, or `message` are opaque strings. Negotiating
`state-sync` does not grant permission to read any namespace.

## 16. Open Questions

The following questions require design review before implementation. None
requires changing v0.1 because v0.2 is a separate wire generation.

1. **Frame ID scope:** require globally unique IDs, or uniqueness only within a
   Session and retention window? Global ULIDs are simpler operationally and are
   the current recommendation.
2. **Canonical State equality:** choose RFC 8785 JSON canonicalization, decoded
   semantic comparison, or another exact algorithm for equal-version duplicate
   validation.
3. **State tombstone retention:** define the minimum verification window and
   how it relates to Session resume and client cache lifetimes.
4. **Session-required capabilities:** persist only capabilities that change
   Session semantics, or require an identical capability set on every Resume?
5. **Namespace-wide synchronization:** remain deferred, or define a separate
   capability with snapshot barrier, cursor, and pagination?
6. **Live State routing:** is routing wholly application-owned, or should a
   future explicit subscription capability be standardized?
7. **Transform activation:** precise framing and activation rules for future
   compression and encryption capabilities.
8. **Authentication staging:** remain application admission outside KMTProto,
   or define a future authentication capability without embedding an auth
   system in the core?
9. **SUSPECT admission:** allow server-driven STATE_UPDATE while SUSPECT, or
   restrict all State traffic to READY as proposed?
10. **RESUME_OK:** retain the explicit frame for single-purpose semantics, or
    fold its payload into WELCOME at the cost of coupling handshake and Replay?
    The explicit frame is recommended.

## 17. Implementation Plan

Implementation is deliberately outside this task. If the proposal is approved,
the recommended order is:

1. resolve open questions, freeze normative schemas, and publish JSON test
   vectors;
2. define the v2 Envelope, universal IDs/`reply_to`, strict codec, validation,
   and negotiated limit model;
3. implement mandatory HELLO/WELCOME negotiation and deterministic connection
   admission;
4. port and re-verify v0.1 SEND/ACK, EVENT, Replay, generation fencing, and
   heartbeat invariants under the v2 Envelope;
5. add RESUME_OK and the revised Error model;
6. implement State Object validation, client version merge rules, bounded cache
   behavior, and protocol actions without storage I/O in locks;
7. add STATE_QUERY/SNAPSHOT/UPDATE plus abstract atomic State Store contracts;
8. add state-transition, failure-injection, property, race, and fuzz tests;
9. publish examples, wire fixtures, capability registry guidance, and an
   interoperability checklist.

Each stage should keep protocol transitions serial while producing Actions for
network, storage, and Application work after locks are released. All frames for
one connection must continue through one writer. Store contracts may require
atomic Claim, compare-and-set, high-water, or snapshot operations, but KMTProto
must not assume multi-process or distributed safety from in-memory helpers.

### 17.1 Concurrency contract

The implementation should make these contracts explicit in GoDoc:

- Client and Server protocol machines serialize state mutation internally or
  require one documented caller-owned actor;
- public methods advertised as concurrent-safe may be called concurrently;
- no protocol mutex spans network I/O, storage I/O, Application callbacks, or
  user callbacks;
- Memory stores and queues, if supplied, are process-local only;
- Replay snapshot and live EVENT publication share a Session serialization
  lane;
- accepted capabilities and negotiated limits are immutable after WELCOME;
- authoritative State Store compare-and-set is atomic for one object;
- cross-object, cross-service, and multi-node atomicity are caller concerns.

### 17.2 Required invariant tests

The future implementation should prove at least:

- required capabilities cannot be silently dropped;
- unaccepted capability frames cannot mutate state;
- all direct responses correlate to one request;
- ACK cannot precede Complete;
- unresolved PROCESSING cannot execute the Application twice;
- only EVENT changes `last_seq`;
- EVENT gaps never advance `last_seq` or leak partial Replay;
- Replay preserves identity and fixed boundaries;
- PING/PONG and State frames never change EVENT ordering;
- stale generations cannot change negotiated, EVENT, heartbeat, or State state;
- old State versions never replace new versions;
- equal version plus different State is rejected as `INVALID_STATE_VERSION`;
- snapshots validate completely before any object is applied;
- all negotiated and local limits fail closed without panic or unbounded
  allocation.

## 18. Breaking Changes Compared with v0.1

v0.2 is intentionally breaking and has no fallback mode:

- wire version changes from 1 to 2;
- every physical transport begins with HELLO, including Session Resume;
- HELLO/WELCOME schemas add capability and directional-limit negotiation;
- WELCOME has `NEW` and `RESUME_REQUIRED` admission meanings;
- every frame has an ID and direct responses use Envelope `reply_to`;
- ACK, PONG, and ERROR no longer use separate payload correlation IDs;
- Resume follows WELCOME on a replacement transport and succeeds via the new
  `RESUME_OK` frame;
- frame admission depends on the immutable accepted capability set;
- the optional `state-sync` capability introduces STATE_QUERY,
  STATE_SNAPSHOT, and server-only STATE_UPDATE;
- Error payload/disposition is regularized and adds `UNSUPPORTED_FEATURE`;
- validators reject mixed v1/v2 or capability-inconsistent traffic.

These changes are acceptable because there are no production consumers or
public clients. If that assumption changes before implementation, the version
and deployment strategy must be reviewed rather than quietly adding partial
compatibility.

## 19. Proposal Summary

KMTProto v0.2 should be a single Wire Version 2 protocol whose mandatory base
defines reliable SEND/ACK, ordered EVENT and fixed Replay, heartbeat, universal
correlation, and capability negotiation. Generic State Synchronization is a
negotiated extension with per-object monotonic versions, complete replacement
semantics, bounded snapshots, and server-authoritative live updates.

EVENT and STATE remain intentionally separate: EVENT is immutable Session
history; STATE is replaceable current value. Disconnect recovery completes
EVENT Replay first, then performs explicit, authorized State queries. This
keeps the protocol simple and extensible without making it a business engine,
storage system, distributed runtime, or full chat server.
