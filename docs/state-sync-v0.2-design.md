# KMTProto v0.2 State Synchronization Extension — Design Proposal

Status: **HISTORICAL DESIGN / IMPLEMENTED WITH RECORDED DEVIATIONS**
Target: KMTProto v0.2 design review  
Compatibility baseline: KMTProto v0.1

The normative implemented State contract is
[Protocol v0.2](protocol-v0.2.md). This document is retained as design
rationale and is not a second wire specification.

## 1. Motivation

KMTProto v0.1 answers two questions:

- Did a reliable client submission cross the server commit boundary?
- Which ordered server events did this Session miss?

Those questions are handled by `SEND`/`ACK` and the replayable `EVENT` stream.
They do not answer a separate question: **what is the current value of an
application-owned object?**

The distinction is normative:

| Model | Meaning | Identity/order | Recovery |
|---|---|---|---|
| EVENT | Something happened | Session-scoped `seq` and immutable event ID | Ordered replay |
| STATE | This is the current value | Object-scoped monotonic `version` | Authoritative snapshot |

For example, `message.read` may be useful historical evidence, while
`message/msg001.status = read` is the current replacement value. A client that
only needs the current value should not have to replay every historical change.

STATE MUST NOT be encoded as an `EVENT` whose `event_type` is `state.update`.
Doing so would mix replacement semantics with history semantics, consume EVENT
sequence numbers for snapshots, and incorrectly make current-state recovery
depend on EVENT retention.

## 2. Scope

The proposed extension defines:

- a business-blind `StateObject` representation;
- object-scoped version and replacement semantics;
- one-shot state queries and authoritative snapshots;
- live authoritative state-update delivery;
- client merge rules for duplicates, stale versions, conflicts, and deletes;
- coordination rules between v0.1 recovery and state synchronization;
- abstract storage, authorization, and publication contracts;
- deterministic validation, concurrency, failure, property, and fuzz tests.

Examples of possible application namespaces include `message`, `delivery`,
`presence`, `task`, and `workflow`. These names have no built-in meaning to
KMTProto.

## 3. Non-goals

The extension does not define or implement:

- message read, delivery, typing, presence, task, or workflow business logic;
- a database, Redis adapter, cache, retention service, or replication system;
- WebSocket, TCP, QUIC, HTTP, connection registry, or Gateway lifecycle;
- authentication, namespace authorization policy, or object routing;
- CRDTs, field-level merges, last-write-wins-by-time, vector clocks, consensus,
  distributed transactions, or a conflict-resolution engine;
- durable client caches, offline mutation queues, subscription registries,
  pagination, flow control, or permanent offline synchronization;
- atomic consistency between arbitrary external business stores and KMTProto;
- any change to the nine v0.1 Frame Types or their payloads and semantics.

The existing in-memory stores and runtime helpers remain reference components.
They are not promoted into a production state service by this proposal.

## 4. Concepts

### 4.1 State identity

A state object is identified by:

```text
(namespace, object_id)
```

`namespace` partitions application-defined object kinds. `object_id` identifies
one object within that namespace. Neither field is interpreted by KMTProto.

The Session carried by the Envelope is the authorization and delivery context;
it is not automatically part of State identity. If an application exposes a
different representation per tenant, user, or Session, it MUST use an identity
or namespace that makes those representations distinct. Within one authority
domain, the same `(namespace, object_id, version)` MUST never represent
different data for different clients.

### 4.2 Replacement, not patch

Every State Object contains a complete replacement value. v0.2 does not define
JSON Patch, partial-field merge, or application-specific mutation operators.
Replacement semantics make out-of-order delivery deterministic: a newer
complete value can safely supersede an older one.

### 4.3 Authoritative server

The proposed v0.2 layer is server-authoritative:

- a client queries current state;
- the server returns committed state;
- the server publishes committed replacements;
- only the authoritative side assigns versions.

A client request to change business state continues to use v0.1 `SEND`. The
Application receives the SEND message ID as its idempotency key, commits the
business/state change, and may then publish an authoritative `STATE_UPDATE`.
This preserves the existing reliable-write boundary instead of inventing a
second write/ACK/idempotency protocol.

### 4.4 Independent client synchronization phase

State synchronization does not add values to the v0.1 `ConnectionState` enum.
An implementation may maintain an independent state-cache phase:

```text
UNKNOWN -> SYNCING -> CURRENT
              |
              +----> STALE
```

This phase never changes connection generation, heartbeat state, EVENT
`last_seq`, or v0.1 READY/RESUMING behavior.

## 5. Frame proposal

### 5.1 Compatibility and wire version

The recommended extension is negotiated by a new wire version, conceptually
`WireVersionV2 = 2`. It does not reinterpret version 1:

- a v0.1 Session continues to accept exactly the nine v0.1 frames;
- a server MUST NOT send State frames on a v0.1 Session;
- a v0.2 Session uses one connection-wide wire version and MUST NOT mix v1 and
  v2 envelopes;
- existing frame schemas and semantics are unchanged under v2;
- a v0.1 server may reject a v2 HELLO with the existing
  `UNSUPPORTED_VERSION` behavior;
- reconnecting and explicitly falling back to v1 is caller policy, not an
  automatic state-machine transition.

No capability fields are added to v0.1 `HELLO` or `WELCOME` by this proposal.

### 5.2 Recommended minimal frame set

The recommended v0.2 design adds three new frame names. These are proposals,
not current constants:

| Frame | Direction | Purpose | Replayable | Consumes EVENT seq |
|---|---|---|---:|---:|
| `STATE_QUERY` | Client -> Server | Request one authoritative snapshot | no | no |
| `STATE_SNAPSHOT` | Server -> Client | Return the complete result of one query | no | no |
| `STATE_UPDATE` | Server -> Client | Publish one committed replacement | no | no |

There is intentionally no client-to-server `STATE_UPDATE`. Client mutation
intent uses reliable `SEND`; `STATE_UPDATE` means the replacement is already
authoritative. There is also no `STATE_ACK`: a missed live update is repaired by
a later snapshot, not replayed as history.

All three State frames have `Envelope.Seq == 0`, never enter Replay Store, never
change EVENT `last_seq`, and never affect PING/PONG.

### 5.3 `STATE_QUERY`

Direction: Client -> Server.

Proposed envelope rules:

- `v == 2`;
- `type == STATE_QUERY`;
- `id` required and unique for query correlation;
- `session_id` required;
- `seq == 0`;
- timestamp optional and diagnostic only;
- payload required.

Proposed payload:

```json
{
  "selectors": [
    {"namespace": "message", "ids": ["msg001", "msg002"]},
    {"namespace": "task", "ids": ["pickup001"]}
  ]
}
```

`ids` identifies exact objects. An omitted or empty `ids` list is a proposed
namespace-wide selector and is subject to strict authorization and object/byte
limits. Whether namespace-wide selection is included in the first v0.2 wire
release remains an open question; exact-ID selection is the safe minimum.

Validation:

- at least one selector;
- no duplicate selector or duplicate ID within a selector;
- namespace and ID are non-empty and within configured limits;
- selector count, total ID count, encoded payload, and resulting snapshot are
  bounded;
- unknown protocol fields follow the configured strict codec policy;
- application-owned names remain semantically opaque.

Allowed protocol state:

- the current generation is active;
- the v0.1 connection state is READY;
- no EVENT replay is in progress.

Expected response:

- exactly one correlated `STATE_SNAPSHOT`; or
- one existing `ERROR` correlated through `ref_id`.

The query is one-shot. It does not create a protocol-owned durable
subscription. Choosing which future committed updates to route to a Session is
an Application/Gateway responsibility.

A query has no commit side effect and is not placed in the SEND outbox. If a
response times out, the client issues a new query ID and supersedes the old
attempt. A late response for a superseded ID is ignored. Reusing one query ID
for a different selector set is a protocol violation.

### 5.4 `STATE_SNAPSHOT`

Direction: Server -> Client.

Proposed envelope rules:

- `v == 2`;
- `type == STATE_SNAPSHOT`;
- unique response `id` required;
- `session_id` required and equal to the active Session;
- `seq == 0`;
- payload required.

Proposed payload:

```json
{
  "ref_id": "query_01K...",
  "objects": [
    {
      "namespace": "message",
      "id": "msg001",
      "version": 5,
      "data": {"status": "read"}
    }
  ],
  "missing": [
    {"namespace": "message", "id": "msg002"}
  ]
}
```

`ref_id` MUST match one outstanding query from the same Session and generation.
`missing` is a normal result for an authorized exact-ID query and is not an
ERROR. An implementation MAY deliberately map unauthorized objects to missing
to avoid an authorization oracle.

For a key previously cached by the client, `missing` removes the visible cached
value but MUST NOT erase the client's remembered version floor. A later object
must have a version greater than that floor. The authoritative store likewise
must preserve the object's version high-water if a tombstone is eventually
pruned; recreating the key never restarts at version 1. A store that cannot
preserve that invariant cannot safely advertise recoverable State identity.

The snapshot is one bounded frame in the minimal proposal. The client validates
the entire frame, all objects, total object count, and total bytes before
changing its cache or emitting application actions. Partial snapshot delivery
is forbidden.

For exact-ID selectors, applying objects and missing results atomically is
sufficient. A namespace-wide snapshot may replace the selected local namespace
only if the server/store contract provides a coherent membership snapshot and
serializes later `STATE_UPDATE` publication after the snapshot boundary.
Otherwise namespace-wide replacement MUST be rejected rather than presented as
complete.

Client transition:

```text
STATE SYNCING + valid correlated snapshot -> apply atomically -> STATE CURRENT
```

An invalid, oversized, uncorrelated, wrong-Session, or stale-generation snapshot
does not partially mutate the cache.

### 5.5 `STATE_UPDATE`

Direction: Server -> Client only.

Proposed envelope rules:

- `v == 2`;
- `type == STATE_UPDATE`;
- unique update `id` required for diagnostics and duplicate observation;
- `session_id` required and equal to the active Session;
- `seq == 0`;
- payload contains exactly one complete `StateObject`.

Example:

```json
{
  "v": 2,
  "type": "STATE_UPDATE",
  "id": "state_01K...",
  "session_id": "s_123",
  "payload": {
    "object": {
      "namespace": "task",
      "id": "pickup001",
      "version": 10,
      "data": {"status": "completed"}
    }
  }
}
```

The update MUST be emitted only after the authoritative replacement is
committed. It is not an instruction to execute business logic and does not
receive ACK. Duplicate and reordered updates are resolved solely by the
StateObject rules below.

Allowed protocol state:

- active Session and current connection generation;
- base connection READY; an implementation may also accept it during SUSPECT,
  but doing so MUST NOT restore heartbeat health;
- never during HANDSHAKING or v0.1 EVENT RESUMING.

Effect: apply, ignore, or reject according to version comparison. It never
changes connection state, heartbeat, EVENT sequence, or Resume position.

### 5.6 Existing ERROR behavior

The proposal does not add or reinterpret v0.1 error codes. Existing codes are
used with a State frame's query/update ID in `ErrorPayload.RefID`:

- `BAD_REQUEST`: malformed selector, State Object, or unsupported query shape;
- `UNAUTHORIZED`: caller policy rejects access;
- `NOT_FOUND`: caller chooses explicit non-disclosure-safe missing behavior;
- `RATE_LIMITED`: query rate or result policy exceeded;
- `INTERNAL`: state reader/publisher failure.

`SYNC_REQUIRED` remains the v0.1 EVENT replay disposition and is not reused for
ordinary State Query failure. Invalid incoming ERROR still cannot cause an
ERROR-about-ERROR loop.

## 6. StateObject model

Proposed conceptual wire model:

```go
type StateObject struct {
    Namespace string          `json:"namespace"`
    ID        string          `json:"id"`
    Version   uint64          `json:"version"`
    Deleted   bool            `json:"deleted,omitempty"`
    Data      json.RawMessage `json:"data,omitempty"`
}
```

Normative constraints:

- `namespace` and `id` are required, bounded strings;
- `version >= 1` on every authoritative wire object;
- `deleted == false` requires non-empty valid JSON `data`;
- `deleted == true` forbids `data`;
- `data` is application-owned and semantically opaque;
- protocol payload decoding may be strict while `data` remains open to
  application-defined fields;
- the full encoded object is bounded by `MaxStateObjectSize`;
- a snapshot is additionally bounded by object count and total bytes.

A deletion is a versioned tombstone rather than absence. Without a tombstone,
a client that missed deletion could retain an old value indefinitely, and a
late old update could resurrect it. Tombstone retention and eventual cache
eviction are storage/application policies, but retention MUST cover every
period in which a stale client may legally attempt state recovery.

## 7. Version semantics

State version is an unsigned, object-scoped logical counter:

```text
(namespace, object_id) -> 1, 2, 3, ...
```

It is not a timestamp, EVENT sequence, Session generation, database row version
shared across objects, or causality proof between namespaces.

Required invariants:

1. The authoritative store assigns the version after a successful commit.
2. The first materialized value or tombstone has version 1.
3. Each committed replacement increases the previous version.
4. A version never decreases or wraps to zero.
5. The same `(namespace, id, version)` always maps to the same `deleted` flag
   and exact logical data.
6. Timestamp and arrival time never choose the winner.
7. State version never consumes or compares with EVENT `seq`.

Client merge rule for cached version `C` and incoming version `I`:

| Condition | Behavior |
|---|---|
| no cached object | accept valid `I >= 1` |
| `I > C` | replace cached object atomically |
| `I == C`, same tombstone/data | safe duplicate; ignore |
| `I == C`, different tombstone/data | `PROTOCOL_VIOLATION` |
| `I < C` | stale replacement; ignore |

Unlike EVENT, a version jump is not a gap. A client may safely replace version 5
with version 9 because State Objects are complete current values. Versions 6-8
need not be replayed.

`uint64` exhaustion MUST fail closed; the store may not wrap or reuse versions.

For the JSON codec, “same data” means the same deterministic canonical JSON
value, not merely two byte strings that happen to retain different whitespace
or object-key order. The authoritative publisher MUST use a stable canonical
encoding for a committed version. A client may retain a digest of that
canonical value to detect same-version conflicts without retaining duplicate
payloads. The exact canonicalization algorithm must be fixed before the v0.2
wire specification is finalized.

## 8. Relationship with EVENT

EVENT and STATE have separate correctness domains:

| Question | EVENT | STATE |
|---|---|---|
| What does it represent? | Immutable occurrence | Current replacement value |
| Scope | One Session EVENT stream | One `(namespace, id)` object |
| Counter | Contiguous `seq` | Monotonic `version`; jumps allowed |
| Duplicate rule | Same seq must have same event ID | Same version must have same value |
| Missed data | Gap + ordered replay | Query current snapshot |
| Retention | Replay window | Current value plus deletion policy |

If one business transaction produces both an EVENT and a State replacement:

- they do not share a version;
- no numeric comparison between EVENT `seq` and State `version` is valid;
- KMTProto does not require a cross-store distributed transaction;
- KMTProto does not guarantee live delivery order between EVENT and
  `STATE_UPDATE`;
- the Application may commit them in one application/database transaction when
  its business invariant requires that stronger guarantee;
- the server publishes only records that their owning stores report committed.

The State Object is authoritative for the current value. EVENT remains
authoritative for ordered history. A client MUST NOT use timestamp or whichever
frame arrived last to override the State version rule.

During recovery, the recommended order is deliberate:

1. complete the existing v0.1 RESUME and EVENT replay;
2. reach v0.1 READY;
3. issue State Query selectors;
4. atomically apply the resulting snapshot;
5. merge later live State Updates by object version.

This allows historical application actions to run before an authoritative
current snapshot replaces the cache. It does not create cross-stream atomicity.

## 9. Resume integration

Three strategies were considered:

| Option | Behavior | Advantages | Problems |
|---|---|---|---|
| A. EVENT only | v0.1 RESUME is unchanged; caller separately manages state | Zero protocol coupling | No standard state recovery; stale cache is easy to overlook |
| B. EVENT plus automatic State Snapshot | every RESUME implicitly returns all state | Simple caller experience | Unbounded/ambiguous scope; changes RESUME/WELCOME semantics; couples two recovery models |
| C. EVENT Resume, then explicit State Query selectors | finish v0.1 recovery, then query required objects/namespaces | Preserves v0.1; bounded, authorized, opt-in, deterministic | Extra round trip; caller must choose selectors |

**Recommendation: Option C.**

The v0.1 `RESUME` payload and `WELCOME(RESUMED)` response remain byte-for-byte
unchanged. State positions are never embedded in `last_seq`, `resume_from`, or
`replay_to`. A reconnect therefore repairs two independent dimensions:

```text
EVENT continuity: RESUME(last_seq) -> fixed EVENT replay -> READY
STATE currency:   STATE_QUERY(selectors) -> STATE_SNAPSHOT -> CURRENT
```

Live State Updates are not replayed. If a disconnect occurs while a State Query
is outstanding, its response belongs to the old generation and is ignored. The
new connection completes EVENT Resume first and issues a new query. A client
does not carry an outstanding query ID across generations.

## 10. Storage contract

KMTProto defines interfaces and invariants, not a storage implementation. A
future implementation needs three conceptual capabilities.

### 10.1 Authoritative read

```text
Get(namespace, id, subject) -> StateObject | missing | error
Snapshot(selectors, subject, limits) -> objects + missing | error
```

`subject` represents caller-provided authorization/visibility context, not a
wire data model prescribed by KMTProto.

Required contract:

- each returned object is one committed replacement;
- results obey visibility and configured bounds;
- an exact-ID snapshot gives one result or missing for each requested key;
- a namespace-wide replacement snapshot is advertised as complete only if
  membership is observed coherently;
- object version high-water survives value/tombstone pruning and recreation;
- storage calls occur without a protocol-state mutex held.

### 10.2 Authoritative commit

If the library exposes a reference commit boundary, it should use optimistic
compare-and-set semantics rather than accepting arbitrary client versions:

```text
CommitReplacement(key, expected_version, data_or_tombstone)
    -> committed StateObject | version conflict | error
```

The store atomically compares the current version, commits the replacement, and
assigns the next version. The Application decides whether to retry, transform,
or reject a version conflict. KMTProto does not merge values.

A blind `Set(version, data)` interface is not recommended because it permits
version reuse, rollback, and two different values at one version.

### 10.3 Publication boundary

`STATE_UPDATE` may be enqueued only for a committed State Object. If commit
succeeds but outbound delivery fails, the state is still committed; the client
recovers it through a later snapshot. No EVENT replay entry or ACK is created.

Implementations that publish both EVENT and STATE from separate durable stores
must document their own transactional/outbox guarantee. KMTProto does not claim
cross-store exactly-once publication.

## 11. Concurrency rules

### 11.1 Concurrent writers

For two writers that both read version 5:

```text
writer A: expected=5, replacement=read
writer B: expected=5, replacement=archived
```

Only one compare-and-set may commit version 6. The other receives a version
conflict. The Application chooses whether to reread and retry. Arrival time,
wire timestamp, goroutine scheduling, and client clock never resolve the
conflict.

This is **version rejection**, not last-write-wins and not a conflict-resolution
engine.

### 11.2 Out-of-order delivery

If version 6 arrives before version 5, the client keeps version 6 and ignores
version 5. If version 9 follows version 6, it replaces version 6 without
requesting versions 7 and 8.

### 11.3 Snapshot versus live update

All outbound frames still pass through the existing single serialization point.
For an exact-ID query, per-object version comparison makes either delivery order
safe:

- snapshot v5 then update v6 -> apply v5, then v6;
- update v6 then snapshot v5 -> keep v6, ignore v5.

If the result is `missing`, the client retains its prior version floor and the
server integration must ensure an update committed before the query boundary is
not enqueued after that missing result. A concurrent creation committed after
the boundary receives the next version and may safely follow the snapshot.

A namespace-wide replacement snapshot additionally requires a storage/publication
barrier so membership and post-snapshot updates cannot be lost. The server must
enqueue the complete snapshot before updates committed after that snapshot
boundary. If the integration cannot provide this contract, it must allow only
exact-ID snapshots.

### 11.4 Lock and callback rules

The v0.1 rules remain:

- serialize protocol state mutation under one owner or mutex;
- compute actions, then release the lock;
- never hold a protocol lock across State Store I/O, authorization, application
  callbacks, outbound byte I/O, or user callbacks;
- generation checks happen before cache or query-state mutation;
- no old-generation snapshot or update may mutate the active cache.

## 12. Security notes

The protocol validates structure and limits; the caller owns identity and
authorization.

Required integration considerations:

- authorize every namespace, object, query, and outgoing update for the active
  subject and Session;
- never treat possession of an object ID as authorization;
- avoid leaking object existence through different unauthorized/missing
  behavior unless the application intentionally permits it;
- limit namespace length, object-ID length, selector count, requested IDs,
  State Object size, snapshot object count, snapshot bytes, and query rate;
- validate the complete snapshot before allocation-heavy application decoding
  and before any cache mutation;
- keep protocol payloads strict while treating State `data` as opaque JSON;
- reject version 0, overflow, contradictory tombstone/data, duplicate keys, and
  same-key/same-version/different-data conflicts;
- fence old connection generations before authorization side effects or state
  delivery;
- do not use timestamps for freshness, authorization, or conflict resolution;
- redact State data from logs according to caller policy.

Typing and presence expiry are application semantics. A timestamp or TTL inside
opaque `data` does not become a protocol clock and KMTProto does not delete the
object automatically.

## 13. Testing plan

No tests are implemented in this design-only change. A future implementation
should add the following deterministic suite without real network or sleeps.

### 13.1 Wire and validation

- valid and invalid `STATE_QUERY`;
- missing/duplicate/oversized selectors and IDs;
- State frames require v2, Session ID, frame ID, `seq == 0`, and valid payload;
- malformed JSON, unknown fields under strict mode, oversized State data, and
  oversized snapshot;
- version 0, tombstone with data, live object without data, duplicate keys;
- State frames on a v1 Session are rejected and never emitted;
- codec/validator fuzzing never panics, deadlocks, or allocates without bounds.

### 13.2 Version invariants

- v5 then v6 replaces;
- v6 then v5 keeps v6;
- v5 then identical v5 is a safe duplicate;
- v5 then different v5 is `PROTOCOL_VIOLATION`;
- v5 then v9 is valid and does not trigger EVENT Gap Detection;
- tombstone prevents resurrection by an older live value;
- pruning a tombstone does not reset authoritative or cached version high-water;
- version never decreases or wraps;
- only State frames alter State cache versions;
- State frames never alter EVENT `last_seq`.

### 13.3 Query and snapshot consistency

- query/response correlation by query ID, Session, and generation;
- missing exact keys are applied atomically with returned objects;
- invalid object N in a snapshot leaves the entire cache unchanged;
- object/byte bounds fail before partial delivery;
- snapshot v5 racing live update v6 ends at v6 in either arrival order;
- namespace-wide snapshot is accepted only with a complete boundary contract;
- disconnect during an outstanding query discards the old response;
- repeated queries converge to the same authoritative versions.

### 13.4 Resume and isolation

- EVENT Resume completes before State Query begins;
- missed live State Update is recovered by a later snapshot;
- State recovery does not modify `resume_from`, `replay_to`, or EVENT replay;
- State Query failure does not silently advance EVENT position;
- PING/PONG never changes State versions;
- State traffic during SUSPECT cannot revive heartbeat;
- old-generation Snapshot/Update cannot mutate current state;
- invalid State ERROR does not create an ERROR loop.

### 13.5 Concurrency and failure injection

- concurrent compare-and-set writers produce one committed next version;
- concurrent publication for one object never maps one version to two values;
- commit failure emits no authoritative State Update;
- commit success plus outbound loss is recovered by snapshot;
- snapshot read failure produces no partial snapshot;
- authorization callback or store panic cannot strand protocol serialization;
- no lock spans store, authorization, application, sender, or user callbacks;
- race detector passes under concurrent query, update, heartbeat, and reconnect.

### 13.6 Property tests

For arbitrary valid operation sequences:

- cached object versions never decrease;
- same key/version never maps to different values;
- stale updates never overwrite a newer cache value;
- a complete valid snapshot plus all later updates converges to authoritative
  state;
- EVENT `last_seq` is independent of every State operation;
- old generations never mutate current query/cache state.

## 14. Proposed hard invariants

1. STATE and EVENT use independent identity and ordering domains.
2. State frames always have `seq == 0` and never enter EVENT Replay.
3. One `(namespace, id, version)` maps to exactly one replacement or tombstone.
4. State versions are server-assigned, object-scoped, monotonic, and never
   timestamp-derived.
5. A stale State version never overwrites a newer cached version.
6. Pruning a value or tombstone never resets its version high-water.
7. A version jump is valid and does not imply a replay gap.
8. A State Snapshot is validated and applied atomically; partial snapshots are
   never exposed to the Application.
9. A State Update is published only after its authoritative commit boundary.
10. Client mutation intent uses reliable SEND; State Update is authoritative
   notification, not a command.
11. EVENT Resume completes before post-reconnect State synchronization.
12. Missed State Updates are repaired by query/snapshot, never EVENT Replay.
13. State synchronization never changes EVENT `last_seq`, Resume boundaries,
    heartbeat health, or connection generation.
14. Old-generation State frames never mutate the active cache or query state.
15. Same business action does not imply shared EVENT seq/State version or
    cross-stream delivery order.
16. No protocol lock spans storage, authorization, application, transport, or
    user callbacks.

## 15. Open questions

The following decisions should remain explicit review items before any code is
written:

1. **Selector scope:** ship exact-ID queries only, or also require
   namespace-wide enumeration in v0.2?
2. **Namespace snapshot barrier:** if namespace-wide queries are included, what
   minimum Store/Publisher contract proves coherent membership and ordering?
3. **Tombstone retention:** what caller-declared recovery window must deletion
   tombstones cover, and when may a client evict them?
4. **State routing:** is one-shot query plus caller-directed State Update enough,
   or does a future version need explicit subscribe/unsubscribe semantics?
5. **Snapshot size:** is one bounded atomic frame sufficient for v0.2, or must
   pagination/chunking be designed before namespace-wide queries are allowed?
6. **Fallback policy:** should an official client helper expose explicit v2 ->
   v1 reconnect fallback, or leave all downgrade decisions to the caller?
7. **SUSPECT behavior:** accept current-generation State Updates while SUSPECT
   without changing heartbeat, or reject all State traffic until READY?
8. **Tombstone data model:** is the `deleted` flag sufficient, or is an optional
   application-owned deletion reason required inside a separate opaque field?
9. **Canonical equality:** which canonical JSON algorithm defines
   same-version data identity for the v0.2 JSON codec?

## 16. Recommended review outcome

The minimal coherent v0.2 direction is:

- preserve v0.1 unchanged and place State frames behind wire version 2;
- use `STATE_QUERY`, `STATE_SNAPSHOT`, and server-only `STATE_UPDATE`;
- keep client write intent on reliable `SEND`;
- model complete, versioned replacement objects with tombstones;
- assign versions per `(namespace, id)` at the authoritative commit boundary;
- recover EVENT history first and query selected State afterward;
- merge snapshots and live updates only by object version;
- require exact-ID snapshots initially unless namespace-wide consistency and
  bounds are proven by the storage/publication contract;
- leave business interpretation, authorization, routing, storage, retention,
  and conflict decisions outside KMTProto.

This introduces general state synchronization without changing any v0.1 frame,
without consuming EVENT sequence numbers, and without making State part of
EVENT replay.
