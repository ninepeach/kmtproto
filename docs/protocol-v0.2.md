# KMTProto Protocol v0.2

Status: **freeze candidate**

KMTProto v0.2 is a transport-independent chat synchronization protocol. It
defines reliable client submissions, an ordered server event stream, bounded
resume/replay, application-level heartbeat, capability negotiation, and
generic current-state synchronization. It does not define transports,
authentication policy, business workflows, databases, distributed ownership,
or conflict-resolution systems.

The terms MUST, MUST NOT, SHOULD, and MAY are normative.

## 1. Wire version and compatibility

`WireVersionV2` has the numeric value `2` and is the only accepted v0.2 wire
version. Every encoded Envelope, including ERROR responses to older peers,
uses `v: 2`. A peer receiving any other version returns
`UNSUPPORTED_VERSION` when possible and closes the connection.

v0.2 is a new wire baseline. It does not provide a v0.1 compatibility or
downgrade mode. The reliable SEND, EVENT, Resume, and heartbeat semantics are
carried forward, but a v0.1 Envelope is rejected because its wire version is
not 2.

## 2. Protocol boundary

KMTProto owns:

- Envelope, Frame, codec, validation, and protocol limits;
- `ClientProtocol` state transitions and reference `ServerAdmission` state;
- HELLO/WELCOME capability negotiation;
- SEND/ACK reliability and idempotency contracts;
- EVENT ordering, gap detection, Resume, and Replay;
- PING/PONG heartbeat semantics;
- generic State Objects and STATE synchronization Frames;
- deterministic protocol errors and Actions.

Callers own transport lifecycle, authentication and authorization decisions,
business meaning, persistence, distributed coordination, and all execution of
Actions. Memory stores, `OutboundQueue`, `SingleWriter`, and
`ServerAdmission` are process-local reference helpers, not a production
runtime requirement.

Normal server-side transport integration is:

```text
caller-owned transport -> ServerAdmission.Handle -> ServerProtocol -> OutboundQueue
```

`ServerAdmission` owns per-connection protocol admission: HELLO-first state,
allowed Frame types, Session correlation, and connection-generation fencing.
`ServerProtocol.ProcessFrame` is a low-level Frame processor and does not
perform those checks. A caller invoking `ProcessFrame` directly MUST provide
equivalent admission externally. Neither object owns or opens a network
connection, and transport lifecycle remains caller-owned.

## 3. Envelope

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

`seq=0` means unset. Only EVENT has a positive sequence. Timestamp is
diagnostic metadata and MUST NOT participate in ordering, deduplication,
Resume, State version comparison, causality, or timeout correctness.

Envelope IDs are operation or item identities. Their correlation meaning is
Frame-specific: SEND ID is acknowledged by `ACK.ref_id`; STATE_QUERY ID is
reused by its STATE_SNAPSHOT; EVENT and STATE_UPDATE IDs identify those wire
items. HELLO may carry an optional ID. WELCOME, PING, PONG, ACK, RESUME, and
ERROR do not carry Envelope IDs.

## 4. Frame model

| Frame | Direction | Required | Forbidden/fixed | Admission/result |
|---|---|---|---|---|
| HELLO | C -> S | payload object | empty session, `seq=0` | awaiting handshake; NEW WELCOME or ERROR |
| WELCOME | S -> C | session, mode | empty ID, `seq=0` | NEW in HANDSHAKING; RESUMED in RESUMING |
| PING | C -> S | session, `ping_id` | empty ID, `seq=0` | READY; matching PONG |
| PONG | S -> C | session, `ping_id` | empty ID, `seq=0` | READY/SUSPECT; current generation and pending ping only |
| SEND | C -> S | ID, session, content | `seq=0` | READY; ACK after reliable completion |
| ACK | S -> C | session, `ref_id` | empty ID, `seq=0` | READY/SUSPECT; clears matching outbox item |
| EVENT | S -> C | ID, session, `seq>=1`, content | none | READY or exact Replay position |
| RESUME | C -> S | session, explicit `last_seq` | empty ID, `seq=0` | awaiting handshake or same-session READY gap recovery |
| STATE_QUERY | C -> S | ID, session, namespace, object IDs | `seq=0` | READY plus `state-sync` |
| STATE_SNAPSHOT | S -> C | ID, session, explicit states | `seq=0` | pending query or Resume State phase |
| STATE_UPDATE | S -> C | ID, session, State Object | `seq=0` | READY plus `state-sync` |
| ERROR | either | known code, canonical retryable flag | empty ID, `seq=0` | code-specific disposition |

Unknown Frame Types and invalid shapes are `BAD_REQUEST`. An invalid ERROR is
logged/ignored or closes locally and MUST NOT trigger ERROR-about-ERROR.

## 5. Capability negotiation

HELLO advertises bounded capability offers:

```json
{"client_name":"web","capabilities":[{"name":"state-sync","versions":[1],"required":true}]}
```

For each offer, the server selects the highest version present in its registry
and in the offered version set. Results are sorted by capability name.
Unsupported optional capabilities are omitted. An unsupported required
capability returns `UNSUPPORTED_FEATURE` and no Session is created. Invalid or
duplicate declarations return `INVALID_CAPABILITY`.

NEW WELCOME contains the accepted capabilities. They become the immutable
capability set of the logical Session. RESUME reuses this set and MUST NOT
renegotiate it. All feature checks use Session capability state. The built-in
State Frames require capability `state-sync` version 1.

Capability names are lower-case ASCII, start with a letter, and may contain
letters, digits, and non-repeated internal `.` or `-` separators. Versions are
positive numeric fields and MUST NOT be encoded into names such as
`state-sync-v2`. The built-in State Frames implement exactly
`{name:"state-sync", version:1}`; `ClientProtocol` and `ServerProtocol`
configuration reject other
versions for that built-in capability. Configuration is performed through
`ClientProtocol` and `ServerProtocol` constructors.

## 6. Session and connection state

A Session is a logical resumable protocol context, not a user, conversation,
or transport connection. Client connection states are DISCONNECTED,
CONNECTING, CONNECTED, HANDSHAKING, RESUMING, READY, and SUSPECT.

Every replacement transport has a monotonically increasing local connection
generation. Input tagged with an older generation returns
`ErrStaleConnection` and MUST NOT mutate current Session, sequence, State, or
heartbeat data.

The reference `ServerAdmission` admits HELLO/RESUME while awaiting handshake;
PING, SEND, STATE_QUERY, and same-session RESUME while READY; and no normal
traffic while RESUMING. Successful RESUME returns it to READY. A RESUME without
explicit success leaves that connection closed or abandoned according to the
ERROR disposition.

`ServerProtocol` is constructed with `ServerDependencies`, which groups the
required Session repository, dedup store, Replay store, Event appender, and
Application handler. Missing dependencies are rejected deterministically at
construction. This grouping is an API boundary only and does not add storage
or runtime semantics.

## 7. Reliable SEND and ACK

`(session_id, msg_id)` is the SEND deduplication identity. A retry MUST reuse
the same ID and the exact same opaque content bytes. A dedup claim atomically
binds that identity to a content fingerprint; same-ID/different-content input
is `BAD_REQUEST`. The reliable completion order is:

```text
Claim -> Application(msg_id) -> Complete(stored ACK) -> emit ACK
```

ACK MUST NOT precede Complete. A COMPLETED duplicate returns the stored logical
ACK and never calls the Application again. A PROCESSING duplicate never calls
the Application again; it may wait or receive a retryable indeterminate result.
PROCESSING MUST NOT be reclaimed solely because TTL elapsed. Recovery requires
Complete, Abort, or an explicit store-specific crash-recovery procedure.
An Application error is an indeterminate commit unless the Application can
prove no side effect occurred; ordinary protocol handling therefore leaves the
claim PROCESSING rather than automatically aborting it.

ACK means the server crossed the reliable protocol completion boundary. It
does not mean recipient delivery/read, external side-effect completion, final
business success, or human response. KMTProto does not claim global exactly-once
side effects. Applications needing end-to-end deduplication MUST durably honor
`msg_id` as their idempotency key.

## 8. EVENT stream and Replay

Each Session owns one EVENT sequence beginning at 1. Only EVENT consumes
sequence; sequence is monotonic and its high-water mark never resets when
retained events are pruned. Within the supported identity retention window,
`(session_id, seq)` maps to exactly one event ID.

- next sequence: deliver and advance `last_seq`;
- earlier/equal sequence with the same ID: ignore as duplicate;
- earlier/equal sequence with another ID: `PROTOCOL_VIOLATION`;
- identity older than the verification window: fail conservatively;
- sequence greater than `last_seq+1`: enter RESUMING, keep `last_seq`, stop
  delivery, and send RESUME.

After a same-connection Gap and before RESUMED WELCOME fixes the Replay
boundary, additional already-in-flight EVENTs are discarded without delivery
or sequence advancement. A reconnect-initiated Resume does not permit EVENTs
before RESUMED WELCOME.

RESUME captures one fixed `replay_to = CurrentSeq(session)`. Replay returns
exactly `last_seq+1 ... replay_to`, with original ID, sequence, and payload.
Live EVENTs produced while Replay is captured are serialized after the Replay
batch. Partial Replay is buffered and never delivered to the Application.

`ClientProtocol` and `ServerProtocol` enforce independent Replay event and byte ceilings. A
Replay store receives `ReplayLimits` before materializing results. An
unavailable retention window or exceeded safety limit produces
`SYNC_REQUIRED`; `last_seq` does not advance.

## 9. State synchronization

State answers “what is current?” and is never encoded as EVENT. A complete
replacement is:

```json
{"namespace":"message","object_id":"msg001","version":5,"data":{"status":"read"}}
```

Identity is `(namespace, object_id)`. Version is positive, non-exhausted,
monotonic per identity, independent of EVENT `seq`, and may jump. Higher
versions replace. Lower versions are stale. Equal version plus semantically
equal JSON is a duplicate; equal version plus different JSON is a conflict.
Stale/conflicting data MUST NOT mutate retained State.

STATE_QUERY requests a bounded, duplicate-free set of IDs in one namespace.
Its STATE_SNAPSHOT response reuses the query ID; missing objects are omitted.
Point-query results do not promise a cross-object database transaction.
STATE_UPDATE carries one already-committed complete replacement and does not
write protocol storage.

Point snapshots and live State output for one Session use the same stream lane.
If a caller attempts a same-Session stream operation synchronously while an
injected State callback is active, it receives `ErrStreamCallbackActive` and
may retry after the callback returns.

Snapshot object count and encoded payload bytes are bounded. Query responses
accumulate only until the bound and then return deterministic `BAD_REQUEST`.
Resume snapshot providers receive `StateSnapshotLimits` and MUST enforce them
while materializing the result; provider failure becomes `STATE_UNAVAILABLE`.

## 10. Resume with optional State

RESUME may include canonical, unique requested State namespaces when the
Session negotiated `state-sync`. Server output order is:

1. RESUMED WELCOME with fixed `resume_from` and `replay_to`;
2. the complete contiguous EVENT Replay;
3. one STATE_SNAPSHOT for the requested namespaces.

EVENT and State remain separate domains. State Frames use `seq=0`, do not enter
Replay, and do not affect `last_seq`. `ClientProtocol` stages both phases and emits no
Application delivery until they are complete. It then emits replayed EVENT
actions, State-change actions, and finally READY. Any interruption discards
staged Replay and retries from the last confirmed `last_seq`.

## 11. Heartbeat

PING/PONG are application-level protocol Frames. They are not replayed and do
not affect EVENT sequence or State versions. A PONG changes health only when
its `ping_id` and connection generation match the outstanding ping, and only
in READY or SUSPECT. Timeouts use local monotonic durations; wire timestamps
are not timeout evidence.

## 12. Errors

Every ERROR contains `code`, optional `message`, optional `ref_id`, and the
code's canonical `retryable` value. Standard codes are `BAD_REQUEST`,
`UNSUPPORTED_VERSION`, `UNSUPPORTED_FEATURE`, `INVALID_CAPABILITY`,
`INVALID_STATE_VERSION`, `UNAUTHORIZED`, `INVALID_SESSION`, `NOT_FOUND`,
`RATE_LIMITED`, `SYNC_REQUIRED`, `STATE_SYNC_REQUIRED`, `STATE_UNAVAILABLE`,
`INTERNAL`, and `PROTOCOL_VIOLATION`.

Unsupported version/feature, unauthorized input, State-sync indeterminacy, and
protocol violation close. INVALID_SESSION abandons the logical Session.
SYNC_REQUIRED stops automatic Resume and requires caller-directed full sync.
STATE_UNAVAILABLE is retryable and closes the current recovery attempt.
INTERNAL disposition is selected at the failure site.

## 13. Limits and validation

Implementations MUST bound complete Frames, payloads, IDs, capability lists,
State namespace/object/data sizes, query and snapshot counts, State snapshot
bytes, Replay events/bytes, Client identity retention, and Client State cache.
Validation occurs before protocol mutation or avoidable large materialization.
Zero configuration values select defaults; negative values are invalid.

Strict mode rejects unknown Envelope and typed-payload fields. SEND/EVENT
content and State data remain opaque valid JSON. Strict mode also rejects
duplicate member names in the Envelope and typed payload objects; duplicate
members inside opaque content/data remain an Application concern. Codec and
validation MUST never panic on malformed input.

## 14. Concurrency and storage contracts

`ClientProtocol`, `ServerAdmission`, `MemoryDedupStore`, `MemoryReplayStore`,
`MemorySessionRepository`, and `OutboundQueue` are safe for concurrent use in
one process. `ServerProtocol` is safe when injected dependencies satisfy their
stated contracts. `ClientProtocol` and `ServerProtocol` are protocol state
machines/processors only: neither owns network connections, opens
WebSocket/TCP/QUIC connections, or manages transport lifecycle. These
guarantees do not imply multi-process or distributed safety.

One Session stream lane serializes Replay, live EVENT, and live STATE output.
Injected callbacks execute without protocol mutexes held. While an injected
callback is active, any new same-Session lane operation returns
`ErrStreamCallbackActive` immediately and may be retried after the callback;
this includes synchronous callback re-entry. No lock is held across transport
I/O, Application callbacks, or user callbacks.

Replay stores and snapshot providers MUST enforce supplied materialization
limits. Dedup stores MUST atomically Claim identities. State stores MUST
atomically enforce monotonic replacement per State identity. Persistence and
distributed consistency remain implementation concerns.

Session repositories MUST atomically reject duplicate Session IDs. Dedup
stores MUST retain completed fingerprints and ACKs for at least the configured
Client retry and Session Resume windows. Clock implementations are
concurrency-safe, prompt value providers. Protocol objects sample Clock values
without holding their state/store mutexes; a synchronous Clock callback cannot
deadlock by re-entering the invoking object.

## 15. Extension rules

New optional behavior requires a negotiated, namespaced capability. A
capability does not permit changing existing Frame semantics. New Frame Types
or incompatible field semantics require a future wire version. Optional
payload additions require explicit negotiation and deterministic behavior when
absent. Business concepts such as read receipts, typing, presence, or workflow
states remain opaque namespaces/data and do not become protocol semantics.

## 16. Frozen invariants

1. Wire v2 is the only accepted v0.2 version.
2. Negotiated capabilities are immutable for a Session.
3. ACK follows reliable Complete and duplicate SEND never repeats Application execution.
4. Only EVENT consumes Session-scoped sequence.
5. Gap detection never advances `last_seq`.
6. Replay uses a fixed boundary and preserves EVENT identity.
7. Partial Replay is never delivered.
8. State versions are object-scoped and independent from EVENT sequence.
9. State Frames never affect EVENT ordering or Replay.
10. Resume with State reaches READY only after both phases complete.
11. PING/PONG never affect EVENT or State ordering.
12. Stale generations never mutate active protocol state.
