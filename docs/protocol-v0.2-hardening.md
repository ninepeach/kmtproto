# KMTProto v0.2 Protocol Hardening

This record defines the safety behavior implemented after capability
negotiation, generic State synchronization, and Resume/State integration. It
does not add a transport, persistence, authorization, business workflow, or a
new synchronization model. SEND/ACK, EVENT ordering, EVENT Replay, and
heartbeat semantics are unchanged.

The normative wire contract is [Protocol v0.2](protocol-v0.2.md). This
hardening record explains implementation safety decisions and does not define
an alternative compatibility mode.

## Validation boundary

An incoming frame is admitted in this order:

1. the JSON codec rejects empty, malformed, trailing, or oversized encoded
   input;
2. Envelope validation checks wire version, bounded identifiers, payload size,
   and complete encoded frame size;
3. typed payload validation checks required, forbidden, and bounded fields;
4. the protocol state machine checks connection state, generation, Session,
   and negotiated capability admission;
5. only then may a store, Application handler, or protocol mutation run.

Strict JSON mode rejects unknown Envelope and protocol-payload fields. Opaque
SEND/EVENT/State `data` values remain application-defined JSON.

Unknown frame types and malformed frame shapes use `BAD_REQUEST`. This is the
canonical `INVALID_FRAME` behavior; a second alias is intentionally not added.
An invalid incoming ERROR is never answered with another ERROR.

## Error dispositions

Every ERROR contains `code`, optional `message`, optional `ref_id`, and the
code's canonical `retryable` value.

| Code | Retryable | Connection behavior |
|---|---:|---|
| `BAD_REQUEST` | false | operation rejected; may remain open |
| `UNSUPPORTED_VERSION` | false | close |
| `UNSUPPORTED_FEATURE` | false | required negotiation failed; close |
| `INVALID_CAPABILITY` | false | malformed negotiation data rejected; may remain open |
| `INVALID_STATE_VERSION` | false | State rejected without replacement; may remain open |
| `STATE_SYNC_REQUIRED` | false | Resume State result is indeterminate; close |
| `STATE_UNAVAILABLE` | true | Resume may be retried without advancing EVENT position; close |
| `PROTOCOL_VIOLATION` | false | close |

`STATE_NOT_FOUND`, `STATE_SYNC_FAILED`, and `INVALID_FRAME` are not added:
exact State query misses are omitted from the snapshot,
`STATE_UNAVAILABLE` represents State synchronization failure, and
`BAD_REQUEST` represents an invalid frame.

## Configurable protocol limits

`Limits` uses positive local ceilings. Zero fields inherit their individual
default, so a caller may override one field without resetting or disabling the
others. Constructors reject negative or otherwise non-positive effective
limits.

| Limit | Default |
|---|---:|
| complete encoded frame | 1 MiB |
| protocol payload | 768 KiB |
| frame/ref ID | 128 bytes |
| Session ID | 128 bytes |
| error message | 1 KiB |
| capabilities | 32 |
| capability name | 64 bytes |
| versions per capability | 16 |
| client name | 128 bytes |
| EVENT type | 128 bytes |
| State namespace | 64 bytes |
| State object ID | 256 bytes |
| State data | 512 KiB |
| complete encoded State Object | 640 KiB |
| object IDs per State query | 128 |
| objects per State snapshot | 128 |
| encoded State snapshot payload | 768 KiB |
| namespaces per Resume State sync | 32 |

Client Replay event/byte bounds, EVENT identity retention, client State cache
object/byte bounds, and server Replay event/byte bounds remain independently
configurable. Replay and namespace-snapshot providers receive these limits
before materializing a result. These are safety ceilings, not persistence
quotas, pagination, backpressure, or a flow-control protocol.

## State safety

- State identity is `(namespace, object_id)` and is independent from EVENT
  sequence.
- versions are positive, non-exhausted, and monotonic per State identity;
- a newer version replaces the retained object;
- an equivalent object at the same version is an idempotent duplicate;
- an older version or differing data at the same version returns
  `INVALID_STATE_VERSION` and does not mutate retained State;
- State frames require the immutable negotiated `state-sync` capability;
- State frames always use `seq = 0`, never advance `last_seq`, and never enter
  EVENT Replay.

## Capability safety

Capability names and version lists are bounded and validated. Duplicate names
or versions are rejected as `INVALID_CAPABILITY`. Negotiation chooses the
highest common version. An unknown optional capability is deterministically
omitted; an unsupported required capability returns `UNSUPPORTED_FEATURE` and
closes. The accepted capability snapshot is immutable for the logical Session
after WELCOME and is reused, not renegotiated, during Resume.

## Compatibility

v0.2 uses Wire Version 2 exclusively and intentionally has no v0.1 wire
downgrade mode. The reliable SEND, EVENT sequence/Replay, and PING/PONG
semantics are preserved, but v0.1 Envelopes are rejected with
`UNSUPPORTED_VERSION`. Inputs that are malformed, oversized, or ambiguous are
rejected before protocol mutation.
