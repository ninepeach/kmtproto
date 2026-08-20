# KMTProto — Reliable Chat Protocol for Go

KMTProto is a lightweight, transport-independent protocol core for building reliable real-time chat and messaging systems in Go.

It provides resumable logical sessions, idempotent `SEND`/`ACK`, ordered server-to-client `EVENT` streams, gap detection, bounded replay, application-level heartbeat, capability negotiation, generic State synchronization, connection-generation fencing, and strict validation—without coupling the protocol to WebSocket, TCP, QUIC, storage, or business logic.

> Status: `v0.2` is the current freeze candidate and uses Wire Version 2 as its single baseline.

## What v0.2 guarantees

- Reliable connection-level `SEND` retry using the same message ID
- Atomic server-side duplicate claim and completed-ACK replay
- Application idempotency-key propagation across the crash-consistency boundary
- Per-session, monotonically increasing `EVENT` sequences
- Duplicate detection and `(session_id, seq) → event_id` conflict rejection
- Gap detection with delivery suspension until replay reaches its fixed boundary
- Explicit `WELCOME(RESUMED)` acknowledgement with `resume_from` and `replay_to`
- Stale connection-generation fencing
- Application-level `PING`/`PONG` liveness detection
- Strict JSON envelope and protocol-payload validation with bounded input sizes
- Single-writer outbound ordering and replay/live-event serialization
- Deterministic fake-clock and in-memory test components
- Immutable HELLO/WELCOME capability negotiation
- Business-blind State Objects with object-scoped monotonic versions
- Capability-gated `STATE_QUERY`, `STATE_SNAPSHOT`, and `STATE_UPDATE`
- Optional Resume State synchronization after fixed-boundary EVENT Replay

KMTProto does **not** claim global exactly-once delivery, process-crash-safe client outbox recovery, cross-service exactly-once side effects, permanent offline synchronization, business conflict resolution, or distributed storage safety. See the normative [Protocol v0.2](docs/protocol-v0.2.md) for the exact boundary. [Protocol v0.1](docs/protocol-v0.1.md) remains historical.

## Install

```bash
go get github.com/ninepeach/kmtproto
```

```go
import "github.com/ninepeach/kmtproto"
```

## Architecture

```text
Application
    ↓ content / business events
KMTProto
    ↓ outbound frames / delivery actions
Transport adapter
    ↓ WebSocket, TCP, QUIC, or an in-process link
```

The protocol core never opens a network connection. `Client` produces `Action` values, `Server` enqueues frames into an `OutboundQueue`, and `SingleWriter` is the only component that serializes bytes to a caller-provided `ByteSender`.

## Minimal client flow

```go
config := kmtproto.DefaultClientConfig()
client, err := kmtproto.NewClient(config)
if err != nil {
    panic(err)
}

generation := client.BeginConnect()
if err := client.TransportConnected(generation); err != nil {
    panic(err)
}

actions, err := client.StartSession(generation, "web-client")
// The transport adapter sends the frame inside SendFrameAction.
```

Retries come from the in-memory outbox and preserve the original message ID:

```go
actions, err := client.EnqueueSend("msg_01K...", json.RawMessage(`{"text":"hello"}`))
retryActions, err := client.RetryPending()
```

## Server boundary

`ApplicationHandler` receives the protocol message ID as its idempotency key:

```go
type ApplicationHandler interface {
    HandleSend(ctx context.Context, idempotencyKey string, payload []byte) error
}
```

SEND IDs must be globally unique (ULID is recommended). The protocol store uses `(session_id, msg_id)` for isolation and passes `msg_id` unchanged to the application. The protocol store and application idempotency must work together. If the application commits and the gateway crashes before `Complete`, a retry can cross that window; the application must suppress the repeated side effect using the same key.

## Example

The runnable example demonstrates a new session, heartbeat-capable state, reliable `SEND`/`ACK`, live `EVENT`, disconnect, and `RESUME` replay without any real network:

```bash
go run ./examples/basic
```

## Verification

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go test -fuzz=FuzzJSONCodec -fuzztime=10s .
```

GitHub Actions runs the same build, vet, test, race, and bounded fuzz checks.

## Package map

- `types.go`, `payload.go`: Wire Version 2 envelope, frame types, and typed payloads
- `capability.go`: capability validation, negotiation, and immutable Session state
- `state.go`: State Object validation and deterministic version merge
- `json_codec.go`, `validate.go`, `limits.go`: bounded strict codec and validation
- `client.go`: generation-fenced client state machine, outbox, heartbeat, and replay delivery
- `server.go`: handshake, idempotent SEND, replay boundary, and per-session serial lane
- `store.go`: required storage interfaces and deterministic in-memory implementations
- `outbound.go`: atomic frame batches and single writer
- `action.go`: transport- and application-facing effects
- `examples/basic`: end-to-end protocol demonstration

`MemoryDedupStore`, `MemoryReplayStore`, `MemorySessionRepository`,
`OutboundQueue`, `SingleWriter`, and `ServerConnection` are reference helpers
for tests, examples, and simple integrations. They do not make transport,
backpressure, persistence, or distributed-session policy part of the wire
protocol. See the [v0.2 final review](docs/review-v0.2-final.md) for the freeze record.

## License

MIT
