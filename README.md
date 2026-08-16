# KMTProto — Reliable Chat Protocol for Go

KMTProto is a lightweight, transport-independent protocol core for building reliable real-time chat and messaging systems in Go.

It provides resumable logical sessions, idempotent `SEND`/`ACK`, ordered server-to-client `EVENT` streams, gap detection, bounded replay, application-level heartbeat, connection-generation fencing, strict validation, and a single outbound serialization point—without coupling the protocol to WebSocket, TCP, QUIC, storage, or business logic.

> Status: protocol and implementation version `v0.1` are under active development. The wire format is not yet declared stable for production interoperability.

## What v0.1 guarantees

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

KMTProto does **not** claim global exactly-once delivery, process-crash-safe client outbox recovery, cross-service exactly-once side effects, or permanent offline synchronization. See [Protocol v0.1](docs/protocol-v0.1.md) for the exact reliability boundary.

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

- `types.go`, `payload.go`: wire envelope, frame types, and typed payloads
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
protocol. See the [v0.1 review](docs/review-v0.1.md) for the hardening record.

## License

MIT
