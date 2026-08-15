// Package kmtproto implements a transport-independent, resumable chat protocol.
//
// The package owns wire frames, validation, client and server state transitions,
// SEND/ACK idempotency, EVENT sequencing, replay, heartbeat, and outbound
// serialization. It deliberately does not own sockets, accounts, conversations,
// authorization policy, persistence technology, or application semantics.
package kmtproto
