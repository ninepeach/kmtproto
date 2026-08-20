// Package kmtproto implements a transport-independent, resumable chat protocol.
//
// The package owns wire frames, validation, client and server state transitions,
// SEND/ACK idempotency, EVENT sequencing, replay, heartbeat, capability
// negotiation, generic State synchronization, and protocol actions. It
// deliberately does not own sockets, accounts, conversations, authorization
// policy, persistence technology, distributed ownership, or application
// semantics. Wire Version 2 is the package's only v0.2 wire baseline.
package kmtproto
