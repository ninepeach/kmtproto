package kmtproto

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestEventIdentityWindowIsBoundedAndConservative(t *testing.T) {
	client, _, gen := configuredReadyClient(t, func(config *ClientConfig) { config.EventIdentityWindow = 2 })
	for seq := uint64(1); seq <= 3; seq++ {
		event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: string(rune('a' + seq)), SessionID: "s_1", Seq: seq, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
		if _, err := client.HandleIncoming(gen, event); err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	retained := len(client.eventIDs)
	client.mu.Unlock()
	if retained != 2 {
		t.Fatalf("retained %d identities, want 2", retained)
	}
	tooOld := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "b", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, tooOld); !errors.Is(err, ErrIdentityExpired) {
		t.Fatalf("old unverifiable duplicate: %v", err)
	}
	safe := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "c", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if actions, err := client.HandleIncoming(gen, safe); err != nil || len(actions) != 0 {
		t.Fatalf("recent safe duplicate: actions=%d err=%v", len(actions), err)
	}
	conflict := copyEnvelope(safe)
	conflict.ID = "different"
	if _, err := client.HandleIncoming(gen, conflict); !errors.Is(err, ErrProtocolConflict) {
		t.Fatalf("recent identity conflict: %v", err)
	}
}

func TestOnlyEventChangesLastSequence(t *testing.T) {
	client, _, gen := readyClient(t)
	if _, err := client.SendPing(gen, "p"); err != nil {
		t.Fatal(err)
	}
	pong := Envelope{V: WireVersionV2, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{PingID: "p"})}
	if _, err := client.HandleIncoming(gen, pong); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnqueueSend("m", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s_1", Payload: mustPayload(AckPayload{RefID: "m"})}
	if _, err := client.HandleIncoming(gen, ack); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("non-EVENT changed last_seq to %d", client.LastSeq())
	}
	event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, event); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 1 {
		t.Fatalf("EVENT did not advance last_seq: %d", client.LastSeq())
	}
}
