package kmtproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

func TestInvalidClientStateTransitionsAndOldWelcome(t *testing.T) {
	client, _, oldGen := readyClient(t)
	newGen := client.BeginConnect()
	oldWelcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: 0})}
	if _, err := client.HandleIncoming(oldGen, oldWelcome); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("old WELCOME: %v", err)
	}
	if err := client.TransportConnected(newGen); err != nil {
		t.Fatal(err)
	}
	pong := Envelope{V: WireVersionV2, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{PingID: "late"})}
	if _, err := client.HandleIncoming(newGen, pong); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("PONG in CONNECTED: %v", err)
	}
	if client.State() != StateConnected || client.LastSeq() != 0 {
		t.Fatal("invalid transition mutated state")
	}
	if _, err := client.Resume(newGen); err != nil {
		t.Fatal(err)
	}
	unexpectedNew := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})}
	actions, err := client.HandleIncoming(newGen, unexpectedNew)
	if err != nil || client.State() != StateDisconnected || !hasFullSyncAction(actions, "s_1") {
		t.Fatalf("invalid resume acknowledgement was not terminal: actions=%#v state=%s err=%v", actions, client.State(), err)
	}
}

func TestOutboundValidationPrecedesClientMutation(t *testing.T) {
	client, _, _ := configuredReadyClient(t, func(config *ClientConfig) {
		config.Limits.MaxPayloadSize = 32
	})
	if _, err := client.EnqueueSend("m", json.RawMessage(`"1234567890123456789012345678"`)); err == nil {
		t.Fatal("expected encoded SEND payload limit error")
	}
	if actions, err := client.RetryPending(); err != nil || len(actions) != 0 {
		t.Fatalf("invalid SEND entered outbox: actions=%d err=%v", len(actions), err)
	}
	pingClient, _, pingGen := configuredReadyClient(t, func(*ClientConfig) {})
	if _, err := pingClient.SendPing(pingGen, string(bytes.Repeat([]byte{'p'}, DefaultLimits().MaxIDLength+1))); err == nil {
		t.Fatal("expected oversized ping ID error")
	}
	if _, err := pingClient.SendPing(pingGen, "valid"); err != nil {
		t.Fatalf("invalid PING mutated heartbeat state: %v", err)
	}
}
