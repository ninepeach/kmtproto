package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestServerReplayEventLimitReturnsSyncRequired(t *testing.T) {
	clock := NewFakeClock(time.Unix(5, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	config.MaxReplayEvents = 2
	replay := NewMemoryReplayStore()
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	out := NewOutboundQueue()
	for i := 1; i <= 3; i++ {
		if err := server.PublishEvent("s_1", string(rune('a'+i)), "test", json.RawMessage(`{}`), out); err != nil {
			t.Fatal(err)
		}
		_ = nextTestFrame(t, out)
	}
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.ProcessFrame(context.Background(), resume, out); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, out)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorSyncRequired {
		t.Fatalf("expected SYNC_REQUIRED, got %#v %#v", frame, payload)
	}
}

func TestServerReplayByteLimitReturnsSyncRequired(t *testing.T) {
	server, _, _ := newTestServer(t, &recordingApp{})
	createTestSession(t, server)
	live := NewOutboundQueue()
	if err := server.PublishEvent("s_1", "large_event", "test", json.RawMessage(`{"value":"012345678901234567890123456789"}`), live); err != nil {
		t.Fatal(err)
	}
	event := nextTestFrame(t, live)
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	server.config.MaxReplayBytes = len(encoded) - 1

	outbound := NewOutboundQueue()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.ProcessFrame(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorSyncRequired {
		t.Fatalf("expected byte-bounded SYNC_REQUIRED, got %#v %#v", frame, payload)
	}
}

func TestReplayStoreReentryFailsWithoutDeadlock(t *testing.T) {
	clock := NewFakeClock(time.Unix(5_500, 0))
	baseReplay := NewMemoryReplayStore()
	if _, err := baseReplay.Append("s_1", "event_1", "test", []byte(`{}`), clock.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	sessions := NewMemorySessionRepository()
	if err := sessions.Create(SessionState{SessionID: "s_1", ExpiresAt: clock.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	replay := &reentrantReplayStore{delegate: baseReplay, outbound: NewOutboundQueue()}
	config := DefaultServerConfig()
	config.Clock = clock
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    sessions,
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    baseReplay,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	replay.server = server
	outbound := NewOutboundQueue()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.ProcessFrame(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(replay.result, ErrStreamCallbackActive) {
		t.Fatalf("same-Session ReplayStore reentry returned %v", replay.result)
	}
	if welcome := nextTestFrame(t, outbound); welcome.Type != FrameWelcome {
		t.Fatalf("outer replay failed after rejected reentry: %#v", welcome)
	}
	if event := nextTestFrame(t, outbound); event.Type != FrameEvent || event.Seq != 1 {
		t.Fatalf("outer replay EVENT missing: %#v", event)
	}
}
