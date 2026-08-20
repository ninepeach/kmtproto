package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestStreamLaneRecoversFromPanic(t *testing.T) {
	server := &ServerProtocol{lanes: make(map[string]*streamLane)}
	if err := server.runStream("s", func(*streamLane) error { panic("boom") }); err == nil {
		t.Fatal("expected recovered panic error")
	}
	if err := server.runStream("s", func(*streamLane) error { return nil }); err != nil {
		t.Fatalf("lane remained unusable after panic: %v", err)
	}
}

func TestEventAppenderCallbackReentryFailsWithoutDeadlock(t *testing.T) {
	clock := NewFakeClock(time.Unix(6, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	nestedOutbound := NewOutboundQueue()
	appender := &reentrantEventAppender{delegate: replay, outbound: nestedOutbound}
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    appender,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	appender.server = server
	createTestSession(t, server)

	outbound := NewOutboundQueue()
	if err := server.PublishEvent("s_1", "event_1", "test", json.RawMessage(`{}`), outbound); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(appender.result, ErrStreamCallbackActive) {
		t.Fatalf("nested Event publication returned %v, want ErrStreamCallbackActive", appender.result)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameEvent || frame.ID != "event_1" || frame.Seq != 1 {
		t.Fatalf("outer Event publication failed after rejected reentry: %#v", frame)
	}
	if queuedFrameCount(nestedOutbound) != 0 {
		t.Fatal("nested Event publication unexpectedly reached outbound")
	}
}

func TestApplicationPanicReleasesSendFlight(t *testing.T) {
	app := &panicOnceApplication{}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	firstOut := NewOutboundQueue()
	first := Envelope{V: WireVersionV2, Type: FrameSend, ID: "panic_send", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.ProcessFrame(context.Background(), first, firstOut); err != nil {
		t.Fatal(err)
	}
	assertErrorFrameCode(t, nextTestFrame(t, firstOut), ErrorInternal)
	secondOut := NewOutboundQueue()
	second := Envelope{V: WireVersionV2, Type: FrameSend, ID: "next_send", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.ProcessFrame(context.Background(), second, secondOut); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, secondOut); frame.Type != FrameAck {
		t.Fatalf("future SEND remained stranded after panic: %#v", frame)
	}
	server.flightMu.Lock()
	remaining := len(server.flights)
	server.flightMu.Unlock()
	if remaining != 0 || app.calls != 2 {
		t.Fatalf("SEND flight leaked after panic: flights=%d calls=%d", remaining, app.calls)
	}
}
