package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingApp struct {
	mu      sync.Mutex
	calls   []string
	started chan struct{}
	release chan struct{}
}

type failAt string

func (f failAt) Fail(point string) error {
	if point == string(f) {
		return errors.New("injected failure")
	}
	return nil
}

func (a *recordingApp) HandleSend(ctx context.Context, key string, _ []byte) error {
	a.mu.Lock()
	a.calls = append(a.calls, key)
	started := a.started
	release := a.release
	a.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
	}
	return nil
}

func (a *recordingApp) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

func newTestServer(t *testing.T, app ApplicationHandler) (*Server, *FakeClock, *MemoryReplayStore) {
	t.Helper()
	clock := NewFakeClock(time.Unix(200, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, app)
	if err != nil {
		t.Fatal(err)
	}
	return server, clock, replay
}

func createTestSession(t *testing.T, server *Server) {
	t.Helper()
	out := NewOutboundQueue()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := server.HandleIncoming(context.Background(), hello, out); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameWelcome || frame.SessionID != "s_1" {
		t.Fatalf("unexpected WELCOME: %#v", frame)
	}
}

func nextTestFrame(t *testing.T, out *OutboundQueue) Envelope {
	t.Helper()
	frame, err := out.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestHelloPingAndUnsupportedVersion(t *testing.T) {
	server, clock, _ := newTestServer(t, &recordingApp{})
	createTestSession(t, server)
	out := NewOutboundQueue()
	ping := Envelope{V: WireVersionV2, Type: FramePing, SessionID: "s_1", Payload: mustPayload(PingPayload{PingID: "p_1", ClientTime: clock.Now().UnixMilli()})}
	if err := server.HandleIncoming(context.Background(), ping, out); err != nil {
		t.Fatal(err)
	}
	if pong := nextTestFrame(t, out); pong.Type != FramePong || pong.Seq != 0 {
		t.Fatalf("unexpected PONG: %#v", pong)
	}
	bad := Envelope{V: 99, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := server.HandleIncoming(context.Background(), bad, out); err != nil {
		t.Fatal(err)
	}
	if protocolErr := nextTestFrame(t, out); protocolErr.Type != FrameError {
		t.Fatalf("expected ERROR, got %#v", protocolErr)
	}
}

func TestWireVersionV2IsTheOnlyAcceptedBaseline(t *testing.T) {
	v2 := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := ValidateFrame(&v2, DefaultLimits(), true); err != nil {
		t.Fatalf("wire v2 HELLO rejected: %v", err)
	}
	v1 := copyEnvelope(v2)
	v1.V = 1
	err := ValidateFrame(&v1, DefaultLimits(), true)
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != ErrorUnsupportedVersion || !protocolErr.Close {
		t.Fatalf("wire v1 rejection = %#v, want closing UNSUPPORTED_VERSION", err)
	}

	server, _, _ := newTestServer(t, &recordingApp{})
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), v1, outbound); err != nil {
		t.Fatal(err)
	}
	response := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if response.V != WireVersionV2 || response.Type != FrameError || decodePayload(response.Payload, &payload, true) != nil || payload.Code != ErrorUnsupportedVersion {
		t.Fatalf("wire v1 server response = %#v payload=%#v", response, payload)
	}
}

func TestConcurrentDuplicateSendCommitsOnceAndReplaysAck(t *testing.T) {
	app := &recordingApp{started: make(chan struct{}), release: make(chan struct{})}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_1", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{"text":"hi"}`)})}
	out1 := NewOutboundQueue()
	out2 := NewOutboundQueue()
	errCh := make(chan error, 2)
	go func() { errCh <- server.HandleIncoming(context.Background(), send, out1) }()
	<-app.started
	go func() { errCh <- server.HandleIncoming(context.Background(), send, out2) }()
	close(app.release)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	ack1 := nextTestFrame(t, out1)
	ack2 := nextTestFrame(t, out2)
	if ack1.Type != FrameAck || ack2.Type != FrameAck || string(ack1.Payload) != string(ack2.Payload) {
		t.Fatalf("ACK replay mismatch: %#v %#v", ack1, ack2)
	}
	if app.count() != 1 {
		t.Fatalf("application called %d times", app.count())
	}

	out3 := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), send, out3); err != nil {
		t.Fatal(err)
	}
	if ack3 := nextTestFrame(t, out3); ack3.Timestamp != ack1.Timestamp || string(ack3.Payload) != string(ack1.Payload) {
		t.Fatal("completed duplicate did not return stored logical ACK")
	}
}

func TestResumeBoundaryAndReplayIdentity(t *testing.T) {
	server, _, _ := newTestServer(t, &recordingApp{})
	createTestSession(t, server)
	live := NewOutboundQueue()
	if err := server.PublishEvent("s_1", "evt_1", "message.new", json.RawMessage(`{"n":1}`), live); err != nil {
		t.Fatal(err)
	}
	original := nextTestFrame(t, live)
	resumeOut := NewOutboundQueue()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.HandleIncoming(context.Background(), resume, resumeOut); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, resumeOut)
	replayed := nextTestFrame(t, resumeOut)
	var bounds WelcomePayload
	if err := decodePayload(welcome.Payload, &bounds, true); err != nil {
		t.Fatal(err)
	}
	if bounds.ReplayTo != 1 || bounds.ResumeFrom != 1 || replayed.ID != original.ID || replayed.Seq != original.Seq || string(replayed.Payload) != string(original.Payload) {
		t.Fatalf("replay identity/boundary changed: bounds=%#v original=%#v replayed=%#v", bounds, original, replayed)
	}
	if err := server.PublishEvent("s_1", "evt_2", "message.new", json.RawMessage(`{"n":2}`), resumeOut); err != nil {
		t.Fatal(err)
	}
	if live2 := nextTestFrame(t, resumeOut); live2.Seq != 2 {
		t.Fatalf("live event interleaved incorrectly: %#v", live2)
	}
}

func TestSyncRequiredOutsideReplayWindow(t *testing.T) {
	server, _, replay := newTestServer(t, &recordingApp{})
	createTestSession(t, server)
	out := NewOutboundQueue()
	for i := 1; i <= 3; i++ {
		id := "evt_" + string(rune('0'+i))
		if err := server.PublishEvent("s_1", id, "test", json.RawMessage(`{}`), out); err != nil {
			t.Fatal(err)
		}
		_ = nextTestFrame(t, out)
	}
	replay.PruneBefore("s_1", 3)
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.HandleIncoming(context.Background(), resume, out); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, out)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorSyncRequired {
		t.Fatalf("expected SYNC_REQUIRED, got %#v %#v", frame, payload)
	}
}

func TestServerConnectionRejectsOldGeneration(t *testing.T) {
	server, _, _ := newTestServer(t, &recordingApp{})
	connection := NewServerConnection()
	oldGeneration, _ := connection.Replace()
	newGeneration, _ := connection.Replace()
	if newGeneration <= oldGeneration {
		t.Fatal("generation did not advance")
	}
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := connection.Handle(context.Background(), server, oldGeneration, hello); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("got %v, want stale generation", err)
	}
}

func TestResumeInvalidSession(t *testing.T) {
	server, _, _ := newTestServer(t, &recordingApp{})
	out := NewOutboundQueue()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "missing", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.HandleIncoming(context.Background(), resume, out); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, out)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorInvalidSession {
		t.Fatalf("expected INVALID_SESSION: %#v %#v", frame, payload)
	}
}

func TestFailureAfterCompleteRecoversByAckReplay(t *testing.T) {
	app := &recordingApp{}
	clock := NewFakeClock(time.Unix(300, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	config.FailureInjector = failAt(FailAfterComplete)
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, app)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_crash", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.HandleIncoming(context.Background(), send, NewOutboundQueue()); err == nil {
		t.Fatal("expected injected post-complete failure")
	}
	server.config.FailureInjector = nil
	out := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), send, out); err != nil {
		t.Fatal(err)
	}
	if ack := nextTestFrame(t, out); ack.Type != FrameAck {
		t.Fatalf("expected stored ACK, got %#v", ack)
	}
	if app.count() != 1 {
		t.Fatalf("application repeated after completed crash window: %d", app.count())
	}
}

func TestTTLConfigurationInvariant(t *testing.T) {
	config := DefaultServerConfig()
	config.DedupTTL = time.Minute
	config.SessionResumeTTL = time.Hour
	clock := NewFakeClock(time.Unix(0, 0))
	replay := NewMemoryReplayStore()
	_, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, time.Minute), replay, replay, &recordingApp{})
	if err == nil {
		t.Fatal("expected incompatible TTL rejection")
	}
}
