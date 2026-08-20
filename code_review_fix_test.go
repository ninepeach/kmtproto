package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestStateFramesRequireExactCapabilityVersion(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_000, 0))
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clock
	clientConfig.Capabilities = []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{2}, Required: true}}
	client, err := NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	generation := client.BeginConnect()
	if err := client.TransportConnected(generation); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartSession(generation, "version-client"); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{
		V:         WireVersionV2,
		Type:      FrameWelcome,
		SessionID: "s_v2_cap",
		Payload: mustPayload(WelcomePayload{
			Mode:                 WelcomeModeNew,
			AcceptedCapabilities: []NegotiatedCapability{{Name: CapabilityStateSync, Version: 2}},
		}),
	}
	if _, err := client.HandleIncoming(generation, welcome); err != nil {
		t.Fatal(err)
	}
	if version, ok := client.CapabilityVersion(CapabilityStateSync); !ok || version != 2 {
		t.Fatalf("negotiated version = %d, %v", version, ok)
	}
	if _, err := client.QueryState("q", "message", []string{"m"}); protocolErrorCode(err) != ErrorProtocolViolation {
		t.Fatalf("v1 STATE_QUERY under state-sync@2 returned %v", err)
	}

	registry, err := NewCapabilityRegistry([]CapabilitySpec{{Name: CapabilityStateSync, Versions: []uint16{2}}}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := DefaultServerConfig()
	serverConfig.Clock = clock
	serverConfig.Capabilities = registry
	serverConfig.StateStore = &testStateStore{objects: make(map[StateIdentity]StateObject)}
	serverConfig.NewSessionID = func() (string, error) { return "s_v2_cap", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServer(serverConfig, NewMemorySessionRepository(), NewMemoryDedupStore(clock, serverConfig.DedupTTL), replay, replay, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	helloOut := NewOutboundQueue()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{Capabilities: clientConfig.Capabilities})}
	if err := server.HandleIncoming(context.Background(), hello, helloOut); err != nil {
		t.Fatal(err)
	}
	_ = nextTestFrame(t, helloOut)
	queryOut := NewOutboundQueue()
	query := Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: "q", SessionID: "s_v2_cap", Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"m"}})}
	if err := server.HandleIncoming(context.Background(), query, queryOut); err != nil {
		t.Fatal(err)
	}
	assertErrorFrameCode(t, nextTestFrame(t, queryOut), ErrorProtocolViolation)
}

func TestInvalidSessionAlwaysAbandonsClientSession(t *testing.T) {
	client, _, generation := readyClient(t)
	if _, err := client.EnqueueSend("pending", json.RawMessage(`{"text":"pending"}`)); err != nil {
		t.Fatal(err)
	}
	frame := Envelope{
		V:         WireVersionV2,
		Type:      FrameError,
		SessionID: "s_1",
		Payload:   mustPayload(ErrorPayload{Code: ErrorInvalidSession, Retryable: false}),
	}
	actions, err := client.HandleIncoming(generation, frame)
	if err != nil || len(actions) != 1 {
		t.Fatalf("INVALID_SESSION actions=%#v err=%v", actions, err)
	}
	if client.State() != StateDisconnected || client.SessionID() != "" || client.LastSeq() != 0 {
		t.Fatalf("Session was not abandoned: state=%s session=%q seq=%d", client.State(), client.SessionID(), client.LastSeq())
	}
	if len(client.outbox) != 0 || len(client.eventIDs) != 0 || len(client.stateObjects) != 0 {
		t.Fatal("Session-scoped Client data survived INVALID_SESSION")
	}
}

func TestInvalidSessionAbandonsServerConnection(t *testing.T) {
	for _, frameType := range []FrameType{FramePing, FrameSend} {
		t.Run(string(frameType), func(t *testing.T) {
			server, clock, _ := newTestServer(t, &recordingApp{})
			connection := NewServerConnection()
			generation, outbound := connection.Replace()
			hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
			if err := connection.Handle(context.Background(), server, generation, hello); err != nil {
				t.Fatal(err)
			}
			_ = nextTestFrame(t, outbound)
			clock.Advance(server.config.SessionResumeTTL)
			frame := Envelope{V: WireVersionV2, Type: frameType, SessionID: "s_1"}
			if frameType == FramePing {
				frame.Payload = mustPayload(PingPayload{PingID: "p"})
			} else {
				frame.ID = "m"
				frame.Payload = mustPayload(SendPayload{Content: json.RawMessage(`{}`)})
			}
			if err := connection.Handle(context.Background(), server, generation, frame); err != nil {
				t.Fatal(err)
			}
			assertErrorFrameCode(t, nextTestFrame(t, outbound), ErrorInvalidSession)
			if connection.State() != ServerConnectionAwaitingHandshake || connection.SessionID() != "" {
				t.Fatalf("server connection retained invalid Session: state=%s session=%q", connection.State(), connection.SessionID())
			}
		})
	}
}

func TestWrongSessionErrorCannotMutateClient(t *testing.T) {
	client, _, generation := readyClient(t)
	if _, err := client.EnqueueSend("pending", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	wrong := Envelope{
		V:         WireVersionV2,
		Type:      FrameError,
		SessionID: "s_other",
		Payload:   mustPayload(ErrorPayload{Code: ErrorInvalidSession, RefID: "pending", Retryable: false}),
	}
	if _, err := client.HandleIncoming(generation, wrong); protocolErrorCode(err) != ErrorProtocolViolation {
		t.Fatalf("wrong-Session ERROR returned %v", err)
	}
	if client.State() != StateReady || client.SessionID() != "s_1" {
		t.Fatal("wrong-Session ERROR changed active Session")
	}
	if _, pending := client.outbox["pending"]; !pending {
		t.Fatal("wrong-Session ERROR removed pending SEND")
	}
}

func TestGapRecoveryIgnoresPreWelcomeInflightEvents(t *testing.T) {
	client, _, generation := readyClient(t)
	gap := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e2", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	actions, err := client.HandleIncoming(generation, gap)
	if err != nil || len(actions) != 1 || client.State() != StateResuming || client.LastSeq() != 0 {
		t.Fatalf("Gap transition actions=%#v state=%s seq=%d err=%v", actions, client.State(), client.LastSeq(), err)
	}
	inflight := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e3", SessionID: "s_1", Seq: 3, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	actions, err = client.HandleIncoming(generation, inflight)
	if err != nil || len(actions) != 0 || client.State() != StateResuming || client.LastSeq() != 0 {
		t.Fatalf("in-flight EVENT actions=%#v state=%s seq=%d err=%v", actions, client.State(), client.LastSeq(), err)
	}
}

type reentrantSendApp struct {
	mu       sync.Mutex
	server   *Server
	frame    Envelope
	outbound *OutboundQueue
	err      error
	calls    int
}

func (a *reentrantSendApp) HandleSend(_ context.Context, _ string, _ []byte) error {
	a.mu.Lock()
	a.calls++
	first := a.calls == 1
	a.mu.Unlock()
	if first {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := a.server.HandleIncoming(ctx, a.frame, a.outbound)
		a.mu.Lock()
		a.err = err
		a.mu.Unlock()
	}
	return nil
}

func TestSameSendApplicationReentryFailsFast(t *testing.T) {
	app := &reentrantSendApp{outbound: NewOutboundQueue()}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	app.server = server
	app.frame = Envelope{V: WireVersionV2, Type: FrameSend, ID: "reentrant", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	outerOut := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), app.frame, outerOut); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, outerOut); frame.Type != FrameAck {
		t.Fatalf("outer SEND did not complete: %#v", frame)
	}
	app.mu.Lock()
	nestedErr, calls := app.err, app.calls
	app.mu.Unlock()
	if nestedErr != nil || calls != 1 {
		t.Fatalf("nested error=%v application calls=%d", nestedErr, calls)
	}
	assertErrorFrameCode(t, nextTestFrame(t, app.outbound), ErrorInternal)
}

type reentrantClaimStore struct {
	delegate *MemoryDedupStore
	once     sync.Once
	server   *Server
	frame    Envelope
	outbound *OutboundQueue
	err      error
}

func (s *reentrantClaimStore) Claim(sessionID, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	s.once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s.err = s.server.HandleIncoming(ctx, s.frame, s.outbound)
	})
	return s.delegate.Claim(sessionID, msgID, fingerprint)
}

func (s *reentrantClaimStore) Complete(sessionID, msgID string, ack *Envelope) error {
	return s.delegate.Complete(sessionID, msgID, ack)
}

func (s *reentrantClaimStore) Abort(sessionID, msgID string) error {
	return s.delegate.Abort(sessionID, msgID)
}

func TestSameSendStoreReentryFailsFast(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_050, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	store := &reentrantClaimStore{delegate: NewMemoryDedupStore(clock, config.DedupTTL), outbound: NewOutboundQueue()}
	app := &recordingApp{}
	server, err := NewServer(config, NewMemorySessionRepository(), store, replay, replay, app)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	store.server = server
	store.frame = Envelope{V: WireVersionV2, Type: FrameSend, ID: "claim-reentrant", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	outerOut := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), store.frame, outerOut); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, outerOut); frame.Type != FrameAck {
		t.Fatalf("outer SEND did not complete: %#v", frame)
	}
	if store.err != nil || app.count() != 1 {
		t.Fatalf("nested error=%v application calls=%d", store.err, app.count())
	}
	assertErrorFrameCode(t, nextTestFrame(t, store.outbound), ErrorInternal)
}

func TestSendIDCannotBeReusedWithDifferentContent(t *testing.T) {
	app := &recordingApp{}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	first := Envelope{V: WireVersionV2, Type: FrameSend, ID: "same", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{"value":1}`)})}
	firstOut := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), first, firstOut); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, firstOut); frame.Type != FrameAck {
		t.Fatalf("first SEND response = %#v", frame)
	}
	conflict := copyEnvelope(first)
	conflict.Payload = mustPayload(SendPayload{Content: json.RawMessage(`{"value":2}`)})
	conflictOut := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), conflict, conflictOut); err != nil {
		t.Fatal(err)
	}
	assertErrorFrameCode(t, nextTestFrame(t, conflictOut), ErrorBadRequest)
	if app.count() != 1 {
		t.Fatalf("conflicting SEND executed Application %d times", app.count())
	}
}

func TestProcessingSendRejectsConflictingContent(t *testing.T) {
	app := &recordingApp{started: make(chan struct{}), release: make(chan struct{})}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	first := Envelope{V: WireVersionV2, Type: FrameSend, ID: "processing", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{"value":1}`)})}
	firstOut := NewOutboundQueue()
	firstDone := make(chan error, 1)
	go func() { firstDone <- server.HandleIncoming(context.Background(), first, firstOut) }()
	<-app.started
	conflict := copyEnvelope(first)
	conflict.Payload = mustPayload(SendPayload{Content: json.RawMessage(`{"value":2}`)})
	conflictOut := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), conflict, conflictOut); err != nil {
		t.Fatal(err)
	}
	assertErrorFrameCode(t, nextTestFrame(t, conflictOut), ErrorBadRequest)
	close(app.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, firstOut); frame.Type != FrameAck {
		t.Fatalf("original SEND response = %#v", frame)
	}
	if app.count() != 1 {
		t.Fatalf("conflicting PROCESSING SEND executed Application %d times", app.count())
	}
}

type failingSendApp struct {
	mu    sync.Mutex
	calls int
}

func (a *failingSendApp) HandleSend(context.Context, string, []byte) error {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return errors.New("commit outcome unknown")
}

func TestApplicationErrorLeavesSendIndeterminate(t *testing.T) {
	app := &failingSendApp{}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "indeterminate", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	for i := 0; i < 2; i++ {
		out := NewOutboundQueue()
		if err := server.HandleIncoming(context.Background(), send, out); err != nil {
			t.Fatal(err)
		}
		assertErrorFrameCode(t, nextTestFrame(t, out), ErrorInternal)
	}
	app.mu.Lock()
	calls := app.calls
	app.mu.Unlock()
	if calls != 1 {
		t.Fatalf("indeterminate Application executed %d times", calls)
	}
}

func TestRequiredWireFieldsAndDuplicateMembers(t *testing.T) {
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s", Payload: json.RawMessage(`{}`)}
	if err := ValidateFrame(&resume, DefaultLimits(), true); protocolErrorCode(err) != ErrorBadRequest {
		t.Fatalf("RESUME without last_seq returned %v", err)
	}
	errorFrame := Envelope{V: WireVersionV2, Type: FrameError, Payload: json.RawMessage(`{"code":"BAD_REQUEST"}`)}
	if err := ValidateFrame(&errorFrame, DefaultLimits(), true); protocolErrorCode(err) != ErrorBadRequest {
		t.Fatalf("ERROR without retryable returned %v", err)
	}
	codec := NewJSONCodec()
	for _, data := range [][]byte{
		[]byte(`{"v":2,"v":2,"type":"HELLO","payload":{}}`),
		[]byte(`{"v":2,"type":"RESUME","session_id":"s","payload":{"last_seq":0,"last_seq":1}}`),
	} {
		if _, err := codec.Decode(data); protocolErrorCode(err) != ErrorBadRequest {
			t.Fatalf("duplicate JSON member returned %v for %s", err, data)
		}
	}
}

type mismatchedEventAppender struct{}

func (mismatchedEventAppender) Append(_ string, eventID, eventType string, content []byte, timestamp int64) (Envelope, error) {
	return Envelope{
		V:         WireVersionV2,
		Type:      FrameEvent,
		ID:        eventID,
		SessionID: "s_other",
		Seq:       1,
		Timestamp: timestamp,
		Payload:   mustPayload(EventPayload{EventType: eventType, Content: append([]byte(nil), content...)}),
	}, nil
}

func TestEventAppenderResultMustMatchRequest(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_100, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, mismatchedEventAppender{}, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	if err := server.PublishEvent("s_1", "e", "message", json.RawMessage(`{}`), NewOutboundQueue()); err == nil {
		t.Fatal("mismatched EventAppender result was accepted")
	}
}

type mismatchedACKStore struct{}

func (mismatchedACKStore) Claim(_ string, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s_other", Payload: mustPayload(AckPayload{RefID: msgID})}
	return false, &DedupRecord{State: DedupCompleted, Fingerprint: fingerprint, Ack: &ack}, nil
}

func (mismatchedACKStore) Complete(string, string, *Envelope) error { return nil }
func (mismatchedACKStore) Abort(string, string) error               { return nil }

func TestDedupACKMustMatchSend(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_150, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), mismatchedACKStore{}, replay, replay, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.HandleIncoming(context.Background(), send, NewOutboundQueue()); err == nil {
		t.Fatal("dedup ACK for another Session was accepted")
	}
}

type blockingQueryStore struct {
	started chan struct{}
	release chan struct{}
	object  StateObject
}

func (s *blockingQueryStore) Get(ctx context.Context, _, _ string) (StateObject, bool, error) {
	close(s.started)
	select {
	case <-ctx.Done():
		return StateObject{}, false, ctx.Err()
	case <-s.release:
		return cloneStateObject(s.object), true, nil
	}
}

func (s *blockingQueryStore) Apply(_ context.Context, incoming StateObject) (StateObject, StateApplyResult, error) {
	return cloneStateObject(incoming), StateApplyApplied, nil
}

func TestStateQueryAndLiveUpdateUseOneStreamLane(t *testing.T) {
	object := StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)}
	store := &blockingQueryStore{started: make(chan struct{}), release: make(chan struct{}), object: object}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	queryOut := NewOutboundQueue()
	query := Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: "q", SessionID: "s_state", Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"m"}})}
	queryDone := make(chan error, 1)
	go func() { queryDone <- server.HandleIncoming(context.Background(), query, queryOut) }()
	<-store.started
	update := cloneStateObject(object)
	update.Version = 2
	if err := server.PublishStateUpdate("s_state", "u", update, NewOutboundQueue()); !errors.Is(err, ErrStreamCallbackActive) {
		t.Fatalf("concurrent live update returned %v", err)
	}
	close(store.release)
	if err := <-queryDone; err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, queryOut); frame.Type != FrameStateSnapshot {
		t.Fatalf("query response = %#v", frame)
	}
	server.laneMu.Lock()
	remainingLanes := len(server.lanes)
	server.laneMu.Unlock()
	if remainingLanes != 0 {
		t.Fatalf("idle stream lanes retained: %d", remainingLanes)
	}
}

func TestMemorySessionRepositoryRejectsCollision(t *testing.T) {
	repository := NewMemorySessionRepository()
	first := SessionState{SessionID: "s", ExpiresAt: time.Unix(3_000, 0)}
	if err := repository.Create(first); err != nil {
		t.Fatal(err)
	}
	second := SessionState{SessionID: "s", ExpiresAt: time.Unix(4_000, 0)}
	if err := repository.Create(second); !errors.Is(err, ErrSessionExists) {
		t.Fatalf("duplicate Create returned %v", err)
	}
	stored, exists, err := repository.Lookup("s", time.Unix(2_500, 0))
	if err != nil || !exists || !stored.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("collision replaced Session: %#v exists=%v err=%v", stored, exists, err)
	}
}

func TestWelcomeReplayBoundsAreOverflowSafe(t *testing.T) {
	frame := Envelope{
		V:         WireVersionV2,
		Type:      FrameWelcome,
		SessionID: "s",
		Payload: mustPayload(WelcomePayload{
			Mode:       WelcomeModeResumed,
			ResumeFrom: math.MaxUint64,
			ReplayTo:   math.MaxUint64,
		}),
	}
	if err := ValidateFrame(&frame, DefaultLimits(), true); err != nil {
		t.Fatalf("final replay boundary rejected: %v", err)
	}
}

func protocolErrorCode(err error) string {
	var protocolError *ProtocolError
	if errors.As(err, &protocolError) {
		return protocolError.Code
	}
	return ""
}

func assertErrorFrameCode(t *testing.T, frame Envelope, want string) {
	t.Helper()
	var payload ErrorPayload
	if frame.Type != FrameError {
		t.Fatalf("frame type=%s, want ERROR", frame.Type)
	}
	if err := decodePayload(frame.Payload, &payload, true); err != nil {
		t.Fatal(err)
	}
	if payload.Code != want {
		t.Fatalf("ERROR code=%s, want %s", payload.Code, want)
	}
}
