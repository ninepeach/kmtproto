package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type reentrantSendApp struct {
	mu       sync.Mutex
	server   *ServerProtocol
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
		err := a.server.ProcessFrame(ctx, a.frame, a.outbound)
		a.mu.Lock()
		a.err = err
		a.mu.Unlock()
	}
	return nil
}

type reentrantClaimStore struct {
	delegate *MemoryDedupStore
	once     sync.Once
	server   *ServerProtocol
	frame    Envelope
	outbound *OutboundQueue
	err      error
}

func (s *reentrantClaimStore) Claim(sessionID, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	s.once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s.err = s.server.ProcessFrame(ctx, s.frame, s.outbound)
	})
	return s.delegate.Claim(sessionID, msgID, fingerprint)
}

func (s *reentrantClaimStore) Complete(sessionID, msgID string, ack *Envelope) error {
	return s.delegate.Complete(sessionID, msgID, ack)
}

func (s *reentrantClaimStore) Abort(sessionID, msgID string) error {
	return s.delegate.Abort(sessionID, msgID)
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

type mismatchedACKStore struct{}

func (mismatchedACKStore) Claim(_ string, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s_other", Payload: mustPayload(AckPayload{RefID: msgID})}
	return false, &DedupRecord{State: DedupCompleted, Fingerprint: fingerprint, Ack: &ack}, nil
}

func (mismatchedACKStore) Complete(string, string, *Envelope) error { return nil }

func (mismatchedACKStore) Abort(string, string) error { return nil }

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

type blockingFirstClaimStore struct {
	delegate *MemoryDedupStore
	mu       sync.Mutex
	calls    int
	started  chan struct{}
	release  chan struct{}
}

func (s *blockingFirstClaimStore) Claim(sessionID, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		close(s.started)
		<-s.release
	}
	return s.delegate.Claim(sessionID, msgID, fingerprint)
}

func (s *blockingFirstClaimStore) Complete(sessionID, msgID string, ack *Envelope) error {
	return s.delegate.Complete(sessionID, msgID, ack)
}

func (s *blockingFirstClaimStore) Abort(sessionID, msgID string) error {
	return s.delegate.Abort(sessionID, msgID)
}

type blockingCompleteStore struct {
	delegate *MemoryDedupStore
	entered  chan struct{}
	release  chan struct{}
	fail     bool
}

func (s *blockingCompleteStore) Claim(sessionID, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	return s.delegate.Claim(sessionID, msgID, fingerprint)
}

func (s *blockingCompleteStore) Complete(sessionID, msgID string, ack *Envelope) error {
	close(s.entered)
	<-s.release
	if s.fail {
		return errors.New("complete failed")
	}
	return s.delegate.Complete(sessionID, msgID, ack)
}

func (s *blockingCompleteStore) Abort(sessionID, msgID string) error {
	return s.delegate.Abort(sessionID, msgID)
}

func configuredReadyClient(t *testing.T, mutate func(*ClientConfig)) (*ClientProtocol, *FakeClock, ConnectionGeneration) {
	t.Helper()
	clock := NewFakeClock(time.Unix(4, 0))
	config := DefaultClientConfig()
	config.Clock = clock
	mutate(&config)
	client, err := NewClientProtocol(config)
	if err != nil {
		t.Fatal(err)
	}
	gen := client.BeginConnect()
	if err := client.TransportConnected(gen); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartSession(gen, "test"); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})}
	if _, err := client.HandleIncoming(gen, welcome); err != nil {
		t.Fatal(err)
	}
	return client, clock, gen
}

func beginGapReplay(t *testing.T, client *ClientProtocol, gen ConnectionGeneration, replayTo uint64) {
	t.Helper()
	gap := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "gap", SessionID: "s_1", Seq: replayTo + 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, gap); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: replayTo})}
	if _, err := client.HandleIncoming(gen, welcome); err != nil {
		t.Fatal(err)
	}
}

type reentrantEventAppender struct {
	server   *ServerProtocol
	delegate EventAppender
	outbound *OutboundQueue
	result   error
}

func (a *reentrantEventAppender) Append(sessionID, eventID, eventType string, content []byte, timestamp int64) (Envelope, error) {
	a.result = a.server.PublishEvent(sessionID, "nested_event", eventType, json.RawMessage(`{}`), a.outbound)
	return a.delegate.Append(sessionID, eventID, eventType, content, timestamp)
}

func triggerClientGap(t *testing.T, client *ClientProtocol, generation ConnectionGeneration, seq uint64) Envelope {
	t.Helper()
	gap := Envelope{
		V: WireVersionV2, Type: FrameEvent, ID: "gap_event", SessionID: client.SessionID(), Seq: seq,
		Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)}),
	}
	actions, err := client.HandleIncoming(generation, gap)
	if err != nil || len(actions) != 1 || client.State() != StateResuming {
		t.Fatalf("gap did not start recovery: actions=%#v state=%s err=%v", actions, client.State(), err)
	}
	resume, ok := actions[0].(SendFrameAction)
	if !ok || resume.Frame.Type != FrameResume {
		t.Fatalf("gap did not emit RESUME: %#v", actions)
	}
	return resume.Frame
}

func assertClientRecoveryCleared(t *testing.T, client *ClientProtocol) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.replayBuffer) != 0 || client.replayTo != 0 || client.replayBytes != 0 ||
		len(client.resumeStateNamespaces) != 0 || client.resumeEventsComplete || client.gapResume ||
		len(client.stateQueries) != 0 || client.pendingPing != nil || !client.suspectAt.IsZero() {
		t.Fatalf("transient recovery state was retained: replay=%d replay_to=%d bytes=%d namespaces=%v events_complete=%v gap=%v queries=%d ping=%#v suspect=%v",
			len(client.replayBuffer), client.replayTo, client.replayBytes, client.resumeStateNamespaces,
			client.resumeEventsComplete, client.gapResume, len(client.stateQueries), client.pendingPing, client.suspectAt)
	}
}

func hasFullSyncAction(actions []Action, sessionID string) bool {
	for _, action := range actions {
		if fullSync, ok := action.(FullSyncRequiredAction); ok && fullSync.SessionID == sessionID {
			return true
		}
	}
	return false
}

type injectedReplayFailure struct {
	current uint64
	err     error
}

func (s *injectedReplayFailure) CurrentSeq(string) (uint64, error) { return s.current, nil }

func (s *injectedReplayFailure) Replay(string, uint64, uint64, ReplayLimits) ([]Envelope, error) {
	return nil, s.err
}

type mutexProbeClock struct {
	now       time.Time
	probe     func() bool
	lockHeld  bool
	callCount int
}

func (c *mutexProbeClock) Now() time.Time {
	c.callCount++
	if c.probe != nil && !c.probe() {
		c.lockHeld = true
	}
	return c.now
}

type panicOnceApplication struct {
	panicked bool
	calls    int
}

func (a *panicOnceApplication) HandleSend(context.Context, string, []byte) error {
	a.calls++
	if !a.panicked {
		a.panicked = true
		panic("injected application panic")
	}
	return nil
}

type failingThenHealthyStateStore struct {
	err    error
	object StateObject
}

func (s *failingThenHealthyStateStore) Get(context.Context, string, string) (StateObject, bool, error) {
	if s.err != nil {
		return StateObject{}, false, s.err
	}
	return cloneStateObject(s.object), true, nil
}

func (s *failingThenHealthyStateStore) Apply(_ context.Context, incoming StateObject) (StateObject, StateApplyResult, error) {
	return cloneStateObject(incoming), StateApplyApplied, nil
}

type reentrantQueryStateStore struct {
	server   *ServerProtocol
	object   StateObject
	outbound *OutboundQueue
	result   error
}

func (s *reentrantQueryStateStore) Get(context.Context, string, string) (StateObject, bool, error) {
	update := cloneStateObject(s.object)
	update.Version++
	s.result = s.server.PublishStateUpdate("s_state", "nested_state", update, s.outbound)
	return cloneStateObject(s.object), true, nil
}

func (s *reentrantQueryStateStore) Apply(_ context.Context, incoming StateObject) (StateObject, StateApplyResult, error) {
	return cloneStateObject(incoming), StateApplyApplied, nil
}

type reentrantReplayStore struct {
	delegate ReplayStore
	server   *ServerProtocol
	outbound *OutboundQueue
	result   error
}

func (s *reentrantReplayStore) CurrentSeq(sessionID string) (uint64, error) {
	return s.delegate.CurrentSeq(sessionID)
}

func (s *reentrantReplayStore) Replay(sessionID string, afterSeq, throughSeq uint64, limits ReplayLimits) ([]Envelope, error) {
	s.result = s.server.PublishEvent(sessionID, "nested_event", "test", json.RawMessage(`{}`), s.outbound)
	return s.delegate.Replay(sessionID, afterSeq, throughSeq, limits)
}

type panicCreateSessionRepository struct {
	delegate    *MemorySessionRepository
	panicCreate bool
}

func (r *panicCreateSessionRepository) Create(state SessionState) error {
	if r.panicCreate {
		panic("injected SessionRepository panic")
	}
	return r.delegate.Create(state)
}

func (r *panicCreateSessionRepository) Lookup(sessionID string, now time.Time) (SessionState, bool, error) {
	return r.delegate.Lookup(sessionID, now)
}

func requireProtocolErrorCode(t *testing.T, err error, code string) *ProtocolError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected protocol error %s", code)
	}
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("expected ProtocolError %s, got %T: %v", code, err, err)
	}
	if protocolErr.Code != code {
		t.Fatalf("protocol error code = %s, want %s: %v", protocolErr.Code, code, err)
	}
	return protocolErr
}
