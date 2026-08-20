package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

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

func TestGapResumeSyncRequiredTerminatesRecovery(t *testing.T) {
	client, _, generation := readyClient(t)
	if _, err := client.SendPing(generation, "pending_ping"); err != nil {
		t.Fatal(err)
	}
	triggerClientGap(t, client, generation, 2)
	if client.LastSeq() != 0 {
		t.Fatalf("gap advanced last_seq: %d", client.LastSeq())
	}
	errorFrame := Envelope{
		V: WireVersionV2, Type: FrameError, SessionID: "s_1",
		Payload: mustPayload(ErrorPayload{Code: ErrorSyncRequired, Retryable: false}),
	}
	actions, err := client.HandleIncoming(generation, errorFrame)
	if err != nil || client.State() != StateDisconnected || !hasFullSyncAction(actions, "s_1") {
		t.Fatalf("SYNC_REQUIRED did not terminate recovery: actions=%#v state=%s err=%v", actions, client.State(), err)
	}
	assertClientRecoveryCleared(t, client)
}

func TestGapResumeInvalidSessionAbandonsAndRequestsFullSync(t *testing.T) {
	client, _, generation := readyClient(t)
	triggerClientGap(t, client, generation, 2)
	errorFrame := Envelope{
		V: WireVersionV2, Type: FrameError, SessionID: "s_1",
		Payload: mustPayload(ErrorPayload{Code: ErrorInvalidSession, Retryable: false}),
	}
	actions, err := client.HandleIncoming(generation, errorFrame)
	if err != nil || !hasFullSyncAction(actions, "s_1") {
		t.Fatalf("INVALID_SESSION did not request full sync: actions=%#v err=%v", actions, err)
	}
	if client.State() != StateDisconnected || client.SessionID() != "" || client.LastSeq() != 0 {
		t.Fatalf("invalid Session survived recovery failure: state=%s session=%q seq=%d", client.State(), client.SessionID(), client.LastSeq())
	}
	assertClientRecoveryCleared(t, client)
}

type injectedReplayFailure struct {
	current uint64
	err     error
}

func (s *injectedReplayFailure) CurrentSeq(string) (uint64, error) { return s.current, nil }

func (s *injectedReplayFailure) Replay(string, uint64, uint64, ReplayLimits) ([]Envelope, error) {
	return nil, s.err
}

func TestGapResumeReplayFailureReturnsTerminalError(t *testing.T) {
	client, clock, generation := readyClient(t)
	resume := triggerClientGap(t, client, generation, 2)
	sessions := NewMemorySessionRepository()
	if err := sessions.Create(SessionState{SessionID: "s_1", ExpiresAt: clock.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	config := DefaultServerConfig()
	config.Clock = clock
	appender := NewMemoryReplayStore()
	server, err := NewServerProtocol(config, sessions, NewMemoryDedupStore(clock, config.DedupTTL),
		&injectedReplayFailure{current: 2, err: errors.New("injected replay failure")}, appender, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorInternal || !payload.Retryable {
		t.Fatalf("replay failure was not mapped deterministically: frame=%#v payload=%#v", frame, payload)
	}
	actions, err := client.HandleIncoming(generation, frame)
	if err != nil || client.State() != StateDisconnected || len(actions) != 2 {
		t.Fatalf("client remained in recovery after replay failure: actions=%#v state=%s err=%v", actions, client.State(), err)
	}
	if _, ok := actions[1].(CloseConnectionAction); !ok {
		t.Fatalf("replay failure did not emit terminal close: %#v", actions)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("failed replay advanced last_seq: %d", client.LastSeq())
	}
	assertClientRecoveryCleared(t, client)
}

func TestGapDisconnectRetryPreservesSequenceAndFencesOldGeneration(t *testing.T) {
	client, _, firstGeneration := readyClient(t)
	first := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "event_1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(firstGeneration, first); err != nil {
		t.Fatal(err)
	}
	triggerClientGap(t, client, firstGeneration, 3)
	if err := client.Disconnect(firstGeneration); err != nil {
		t.Fatal(err)
	}
	assertClientRecoveryCleared(t, client)
	if client.LastSeq() != 1 {
		t.Fatalf("disconnect changed confirmed sequence: %d", client.LastSeq())
	}

	secondGeneration := client.BeginConnect()
	if err := client.TransportConnected(secondGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resume(secondGeneration); err != nil {
		t.Fatal(err)
	}
	oldWelcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 2, ReplayTo: 2})}
	if _, err := client.HandleIncoming(firstGeneration, oldWelcome); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("old generation completed recovery: %v", err)
	}
	if client.State() != StateResuming || client.LastSeq() != 1 {
		t.Fatal("old generation mutated active recovery")
	}
	if actions, err := client.HandleIncoming(secondGeneration, oldWelcome); err != nil || len(actions) != 0 {
		t.Fatalf("new resume WELCOME failed: actions=%#v err=%v", actions, err)
	}
	replayed := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "event_2", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if actions, err := client.HandleIncoming(secondGeneration, replayed); err != nil || len(actions) != 2 {
		t.Fatalf("retried recovery failed: actions=%#v err=%v", actions, err)
	}
	if client.State() != StateReady || client.LastSeq() != 2 {
		t.Fatalf("retry did not complete: state=%s seq=%d", client.State(), client.LastSeq())
	}
	assertClientRecoveryCleared(t, client)
}

func TestMalformedReplayCannotStrandClientOrAdvanceSequence(t *testing.T) {
	client, _, generation := readyClient(t)
	triggerClientGap(t, client, generation, 3)
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: 2})}
	if _, err := client.HandleIncoming(generation, welcome); err != nil {
		t.Fatal(err)
	}
	badReplay := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "event_2", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	actions, err := client.HandleIncoming(generation, badReplay)
	if err != nil || client.State() != StateDisconnected || !hasFullSyncAction(actions, "s_1") {
		t.Fatalf("malformed replay was not terminal: actions=%#v state=%s err=%v", actions, client.State(), err)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("malformed replay advanced last_seq: %d", client.LastSeq())
	}
	assertClientRecoveryCleared(t, client)
}

func TestStateUpdateNeverChangesEventSequenceInvariant(t *testing.T) {
	client, _, generation := readyStateSyncClient(t)
	update := Envelope{
		V: WireVersionV2, Type: FrameStateUpdate, ID: "state_1", SessionID: "s_state",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)}}),
	}
	if _, err := client.HandleIncoming(generation, update); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("STATE changed EVENT sequence: %d", client.LastSeq())
	}
}

func TestCapabilityNameAndVersionCannotBeAmbiguous(t *testing.T) {
	clientConfig := DefaultClientConfig()
	clientConfig.Capabilities = []CapabilityOffer{
		{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}},
		{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}},
	}
	if _, err := NewClientProtocol(clientConfig); protocolErrorCode(err) != ErrorInvalidCapability {
		t.Fatalf("duplicate capability name was accepted: %v", err)
	}
	if _, err := NewCapabilityRegistry([]CapabilitySpec{
		{Name: "presence", Versions: []uint16{1}},
		{Name: "presence", Versions: []uint16{2}},
	}, DefaultLimits()); protocolErrorCode(err) != ErrorInvalidCapability {
		t.Fatalf("ambiguous registry capability was accepted: %v", err)
	}
	frame := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{Capabilities: []CapabilityOffer{{Name: "state-sync-v2", Versions: []uint16{2}}}})}
	if err := ValidateFrame(&frame, DefaultLimits(), true); protocolErrorCode(err) != ErrorInvalidCapability {
		t.Fatalf("version-encoded capability name was accepted: %v", err)
	}
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

func TestClockIsSampledOutsideClientAndDedupMutexes(t *testing.T) {
	clientClock := &mutexProbeClock{now: time.Unix(4_000, 0)}
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clientClock
	client, err := NewClientProtocol(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientClock.probe = func() bool {
		if !client.mu.TryLock() {
			return false
		}
		client.mu.Unlock()
		return true
	}
	generation := client.BeginConnect()
	if err := client.TransportConnected(generation); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartSession(generation, "clock-client"); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_clock", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})}
	if _, err := client.HandleIncoming(generation, welcome); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnqueueSend("m", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if clientClock.lockHeld || clientClock.callCount == 0 {
		t.Fatalf("ClientProtocol sampled Clock under its mutex: held=%v calls=%d", clientClock.lockHeld, clientClock.callCount)
	}

	dedupClock := &mutexProbeClock{now: time.Unix(5_000, 0)}
	store := NewMemoryDedupStore(dedupClock, time.Hour)
	dedupClock.probe = func() bool {
		if !store.mu.TryLock() {
			return false
		}
		store.mu.Unlock()
		return true
	}
	if claimed, _, err := store.Claim("s", "m", SendFingerprint{}); err != nil || !claimed {
		t.Fatalf("Claim failed: claimed=%v err=%v", claimed, err)
	}
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s", Payload: mustPayload(AckPayload{RefID: "m"})}
	if err := store.Complete("s", "m", &ack); err != nil {
		t.Fatal(err)
	}
	if dedupClock.lockHeld || dedupClock.callCount < 2 {
		t.Fatalf("dedup store sampled Clock under its mutex: held=%v calls=%d", dedupClock.lockHeld, dedupClock.callCount)
	}
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

func TestApplicationPanicReleasesSendFlight(t *testing.T) {
	app := &panicOnceApplication{}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	firstOut := NewOutboundQueue()
	first := Envelope{V: WireVersionV2, Type: FrameSend, ID: "panic_send", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.HandleIncoming(context.Background(), first, firstOut); err != nil {
		t.Fatal(err)
	}
	assertErrorFrameCode(t, nextTestFrame(t, firstOut), ErrorInternal)
	secondOut := NewOutboundQueue()
	second := Envelope{V: WireVersionV2, Type: FrameSend, ID: "next_send", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.HandleIncoming(context.Background(), second, secondOut); err != nil {
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

func TestFailingStateStoreReleasesSessionLane(t *testing.T) {
	object := StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)}
	store := &failingThenHealthyStateStore{err: errors.New("injected StateStore failure"), object: object}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	query := func(id string) Envelope {
		return Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: id, SessionID: "s_state", Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"m"}})}
	}
	if err := server.HandleIncoming(context.Background(), query("q1"), NewOutboundQueue()); err == nil {
		t.Fatal("expected StateStore failure")
	}
	server.laneMu.Lock()
	remaining := len(server.lanes)
	server.laneMu.Unlock()
	if remaining != 0 {
		t.Fatalf("failed StateStore call stranded Session lane: %d", remaining)
	}
	store.err = nil
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), query("q2"), outbound); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameStateSnapshot {
		t.Fatalf("future State query remained stranded: %#v", frame)
	}
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

func TestStateStoreReentryFailsWithoutDeadlock(t *testing.T) {
	store := &reentrantQueryStateStore{
		object:   StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)},
		outbound: NewOutboundQueue(),
	}
	server, _, _ := newStateSyncTestServer(t, store)
	store.server = server
	createStateSyncTestSession(t, server)
	query := Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: "q", SessionID: "s_state", Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"m"}})}
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), query, outbound); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(store.result, ErrStreamCallbackActive) {
		t.Fatalf("same-Session StateStore reentry returned %v", store.result)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameStateSnapshot {
		t.Fatalf("outer query failed after rejected reentry: %#v", frame)
	}
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
	server, err := NewServerProtocol(config, sessions, NewMemoryDedupStore(clock, config.DedupTTL), replay, baseReplay, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	replay.server = server
	outbound := NewOutboundQueue()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
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

func TestServerAdmissionPanicDoesNotStrandAdmission(t *testing.T) {
	clock := NewFakeClock(time.Unix(6_000, 0))
	repository := &panicCreateSessionRepository{delegate: NewMemorySessionRepository(), panicCreate: true}
	config := DefaultServerConfig()
	config.Clock = clock
	nextID := 0
	config.NewSessionID = func() (string, error) {
		nextID++
		if nextID == 1 {
			return "s_panic", nil
		}
		return "s_recovered", nil
	}
	replay := NewMemoryReplayStore()
	server, err := NewServerProtocol(config, repository, NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	connection := NewServerAdmission()
	generation, _ := connection.Replace()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := connection.Handle(context.Background(), server, generation, hello); err == nil {
		t.Fatal("expected recovered repository panic")
	}
	if connection.State() != ServerAdmissionClosed {
		t.Fatalf("panic left admission in an ambiguous state: %s", connection.State())
	}
	repository.panicCreate = false
	generation, outbound := connection.Replace()
	if err := connection.Handle(context.Background(), server, generation, hello); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameWelcome || connection.State() != ServerAdmissionReady {
		t.Fatalf("replacement admission remained stranded: frame=%#v state=%s", frame, connection.State())
	}
}
