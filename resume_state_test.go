package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type failingStateSnapshotProvider struct {
	err    error
	limits StateSnapshotLimits
}

func (p *failingStateSnapshotProvider) Snapshot(_ context.Context, _ []string, limits StateSnapshotLimits) ([]StateObject, error) {
	p.limits = limits
	return nil, p.err
}

type blockingStateSnapshotProvider struct {
	delegate StateSnapshotProvider
	started  chan struct{}
	release  chan struct{}
}

func (p *blockingStateSnapshotProvider) Snapshot(ctx context.Context, namespaces []string, limits StateSnapshotLimits) ([]StateObject, error) {
	close(p.started)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.release:
	}
	return p.delegate.Snapshot(ctx, namespaces, limits)
}

func reconnectStateClient(t *testing.T, client *ClientProtocol, oldGeneration ConnectionGeneration, namespaces []string) (ConnectionGeneration, Envelope) {
	t.Helper()
	if err := client.Disconnect(oldGeneration); err != nil {
		t.Fatal(err)
	}
	generation := client.BeginConnect()
	if err := client.TransportConnected(generation); err != nil {
		t.Fatal(err)
	}
	actions, err := client.ResumeWithState(generation, namespaces)
	if err != nil || len(actions) != 1 {
		t.Fatalf("ResumeWithState failed: actions=%d err=%v", len(actions), err)
	}
	return generation, actions[0].(SendFrameAction).Frame
}

func TestResumeWithoutStateSyncKeepsExistingEventOnlyBehavior(t *testing.T) {
	store := &testStateStore{objects: map[StateIdentity]StateObject{
		{Namespace: "message", ObjectID: "msg001"}: {Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)},
	}}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	live := NewOutboundQueue()
	if err := server.PublishEvent("s_state", "event_1", "message.new", json.RawMessage(`{}`), live); err != nil {
		t.Fatal(err)
	}
	_ = nextTestFrame(t, live)

	client, _, oldGeneration := readyStateSyncClient(t)
	if err := client.Disconnect(oldGeneration); err != nil {
		t.Fatal(err)
	}
	generation := client.BeginConnect()
	if err := client.TransportConnected(generation); err != nil {
		t.Fatal(err)
	}
	actions, err := client.Resume(generation)
	if err != nil {
		t.Fatal(err)
	}
	resume := actions[0].(SendFrameAction).Frame
	var resumePayload ResumePayload
	if err := decodePayload(resume.Payload, &resumePayload, true); err != nil || resumePayload.StateSync != nil {
		t.Fatalf("plain RESUME changed wire shape: payload=%#v err=%v", resumePayload, err)
	}
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, outbound)
	event := nextTestFrame(t, outbound)
	if queuedFrameCount(outbound) != 0 {
		t.Fatal("plain RESUME unexpectedly enqueued STATE_SNAPSHOT")
	}
	if actions, err = client.HandleIncoming(generation, welcome); err != nil || len(actions) != 0 {
		t.Fatalf("plain resume WELCOME failed: actions=%d err=%v", len(actions), err)
	}
	actions, err = client.HandleIncoming(generation, event)
	if err != nil || len(actions) != 2 || client.State() != StateReady || client.LastSeq() != 1 {
		t.Fatalf("plain EVENT resume changed: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
	}
}

func TestResumeWithStateGatesDeliveryUntilReplayAndSnapshotComplete(t *testing.T) {
	message := StateObject{Namespace: "message", ObjectID: "msg001", Version: 5, Data: json.RawMessage(`{"status":"read"}`)}
	task := StateObject{Namespace: "task", ObjectID: "task001", Version: 3, Data: json.RawMessage(`{"status":"open"}`)}
	store := &testStateStore{objects: map[StateIdentity]StateObject{message.Identity(): message, task.Identity(): task}}
	server, _, replay := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	live := NewOutboundQueue()
	for seq := 1; seq <= 2; seq++ {
		if err := server.PublishEvent("s_state", "event_"+string(rune('0'+seq)), "message.changed", json.RawMessage(`{}`), live); err != nil {
			t.Fatal(err)
		}
		_ = nextTestFrame(t, live)
	}

	client, _, oldGeneration := readyStateSyncClient(t)
	first := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "event_1", SessionID: "s_state", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(oldGeneration, first); err != nil {
		t.Fatal(err)
	}
	generation, resume := reconnectStateClient(t, client, oldGeneration, []string{"task", "message"})
	var request ResumePayload
	if err := decodePayload(resume.Payload, &request, true); err != nil {
		t.Fatal(err)
	}
	if request.StateSync == nil || !equalStrings(request.StateSync.Namespaces, []string{"message", "task"}) {
		t.Fatalf("RESUME namespaces are not canonical: %#v", request.StateSync)
	}

	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, outbound)
	replayed := nextTestFrame(t, outbound)
	snapshot := nextTestFrame(t, outbound)
	if welcome.Type != FrameWelcome || replayed.Type != FrameEvent || replayed.Seq != 2 || snapshot.Type != FrameStateSnapshot || snapshot.Seq != 0 {
		t.Fatalf("resume output order changed: %#v %#v %#v", welcome, replayed, snapshot)
	}
	var welcomePayload WelcomePayload
	if err := decodePayload(welcome.Payload, &welcomePayload, true); err != nil || welcomePayload.ReplayTo != 2 || welcomePayload.StateSync == nil {
		t.Fatalf("invalid resumed WELCOME: %#v err=%v", welcomePayload, err)
	}
	if actions, err := client.HandleIncoming(generation, welcome); err != nil || len(actions) != 0 {
		t.Fatalf("WELCOME leaked actions: %d %v", len(actions), err)
	}
	if actions, err := client.HandleIncoming(generation, replayed); err != nil || len(actions) != 0 || client.LastSeq() != 1 {
		t.Fatalf("partial synchronization leaked replay: actions=%d seq=%d err=%v", len(actions), client.LastSeq(), err)
	}
	actions, err := client.HandleIncoming(generation, snapshot)
	if err != nil || len(actions) != 4 || client.State() != StateReady || client.LastSeq() != 2 {
		t.Fatalf("snapshot completion failed: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
	}
	if _, ok := actions[0].(DeliverEventAction); !ok {
		t.Fatalf("EVENT replay delivery order changed: %#v", actions)
	}
	if _, ok := actions[1].(StateChangedAction); !ok {
		t.Fatalf("State delivery did not follow replay: %#v", actions)
	}
	if _, ok := actions[2].(StateChangedAction); !ok {
		t.Fatalf("missing State action: %#v", actions)
	}
	if _, ok := actions[3].(SessionReadyAction); !ok {
		t.Fatalf("READY did not remain final: %#v", actions)
	}
	if current, found := client.StateObject("message", "msg001"); !found || current.Version != 5 {
		t.Fatalf("message State missing after resume: %#v found=%v", current, found)
	}
	if currentSeq, err := replay.CurrentSeq("s_state"); err != nil || currentSeq != 2 {
		t.Fatalf("STATE snapshot affected EVENT high-water: seq=%d err=%v", currentSeq, err)
	}

	updated := message
	updated.Version = 6
	updated.Data = json.RawMessage(`{"status":"archived"}`)
	if err := server.PublishStateUpdate("s_state", "state_update_6", updated, outbound); err != nil {
		t.Fatal(err)
	}
	update := nextTestFrame(t, outbound)
	if _, err := client.HandleIncoming(generation, update); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 2 {
		t.Fatalf("live STATE_UPDATE changed EVENT seq: %d", client.LastSeq())
	}
}

func TestStaleStateDuringResumeFailsConservatively(t *testing.T) {
	client, _, oldGeneration := readyStateSyncClient(t)
	newer := Envelope{
		V: WireVersionV2, Type: FrameStateUpdate, ID: "update_6", SessionID: "s_state",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{Namespace: "message", ObjectID: "msg001", Version: 6, Data: json.RawMessage(`{}`)}}),
	}
	if _, err := client.HandleIncoming(oldGeneration, newer); err != nil {
		t.Fatal(err)
	}
	generation, _ := reconnectStateClient(t, client, oldGeneration, []string{"message"})
	welcome := Envelope{
		V: WireVersionV2, Type: FrameWelcome, SessionID: "s_state",
		Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: 0, StateSync: &ResumeStateSync{Namespaces: []string{"message"}}}),
	}
	if actions, err := client.HandleIncoming(generation, welcome); err != nil || len(actions) != 0 {
		t.Fatalf("resume acknowledgement failed: actions=%d err=%v", len(actions), err)
	}
	snapshot := Envelope{
		V: WireVersionV2, Type: FrameStateSnapshot, ID: "resume_snapshot", SessionID: "s_state",
		Payload: mustPayload(StateSnapshotPayload{States: []StateObject{{Namespace: "message", ObjectID: "msg001", Version: 5, Data: json.RawMessage(`{}`)}}}),
	}
	actions, err := client.HandleIncoming(generation, snapshot)
	if err != nil || len(actions) != 3 || client.State() != StateDisconnected || client.LastSeq() != 0 {
		t.Fatalf("stale resume State was not rejected: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
	}
	protocolAction, ok := actions[0].(ProtocolErrorAction)
	if !ok || protocolAction.Error.Code != ErrorStateSyncRequired {
		t.Fatalf("missing STATE_SYNC_REQUIRED action: %#v", actions)
	}
	if _, ok := actions[1].(FullSyncRequiredAction); !ok {
		t.Fatalf("missing terminal full-sync action: %#v", actions)
	}
	if current, found := client.StateObject("message", "msg001"); !found || current.Version != 6 {
		t.Fatalf("stale resume State overwrote client: %#v found=%v", current, found)
	}
}

func TestResumeStateUnavailableReturnsProtocolErrorBeforeReplay(t *testing.T) {
	server, _, _ := newStateSyncTestServer(t, &testStateStore{objects: make(map[StateIdentity]StateObject)})
	provider := &failingStateSnapshotProvider{err: errors.New("injected snapshot failure")}
	server.config.StateSnapshots = provider
	createStateSyncTestSession(t, server)
	live := NewOutboundQueue()
	if err := server.PublishEvent("s_state", "event_1", "message.new", json.RawMessage(`{}`), live); err != nil {
		t.Fatal(err)
	}
	_ = nextTestFrame(t, live)
	client, _, oldGeneration := readyStateSyncClient(t)
	generation, resume := reconnectStateClient(t, client, oldGeneration, []string{"message"})
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorStateUnavailable || !payload.Retryable {
		t.Fatalf("unexpected State failure response: frame=%#v payload=%#v", frame, payload)
	}
	if queuedFrameCount(outbound) != 0 {
		t.Fatal("failed State sync leaked WELCOME or EVENT replay")
	}
	if provider.limits.MaxObjects != server.config.Limits.MaxStateSnapshotObjects || provider.limits.MaxBytes <= 0 || provider.limits.MaxBytes > server.config.Limits.MaxStateSnapshotBytes {
		t.Fatalf("snapshot provider did not receive materialization bounds: %#v", provider.limits)
	}
	actions, err := client.HandleIncoming(generation, frame)
	if err != nil || len(actions) != 2 || client.State() != StateDisconnected || client.LastSeq() != 0 {
		t.Fatalf("client State failure handling mismatch: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
	}
}

func TestInterruptedResumeWithStateCanRetryWithoutAdvancingSequence(t *testing.T) {
	state := StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}
	store := &testStateStore{objects: map[StateIdentity]StateObject{state.Identity(): state}}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	live := NewOutboundQueue()
	if err := server.PublishEvent("s_state", "event_1", "message.new", json.RawMessage(`{}`), live); err != nil {
		t.Fatal(err)
	}
	_ = nextTestFrame(t, live)

	client, _, initialGeneration := readyStateSyncClient(t)
	firstGeneration, firstResume := reconnectStateClient(t, client, initialGeneration, []string{"message"})
	firstOutbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), firstResume, firstOutbound); err != nil {
		t.Fatal(err)
	}
	firstWelcome := nextTestFrame(t, firstOutbound)
	firstEvent := nextTestFrame(t, firstOutbound)
	firstSnapshot := nextTestFrame(t, firstOutbound)
	if _, err := client.HandleIncoming(firstGeneration, firstWelcome); err != nil {
		t.Fatal(err)
	}
	if actions, err := client.HandleIncoming(firstGeneration, firstEvent); err != nil || len(actions) != 0 || client.LastSeq() != 0 {
		t.Fatalf("first partial resume leaked: actions=%d seq=%d err=%v", len(actions), client.LastSeq(), err)
	}

	secondGeneration, secondResume := reconnectStateClient(t, client, firstGeneration, []string{"message"})
	if _, err := client.HandleIncoming(firstGeneration, firstSnapshot); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("old resume snapshot was not generation-fenced: %v", err)
	}
	secondOutbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), secondResume, secondOutbound); err != nil {
		t.Fatal(err)
	}
	frames := []Envelope{nextTestFrame(t, secondOutbound), nextTestFrame(t, secondOutbound), nextTestFrame(t, secondOutbound)}
	for i, frame := range frames {
		actions, err := client.HandleIncoming(secondGeneration, frame)
		if err != nil {
			t.Fatalf("retry frame %d failed: %v", i, err)
		}
		if i < 2 && len(actions) != 0 {
			t.Fatalf("retry leaked action before snapshot: frame=%d actions=%#v", i, actions)
		}
		if i == 2 && (len(actions) != 3 || client.State() != StateReady || client.LastSeq() != 1) {
			t.Fatalf("retry did not complete once: actions=%d state=%s seq=%d", len(actions), client.State(), client.LastSeq())
		}
	}
}

func TestResumeSnapshotSerializesBeforeConcurrentLiveStateUpdate(t *testing.T) {
	state := StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}
	store := &testStateStore{objects: map[StateIdentity]StateObject{state.Identity(): state}}
	server, _, _ := newStateSyncTestServer(t, store)
	blocker := &blockingStateSnapshotProvider{delegate: store, started: make(chan struct{}), release: make(chan struct{})}
	server.config.StateSnapshots = blocker
	createStateSyncTestSession(t, server)
	live := NewOutboundQueue()
	if err := server.PublishEvent("s_state", "event_1", "message.new", json.RawMessage(`{}`), live); err != nil {
		t.Fatal(err)
	}
	_ = nextTestFrame(t, live)
	client, _, oldGeneration := readyStateSyncClient(t)
	_, resume := reconnectStateClient(t, client, oldGeneration, []string{"message"})
	outbound := NewOutboundQueue()
	resumeDone := make(chan error, 1)
	go func() { resumeDone <- server.HandleIncoming(context.Background(), resume, outbound) }()
	<-blocker.started

	updated := state
	updated.Version = 2
	if err := server.PublishStateUpdate("s_state", "update_2", updated, outbound); !errors.Is(err, ErrStreamCallbackActive) {
		t.Fatalf("concurrent stream entry returned %v, want ErrStreamCallbackActive", err)
	}
	close(blocker.release)
	if err := <-resumeDone; err != nil {
		t.Fatal(err)
	}
	if err := server.PublishStateUpdate("s_state", "update_2", updated, outbound); err != nil {
		t.Fatal(err)
	}
	frames := []Envelope{
		nextTestFrame(t, outbound),
		nextTestFrame(t, outbound),
		nextTestFrame(t, outbound),
		nextTestFrame(t, outbound),
	}
	want := []FrameType{FrameWelcome, FrameEvent, FrameStateSnapshot, FrameStateUpdate}
	for i := range frames {
		if frames[i].Type != want[i] {
			t.Fatalf("resume/live State order[%d]=%s want %s", i, frames[i].Type, want[i])
		}
	}
}

type reentrantStateSnapshotProvider struct {
	server   *ServerProtocol
	delegate StateSnapshotProvider
	outbound *OutboundQueue
	result   error
}

func (p *reentrantStateSnapshotProvider) Snapshot(ctx context.Context, namespaces []string, limits StateSnapshotLimits) ([]StateObject, error) {
	object := StateObject{Namespace: "message", ObjectID: "msg001", Version: 2, Data: json.RawMessage(`{}`)}
	p.result = p.server.PublishStateUpdate("s_state", "nested_update", object, p.outbound)
	return p.delegate.Snapshot(ctx, namespaces, limits)
}

func TestResumeSnapshotCallbackReentryFailsWithoutDeadlock(t *testing.T) {
	state := StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}
	store := &testStateStore{objects: map[StateIdentity]StateObject{state.Identity(): state}}
	server, _, _ := newStateSyncTestServer(t, store)
	outbound := NewOutboundQueue()
	provider := &reentrantStateSnapshotProvider{server: server, delegate: store, outbound: outbound}
	server.config.StateSnapshots = provider
	createStateSyncTestSession(t, server)

	client, _, oldGeneration := readyStateSyncClient(t)
	_, resume := reconnectStateClient(t, client, oldGeneration, []string{"message"})
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(provider.result, ErrStreamCallbackActive) {
		t.Fatalf("nested stream entry returned %v, want ErrStreamCallbackActive", provider.result)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameWelcome {
		t.Fatalf("resume did not complete after rejected reentry: %#v", frame)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameStateSnapshot {
		t.Fatalf("resume snapshot missing after rejected reentry: %#v", frame)
	}
}

func TestResumeStateSyncValidation(t *testing.T) {
	valid := Envelope{
		V: WireVersionV2, Type: FrameResume, SessionID: "s_1",
		Payload: mustPayload(ResumePayload{LastSeq: 1, StateSync: &ResumeStateSync{Namespaces: []string{"message", "task"}}}),
	}
	if err := ValidateFrame(&valid, DefaultLimits(), true); err != nil {
		t.Fatalf("valid State RESUME rejected: %v", err)
	}
	tests := []ResumeStateSync{
		{},
		{Namespaces: []string{"Message"}},
		{Namespaces: []string{"task", "message"}},
		{Namespaces: []string{"message", "message"}},
	}
	for _, stateSync := range tests {
		frame := valid
		frame.Payload = mustPayload(ResumePayload{LastSeq: 1, StateSync: &stateSync})
		if err := ValidateFrame(&frame, DefaultLimits(), true); err == nil {
			t.Fatalf("invalid State RESUME accepted: %#v", stateSync)
		}
	}
}

func queuedFrameCount(queue *OutboundQueue) int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return len(queue.frames)
}
