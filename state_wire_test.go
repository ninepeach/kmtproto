package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type testStateStore struct {
	mu      sync.Mutex
	objects map[StateIdentity]StateObject
}

type countingStateStore struct {
	*testStateStore
	getMu sync.Mutex
	gets  []string
}

func (s *countingStateStore) Get(ctx context.Context, namespace, objectID string) (StateObject, bool, error) {
	s.getMu.Lock()
	s.gets = append(s.gets, objectID)
	s.getMu.Unlock()
	return s.testStateStore.Get(ctx, namespace, objectID)
}

func (s *countingStateStore) getCount() int {
	s.getMu.Lock()
	defer s.getMu.Unlock()
	return len(s.gets)
}

func (s *testStateStore) Get(_ context.Context, namespace, objectID string) (StateObject, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, found := s.objects[StateIdentity{Namespace: namespace, ObjectID: objectID}]
	return cloneStateObject(object), found, nil
}

func (s *testStateStore) Apply(_ context.Context, incoming StateObject) (StateObject, StateApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := incoming.Identity()
	current, found := s.objects[identity]
	var currentPointer *StateObject
	if found {
		currentPointer = &current
	}
	committed, result, err := ApplyStateObject(currentPointer, incoming, DefaultLimits())
	if err == nil && result == StateApplyApplied {
		s.objects[identity] = cloneStateObject(committed)
	}
	return committed, result, err
}

func (s *testStateStore) Snapshot(_ context.Context, namespaces []string, limits StateSnapshotLimits) ([]StateObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	allowed := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		allowed[namespace] = struct{}{}
	}
	accumulator, err := newStateSnapshotAccumulator(DefaultLimits(), limits)
	if err != nil {
		return nil, err
	}
	for _, object := range s.objects {
		if _, ok := allowed[object.Namespace]; ok {
			if err := accumulator.Add(object); err != nil {
				return nil, err
			}
		}
	}
	return append([]StateObject(nil), accumulator.states...), nil
}

func readyStateSyncClient(t *testing.T) (*Client, *FakeClock, ConnectionGeneration) {
	t.Helper()
	clock := NewFakeClock(time.Unix(1_000, 0))
	config := DefaultClientConfig()
	config.Clock = clock
	config.Capabilities = []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{1}, Required: true}}
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	generation := client.BeginConnect()
	if err := client.TransportConnected(generation); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartSession(generation, "state-client"); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{
		V:         WireVersionV2,
		Type:      FrameWelcome,
		SessionID: "s_state",
		Payload: mustPayload(WelcomePayload{
			Mode:                 WelcomeModeNew,
			ServerTime:           clock.Now().UnixMilli(),
			AcceptedCapabilities: []NegotiatedCapability{{Name: CapabilityStateSync, Version: 1}},
		}),
	}
	if _, err := client.HandleIncoming(generation, welcome); err != nil {
		t.Fatal(err)
	}
	return client, clock, generation
}

func newStateSyncTestServer(t *testing.T, stateStore StateStore) (*Server, *FakeClock, *MemoryReplayStore) {
	t.Helper()
	clock := NewFakeClock(time.Unix(1_100, 0))
	registry, err := NewCapabilityRegistry([]CapabilitySpec{{Name: CapabilityStateSync, Versions: []uint16{1}}}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultServerConfig()
	config.Clock = clock
	config.Capabilities = registry
	config.StateStore = stateStore
	if snapshots, ok := stateStore.(StateSnapshotProvider); ok {
		config.StateSnapshots = snapshots
	}
	config.NewSessionID = func() (string, error) { return "s_state", nil }
	config.NewFrameID = func() (string, error) { return "resume_snapshot_1", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	return server, clock, replay
}

func createStateSyncTestSession(t *testing.T, server *Server) {
	t.Helper()
	outbound := NewOutboundQueue()
	hello := Envelope{
		V:    WireVersionV2,
		Type: FrameHello,
		Payload: mustPayload(HelloPayload{Capabilities: []CapabilityOffer{
			{Name: CapabilityStateSync, Versions: []uint16{1}, Required: true},
		}}),
	}
	if err := server.HandleIncoming(context.Background(), hello, outbound); err != nil {
		t.Fatal(err)
	}
	if welcome := nextTestFrame(t, outbound); welcome.Type != FrameWelcome || welcome.SessionID != "s_state" {
		t.Fatalf("unexpected State WELCOME: %#v", welcome)
	}
}

func TestStateQueryWireAndServerSnapshot(t *testing.T) {
	object := StateObject{Namespace: "message", ObjectID: "msg001", Version: 5, Data: json.RawMessage(`{"status":"read"}`)}
	store := &testStateStore{objects: map[StateIdentity]StateObject{object.Identity(): object}}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)

	client, _, generation := readyStateSyncClient(t)
	actions, err := client.QueryState("query_1", "message", []string{"missing", "msg001"})
	if err != nil || len(actions) != 1 {
		t.Fatalf("STATE_QUERY creation failed: actions=%d err=%v", len(actions), err)
	}
	query := actions[0].(SendFrameAction).Frame
	if query.Type != FrameStateQuery || query.ID != "query_1" || query.SessionID != "s_state" || query.Seq != 0 {
		t.Fatalf("unexpected STATE_QUERY: %#v", query)
	}
	if err := ValidateFrame(&query, DefaultLimits(), true); err != nil {
		t.Fatal(err)
	}

	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), query, outbound); err != nil {
		t.Fatal(err)
	}
	snapshot := nextTestFrame(t, outbound)
	if snapshot.Type != FrameStateSnapshot || snapshot.ID != query.ID || snapshot.Seq != 0 {
		t.Fatalf("unexpected STATE_SNAPSHOT: %#v", snapshot)
	}
	var payload StateSnapshotPayload
	if err := decodePayload(snapshot.Payload, &payload, true); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload.States, []StateObject{object}) {
		t.Fatalf("snapshot mismatch: got %#v want %#v", payload.States, []StateObject{object})
	}
	actions, err = client.HandleIncoming(generation, snapshot)
	if err != nil || len(actions) != 1 {
		t.Fatalf("STATE_SNAPSHOT handling failed: actions=%d err=%v", len(actions), err)
	}
	changed, ok := actions[0].(StateChangedAction)
	if !ok || changed.Result != StateApplyApplied || changed.State.Version != 5 {
		t.Fatalf("unexpected State action: %#v", actions)
	}
	if cached, found := client.StateObject("message", "msg001"); !found || cached.Version != 5 {
		t.Fatalf("State snapshot was not retained: %#v found=%v", cached, found)
	}
}

func TestStateQueryStopsAccumulatingAtSnapshotByteLimit(t *testing.T) {
	objects := make(map[StateIdentity]StateObject)
	for _, id := range []string{"a", "b", "c"} {
		object := StateObject{Namespace: "message", ObjectID: id, Version: 1, Data: json.RawMessage(`{"value":"012345678901234567890123456789"}`)}
		objects[object.Identity()] = object
	}
	store := &countingStateStore{testStateStore: &testStateStore{objects: objects}}
	server, _, _ := newStateSyncTestServer(t, store)
	one := mustPayload(StateSnapshotPayload{States: []StateObject{objects[StateIdentity{Namespace: "message", ObjectID: "a"}]}})
	server.config.Limits.MaxStateSnapshotBytes = len(one)
	createStateSyncTestSession(t, server)

	query := Envelope{
		V: WireVersionV2, Type: FrameStateQuery, ID: "bounded_query", SessionID: "s_state",
		Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"a", "b", "c"}}),
	}
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), query, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorBadRequest {
		t.Fatalf("oversized snapshot did not produce BAD_REQUEST: frame=%#v payload=%#v", frame, payload)
	}
	if got := store.getCount(); got != 2 {
		t.Fatalf("State query fetched %d objects; want early stop after 2", got)
	}
}

func TestStateQueryValidationRejectsInvalidInput(t *testing.T) {
	valid := func() Envelope {
		return Envelope{
			V:         WireVersionV2,
			Type:      FrameStateQuery,
			ID:        "query_1",
			SessionID: "s_1",
			Payload:   mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"msg001"}}),
		}
	}
	tests := []struct {
		name   string
		mutate func(*Envelope, *Limits)
	}{
		{name: "missing id", mutate: func(frame *Envelope, _ *Limits) { frame.ID = "" }},
		{name: "missing session", mutate: func(frame *Envelope, _ *Limits) { frame.SessionID = "" }},
		{name: "sequence", mutate: func(frame *Envelope, _ *Limits) { frame.Seq = 1 }},
		{name: "invalid namespace", mutate: func(frame *Envelope, _ *Limits) {
			frame.Payload = mustPayload(StateQueryPayload{Namespace: "Message", ObjectIDs: []string{"msg001"}})
		}},
		{name: "empty objects", mutate: func(frame *Envelope, _ *Limits) {
			frame.Payload = mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{}})
		}},
		{name: "invalid object", mutate: func(frame *Envelope, _ *Limits) {
			frame.Payload = mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"bad\nobject"}})
		}},
		{name: "duplicate object", mutate: func(frame *Envelope, _ *Limits) {
			frame.Payload = mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"msg001", "msg001"}})
		}},
		{name: "query limit", mutate: func(frame *Envelope, limits *Limits) {
			limits.MaxStateQueryObjects = 1
			frame.Payload = mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"msg001", "msg002"}})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := valid()
			limits := DefaultLimits()
			test.mutate(&frame, &limits)
			if err := ValidateFrame(&frame, limits, true); err == nil {
				t.Fatal("expected STATE_QUERY validation failure")
			}
		})
	}
}

func TestStateSnapshotValidation(t *testing.T) {
	validObject := StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}
	valid := Envelope{V: WireVersionV2, Type: FrameStateSnapshot, ID: "query_1", SessionID: "s_1", Payload: mustPayload(StateSnapshotPayload{States: []StateObject{validObject}})}
	if err := ValidateFrame(&valid, DefaultLimits(), true); err != nil {
		t.Fatalf("valid STATE_SNAPSHOT rejected: %v", err)
	}

	invalid := valid
	badObject := validObject
	badObject.Version = 0
	invalid.Payload = mustPayload(StateSnapshotPayload{States: []StateObject{badObject}})
	if err := ValidateFrame(&invalid, DefaultLimits(), true); err == nil {
		t.Fatal("invalid State Object in snapshot was accepted")
	}

	empty := valid
	empty.Payload = json.RawMessage(`{"states":[]}`)
	if err := ValidateFrame(&empty, DefaultLimits(), true); err != nil {
		t.Fatalf("explicit empty STATE_SNAPSHOT rejected: %v", err)
	}
	missing := valid
	missing.Payload = json.RawMessage(`{}`)
	if err := ValidateFrame(&missing, DefaultLimits(), true); err == nil {
		t.Fatal("STATE_SNAPSHOT without states was accepted")
	}
}

func TestStateUpdateAppliesNewerAndRejectsStale(t *testing.T) {
	client, _, generation := readyStateSyncClient(t)
	newer := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateUpdate,
		ID:        "update_6",
		SessionID: "s_state",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{
			Namespace: "message", ObjectID: "msg001", Version: 6, Data: json.RawMessage(`{"status":"archived"}`),
		}}),
	}
	actions, err := client.HandleIncoming(generation, newer)
	if err != nil || len(actions) != 1 {
		t.Fatalf("valid STATE_UPDATE failed: actions=%d err=%v", len(actions), err)
	}
	if changed, ok := actions[0].(StateChangedAction); !ok || changed.State.Version != 6 || changed.Result != StateApplyApplied {
		t.Fatalf("unexpected State update action: %#v", actions)
	}

	stale := newer
	stale.ID = "update_5"
	stale.Payload = mustPayload(StateUpdatePayload{State: StateObject{
		Namespace: "message", ObjectID: "msg001", Version: 5, Data: json.RawMessage(`{"status":"read"}`),
	}})
	if _, err := client.HandleIncoming(generation, stale); !errors.Is(err, ErrStateStale) {
		t.Fatalf("stale STATE_UPDATE returned %v, want ErrStateStale", err)
	}
	if cached, found := client.StateObject("message", "msg001"); !found || cached.Version != 6 {
		t.Fatalf("stale update changed State: %#v found=%v", cached, found)
	}
}

func TestStateFramesRequireNegotiatedCapability(t *testing.T) {
	client, _, generation := readyClient(t)
	update := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateUpdate,
		ID:        "update_1",
		SessionID: "s_1",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{
			Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`),
		}}),
	}
	_, err := client.HandleIncoming(generation, update)
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != ErrorProtocolViolation {
		t.Fatalf("client accepted STATE_UPDATE without capability: %v", err)
	}

	server, _, _ := newTestServer(t, &recordingApp{})
	createTestSession(t, server)
	query := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateQuery,
		ID:        "query_1",
		SessionID: "s_1",
		Payload:   mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"msg001"}}),
	}
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), query, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorProtocolViolation {
		t.Fatalf("server accepted STATE_QUERY without capability: frame=%#v payload=%#v", frame, payload)
	}
}

func TestStateFramesDoNotAffectEventSequenceOrReplay(t *testing.T) {
	client, _, generation := readyStateSyncClient(t)
	event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "event_1", SessionID: "s_state", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(generation, event); err != nil {
		t.Fatal(err)
	}
	if _, err := client.QueryState("query_1", "message", []string{"msg001"}); err != nil {
		t.Fatal(err)
	}
	snapshot := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateSnapshot,
		ID:        "query_1",
		SessionID: "s_state",
		Payload: mustPayload(StateSnapshotPayload{States: []StateObject{
			{Namespace: "message", ObjectID: "msg001", Version: 50, Data: json.RawMessage(`{}`)},
		}}),
	}
	if _, err := client.HandleIncoming(generation, snapshot); err != nil {
		t.Fatal(err)
	}
	update := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateUpdate,
		ID:        "update_51",
		SessionID: "s_state",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{
			Namespace: "message", ObjectID: "msg001", Version: 51, Data: json.RawMessage(`{"updated":true}`),
		}}),
	}
	if _, err := client.HandleIncoming(generation, update); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 1 || client.State() != StateReady {
		t.Fatalf("State frames changed EVENT state: seq=%d state=%s", client.LastSeq(), client.State())
	}

	store := &testStateStore{objects: make(map[StateIdentity]StateObject)}
	server, _, replay := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	outbound := NewOutboundQueue()
	if err := server.PublishStateUpdate("s_state", "update_1", StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}, outbound); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameStateUpdate || frame.Seq != 0 {
		t.Fatalf("unexpected published STATE_UPDATE: %#v", frame)
	}
	if seq, err := replay.CurrentSeq("s_state"); err != nil || seq != 0 {
		t.Fatalf("STATE_UPDATE entered EVENT replay stream: seq=%d err=%v", seq, err)
	}
}
