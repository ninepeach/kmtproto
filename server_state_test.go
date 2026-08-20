package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestStateQueryAndLiveUpdateUseOneStreamLane(t *testing.T) {
	object := StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)}
	store := &blockingQueryStore{started: make(chan struct{}), release: make(chan struct{}), object: object}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	queryOut := NewOutboundQueue()
	query := Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: "q", SessionID: "s_state", Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"m"}})}
	queryDone := make(chan error, 1)
	go func() { queryDone <- server.ProcessFrame(context.Background(), query, queryOut) }()
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

func TestFailingStateStoreReleasesSessionLane(t *testing.T) {
	object := StateObject{Namespace: "message", ObjectID: "m", Version: 1, Data: json.RawMessage(`{}`)}
	store := &failingThenHealthyStateStore{err: errors.New("injected StateStore failure"), object: object}
	server, _, _ := newStateSyncTestServer(t, store)
	createStateSyncTestSession(t, server)
	query := func(id string) Envelope {
		return Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: id, SessionID: "s_state", Payload: mustPayload(StateQueryPayload{Namespace: "message", ObjectIDs: []string{"m"}})}
	}
	if err := server.ProcessFrame(context.Background(), query("q1"), NewOutboundQueue()); err == nil {
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
	if err := server.ProcessFrame(context.Background(), query("q2"), outbound); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameStateSnapshot {
		t.Fatalf("future State query remained stranded: %#v", frame)
	}
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
	if err := server.ProcessFrame(context.Background(), query, outbound); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(store.result, ErrStreamCallbackActive) {
		t.Fatalf("same-Session StateStore reentry returned %v", store.result)
	}
	if frame := nextTestFrame(t, outbound); frame.Type != FrameStateSnapshot {
		t.Fatalf("outer query failed after rejected reentry: %#v", frame)
	}
}
