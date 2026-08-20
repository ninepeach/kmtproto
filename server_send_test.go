package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSameSendApplicationReentryFailsFast(t *testing.T) {
	app := &reentrantSendApp{outbound: NewOutboundQueue()}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	app.server = server
	app.frame = Envelope{V: WireVersionV2, Type: FrameSend, ID: "reentrant", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	outerOut := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), app.frame, outerOut); err != nil {
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

func TestSameSendStoreReentryFailsFast(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_050, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	store := &reentrantClaimStore{delegate: NewMemoryDedupStore(clock, config.DedupTTL), outbound: NewOutboundQueue()}
	app := &recordingApp{}
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       store,
		Replay:      replay,
		Appender:    replay,
		Application: app,
	})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	store.server = server
	store.frame = Envelope{V: WireVersionV2, Type: FrameSend, ID: "claim-reentrant", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	outerOut := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), store.frame, outerOut); err != nil {
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
	if err := server.ProcessFrame(context.Background(), first, firstOut); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, firstOut); frame.Type != FrameAck {
		t.Fatalf("first SEND response = %#v", frame)
	}
	conflict := copyEnvelope(first)
	conflict.Payload = mustPayload(SendPayload{Content: json.RawMessage(`{"value":2}`)})
	conflictOut := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), conflict, conflictOut); err != nil {
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
	go func() { firstDone <- server.ProcessFrame(context.Background(), first, firstOut) }()
	<-app.started
	conflict := copyEnvelope(first)
	conflict.Payload = mustPayload(SendPayload{Content: json.RawMessage(`{"value":2}`)})
	conflictOut := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), conflict, conflictOut); err != nil {
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

func TestApplicationErrorLeavesSendIndeterminate(t *testing.T) {
	app := &failingSendApp{}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "indeterminate", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	for i := 0; i < 2; i++ {
		out := NewOutboundQueue()
		if err := server.ProcessFrame(context.Background(), send, out); err != nil {
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

func TestDedupACKMustMatchSend(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_150, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       mismatchedACKStore{},
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := server.ProcessFrame(context.Background(), send, NewOutboundQueue()); err == nil {
		t.Fatal("dedup ACK for another Session was accepted")
	}
}

func TestDuplicateBindsToFlightBeforeClaim(t *testing.T) {
	clock := NewFakeClock(time.Unix(2, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	store := &blockingFirstClaimStore{
		delegate: NewMemoryDedupStore(clock, config.DedupTTL),
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	replay := NewMemoryReplayStore()
	app := &recordingApp{}
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       store,
		Replay:      replay,
		Appender:    replay,
		Application: app,
	})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_flight", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	firstDone := make(chan error, 1)
	go func() { firstDone <- server.ProcessFrame(context.Background(), send, NewOutboundQueue()) }()
	<-store.started

	duplicateOut := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), send, duplicateOut); err != nil {
		t.Fatalf("duplicate did not fail fast while Claim callback was active: %v", err)
	}
	duplicateError := nextTestFrame(t, duplicateOut)
	var duplicatePayload ErrorPayload
	if duplicateError.Type != FrameError || decodePayload(duplicateError.Payload, &duplicatePayload, true) != nil ||
		duplicatePayload.Code != ErrorInternal || !duplicatePayload.Retryable {
		t.Fatalf("duplicate callback response = %#v payload=%#v", duplicateError, duplicatePayload)
	}
	close(store.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if app.count() != 1 {
		t.Fatalf("application called %d times", app.count())
	}
}

func TestACKCannotPrecedeComplete(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			clock := NewFakeClock(time.Unix(3, 0))
			config := DefaultServerConfig()
			config.Clock = clock
			config.NewSessionID = func() (string, error) { return "s_1", nil }
			store := &blockingCompleteStore{
				delegate: NewMemoryDedupStore(clock, config.DedupTTL),
				entered:  make(chan struct{}),
				release:  make(chan struct{}),
				fail:     fail,
			}
			replay := NewMemoryReplayStore()
			server, err := NewServerProtocol(config, ServerDependencies{
				Sessions:    NewMemorySessionRepository(),
				Dedup:       store,
				Replay:      replay,
				Appender:    replay,
				Application: &recordingApp{},
			})
			if err != nil {
				t.Fatal(err)
			}
			createTestSession(t, server)
			out := NewOutboundQueue()
			send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_complete", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
			done := make(chan error, 1)
			go func() { done <- server.ProcessFrame(context.Background(), send, out) }()
			<-store.entered
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := out.Next(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("ACK visible before Complete returned: %v", err)
			}
			close(store.release)
			handleErr := <-done
			if fail {
				if handleErr == nil {
					t.Fatal("expected Complete failure")
				}
				if _, err := out.Next(cancelled); !errors.Is(err, context.Canceled) {
					t.Fatalf("ACK emitted after failed Complete: %v", err)
				}
				return
			}
			if handleErr != nil {
				t.Fatal(handleErr)
			}
			if frame := nextTestFrame(t, out); frame.Type != FrameAck {
				t.Fatalf("expected ACK after Complete, got %#v", frame)
			}
		})
	}
}

func TestPreCompleteFailureWindowsNeverRepeatApplication(t *testing.T) {
	for _, point := range []string{FailAfterClaim, FailAfterApplication} {
		t.Run(point, func(t *testing.T) {
			clock := NewFakeClock(time.Unix(7, 0))
			config := DefaultServerConfig()
			config.Clock = clock
			config.NewSessionID = func() (string, error) { return "s_1", nil }
			config.FailureInjector = failAt(point)
			replay := NewMemoryReplayStore()
			app := &recordingApp{}
			server, err := NewServerProtocol(config, ServerDependencies{
				Sessions:    NewMemorySessionRepository(),
				Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
				Replay:      replay,
				Appender:    replay,
				Application: app,
			})
			if err != nil {
				t.Fatal(err)
			}
			createTestSession(t, server)
			send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
			if err := server.ProcessFrame(context.Background(), send, NewOutboundQueue()); err == nil {
				t.Fatal("expected injected failure")
			}
			wantCalls := 0
			if point == FailAfterApplication {
				wantCalls = 1
			}
			if app.count() != wantCalls {
				t.Fatalf("application calls=%d, want %d", app.count(), wantCalls)
			}
			server.config.FailureInjector = nil
			out := NewOutboundQueue()
			if err := server.ProcessFrame(context.Background(), send, out); err != nil {
				t.Fatal(err)
			}
			frame := nextTestFrame(t, out)
			var payload ErrorPayload
			if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorInternal || !payload.Retryable {
				t.Fatalf("retry did not expose PROCESSING state: %#v %#v", frame, payload)
			}
			if app.count() != wantCalls {
				t.Fatalf("retry repeated application: calls=%d want=%d", app.count(), wantCalls)
			}
		})
	}
}

func TestCompleteThenOutboundFailureReplaysStoredACK(t *testing.T) {
	app := &recordingApp{}
	server, _, _ := newTestServer(t, app)
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m_lost", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	lost := NewOutboundQueue()
	lost.Close()
	if err := server.ProcessFrame(context.Background(), send, lost); !errors.Is(err, ErrOutboundClosed) {
		t.Fatalf("got %v, want outbound failure", err)
	}
	out := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), send, out); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameAck {
		t.Fatalf("stored ACK not replayed: %#v", frame)
	}
	if app.count() != 1 {
		t.Fatalf("application repeated after lost ACK: %d", app.count())
	}
}
