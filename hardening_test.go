package kmtproto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestProcessingDedupClaimDoesNotExpire(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	store := NewMemoryDedupStore(clock, time.Minute)
	claimed, _, err := store.Claim("s", "m", SendFingerprint{})
	if err != nil || !claimed {
		t.Fatalf("first claim: claimed=%v err=%v", claimed, err)
	}
	clock.Advance(2 * time.Minute)
	claimed, record, err := store.Claim("s", "m", SendFingerprint{})
	if err != nil || claimed || record == nil || record.State != DedupProcessing {
		t.Fatalf("active claim expired: claimed=%v record=%#v err=%v", claimed, record, err)
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
	server, err := NewServer(config, NewMemorySessionRepository(), store, replay, replay, app)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_flight", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	firstDone := make(chan error, 1)
	go func() { firstDone <- server.HandleIncoming(context.Background(), send, NewOutboundQueue()) }()
	<-store.started

	duplicateOut := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), send, duplicateOut); err != nil {
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
			server, err := NewServer(config, NewMemorySessionRepository(), store, replay, replay, &recordingApp{})
			if err != nil {
				t.Fatal(err)
			}
			createTestSession(t, server)
			out := NewOutboundQueue()
			send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "msg_complete", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
			done := make(chan error, 1)
			go func() { done <- server.HandleIncoming(context.Background(), send, out) }()
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

func TestReplayHighWaterSurvivesFullPrune(t *testing.T) {
	store := NewMemoryReplayStore()
	first, err := store.Append("s", "e1", "test", []byte(`{}`), 0)
	if err != nil {
		t.Fatal(err)
	}
	store.PruneBefore("s", first.Seq+1)
	current, err := store.CurrentSeq("s")
	if err != nil || current != 1 {
		t.Fatalf("high-water decreased: current=%d err=%v", current, err)
	}
	second, err := store.Append("s", "e2", "test", []byte(`{}`), 0)
	if err != nil || second.Seq != 2 {
		t.Fatalf("sequence reused after prune: event=%#v err=%v", second, err)
	}
}

func TestResumedWelcomeCarriesExplicitBounds(t *testing.T) {
	payload := mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: 0})
	if !bytes.Contains(payload, []byte(`"resume_from":1`)) || !bytes.Contains(payload, []byte(`"replay_to":0`)) {
		t.Fatalf("missing explicit resume bounds: %s", payload)
	}
	valid := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s", Payload: payload}
	if err := ValidateFrame(&valid, DefaultLimits(), true); err != nil {
		t.Fatal(err)
	}
	missing := copyEnvelope(valid)
	missing.Payload = json.RawMessage(`{"mode":"RESUMED","server_time":0,"resume_from":1}`)
	if err := ValidateFrame(&missing, DefaultLimits(), true); err == nil {
		t.Fatal("RESUMED WELCOME without replay_to was accepted")
	}
	newWithBounds := copyEnvelope(valid)
	newWithBounds.Payload = json.RawMessage(`{"mode":"NEW","server_time":0,"resume_from":0,"replay_to":0}`)
	if err := ValidateFrame(&newWithBounds, DefaultLimits(), true); err == nil {
		t.Fatal("NEW WELCOME with replay bounds was accepted")
	}
}

func configuredReadyClient(t *testing.T, mutate func(*ClientConfig)) (*Client, *FakeClock, ConnectionGeneration) {
	t.Helper()
	clock := NewFakeClock(time.Unix(4, 0))
	config := DefaultClientConfig()
	config.Clock = clock
	mutate(&config)
	client, err := NewClient(config)
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

func beginGapReplay(t *testing.T, client *Client, gen ConnectionGeneration, replayTo uint64) {
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

func TestClientReplayLimitsDoNotAdvanceSequence(t *testing.T) {
	t.Run("event count", func(t *testing.T) {
		client, _, gen := configuredReadyClient(t, func(config *ClientConfig) { config.MaxReplayEvents = 2 })
		gap := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "gap", SessionID: "s_1", Seq: 5, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
		if _, err := client.HandleIncoming(gen, gap); err != nil {
			t.Fatal(err)
		}
		welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: 4})}
		actions, err := client.HandleIncoming(gen, welcome)
		if err != nil || len(actions) != 3 || client.State() != StateDisconnected || client.LastSeq() != 0 {
			t.Fatalf("count bound: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
		}
	})

	t.Run("retained bytes", func(t *testing.T) {
		client, _, gen := configuredReadyClient(t, func(config *ClientConfig) {
			config.MaxReplayEvents = 10
			config.MaxReplayBytes = 1
		})
		beginGapReplay(t, client, gen, 2)
		event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
		actions, err := client.HandleIncoming(gen, event)
		if err != nil || len(actions) != 3 || client.State() != StateDisconnected || client.LastSeq() != 0 {
			t.Fatalf("byte bound: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
		}
	})
}

func TestEventIdentityWindowIsBoundedAndConservative(t *testing.T) {
	client, _, gen := configuredReadyClient(t, func(config *ClientConfig) { config.EventIdentityWindow = 2 })
	for seq := uint64(1); seq <= 3; seq++ {
		event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: string(rune('a' + seq)), SessionID: "s_1", Seq: seq, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
		if _, err := client.HandleIncoming(gen, event); err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	retained := len(client.eventIDs)
	client.mu.Unlock()
	if retained != 2 {
		t.Fatalf("retained %d identities, want 2", retained)
	}
	tooOld := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "b", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, tooOld); !errors.Is(err, ErrIdentityExpired) {
		t.Fatalf("old unverifiable duplicate: %v", err)
	}
	safe := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "c", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if actions, err := client.HandleIncoming(gen, safe); err != nil || len(actions) != 0 {
		t.Fatalf("recent safe duplicate: actions=%d err=%v", len(actions), err)
	}
	conflict := copyEnvelope(safe)
	conflict.ID = "different"
	if _, err := client.HandleIncoming(gen, conflict); !errors.Is(err, ErrProtocolConflict) {
		t.Fatalf("recent identity conflict: %v", err)
	}
}

func TestPartialReplayIsDiscardedAcrossReconnect(t *testing.T) {
	client, _, gen := readyClient(t)
	first := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, first); err != nil {
		t.Fatal(err)
	}
	gap := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e5", SessionID: "s_1", Seq: 5, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, gap); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 2, ReplayTo: 4})}
	if _, err := client.HandleIncoming(gen, welcome); err != nil {
		t.Fatal(err)
	}
	partial := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e2", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if actions, err := client.HandleIncoming(gen, partial); err != nil || len(actions) != 0 || client.LastSeq() != 1 {
		t.Fatalf("partial replay leaked: actions=%d seq=%d err=%v", len(actions), client.LastSeq(), err)
	}
	if err := client.Disconnect(gen); err != nil {
		t.Fatal(err)
	}
	newGen := client.BeginConnect()
	if err := client.TransportConnected(newGen); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resume(newGen); err != nil {
		t.Fatal(err)
	}
	if _, err := client.HandleIncoming(newGen, welcome); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(2); seq <= 4; seq++ {
		event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: string(rune('c' + seq)), SessionID: "s_1", Seq: seq, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
		actions, err := client.HandleIncoming(newGen, event)
		if err != nil {
			t.Fatal(err)
		}
		if seq < 4 && len(actions) != 0 {
			t.Fatalf("partial replay delivered at seq %d", seq)
		}
		if seq == 4 && len(actions) != 4 {
			t.Fatalf("final replay actions=%d, want 4", len(actions))
		}
	}
	if client.LastSeq() != 4 || client.State() != StateReady {
		t.Fatalf("reconnect replay state=%s seq=%d", client.State(), client.LastSeq())
	}
}

func TestInvalidClientStateTransitionsAndOldWelcome(t *testing.T) {
	client, _, oldGen := readyClient(t)
	newGen := client.BeginConnect()
	oldWelcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 1, ReplayTo: 0})}
	if _, err := client.HandleIncoming(oldGen, oldWelcome); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("old WELCOME: %v", err)
	}
	if err := client.TransportConnected(newGen); err != nil {
		t.Fatal(err)
	}
	pong := Envelope{V: WireVersionV2, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{PingID: "late"})}
	if _, err := client.HandleIncoming(newGen, pong); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("PONG in CONNECTED: %v", err)
	}
	if client.State() != StateConnected || client.LastSeq() != 0 {
		t.Fatal("invalid transition mutated state")
	}
	if _, err := client.Resume(newGen); err != nil {
		t.Fatal(err)
	}
	unexpectedNew := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})}
	if _, err := client.HandleIncoming(newGen, unexpectedNew); err == nil {
		t.Fatal("NEW WELCOME accepted during RESUMING")
	}
}

func TestServerConnectionAdmissionState(t *testing.T) {
	app := &recordingApp{}
	server, _, _ := newTestServer(t, app)
	connection := NewServerConnection()
	gen, out := connection.Replace()
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := connection.Handle(context.Background(), server, gen, send); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameError || connection.State() != ServerConnectionClosed || app.count() != 0 {
		t.Fatalf("pre-handshake SEND was not rejected: frame=%#v state=%s calls=%d", frame, connection.State(), app.count())
	}

	gen, out = connection.Replace()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := connection.Handle(context.Background(), server, gen, hello); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, out)
	if welcome.Type != FrameWelcome || connection.State() != ServerConnectionReady || connection.SessionID() != "s_1" {
		t.Fatalf("handshake state: frame=%#v state=%s session=%q", welcome, connection.State(), connection.SessionID())
	}
	if err := connection.Handle(context.Background(), server, gen, hello); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameError || connection.State() != ServerConnectionClosed {
		t.Fatalf("second HELLO accepted: frame=%#v state=%s", frame, connection.State())
	}

	gen, out = connection.Replace()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := connection.Handle(context.Background(), server, gen, resume); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameWelcome || connection.State() != ServerConnectionReady {
		t.Fatalf("resume state: frame=%#v state=%s", frame, connection.State())
	}
}

func TestServerReplayEventLimitReturnsSyncRequired(t *testing.T) {
	clock := NewFakeClock(time.Unix(5, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	config.MaxReplayEvents = 2
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, &recordingApp{})
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
	if err := server.HandleIncoming(context.Background(), resume, out); err != nil {
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
	if err := server.HandleIncoming(context.Background(), resume, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorSyncRequired {
		t.Fatalf("expected byte-bounded SYNC_REQUIRED, got %#v %#v", frame, payload)
	}
}

func TestGapResumeAcceptedOnReadyServerConnection(t *testing.T) {
	server, clock, _ := newTestServer(t, &recordingApp{})
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clock
	client, err := NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientGeneration := client.BeginConnect()
	if err := client.TransportConnected(clientGeneration); err != nil {
		t.Fatal(err)
	}
	helloActions, err := client.StartSession(clientGeneration, "test")
	if err != nil {
		t.Fatal(err)
	}

	connection := NewServerConnection()
	serverGeneration, outbound := connection.Replace()
	if err := connection.Handle(context.Background(), server, serverGeneration, helloActions[0].(SendFrameAction).Frame); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, outbound)
	if _, err := client.HandleIncoming(clientGeneration, welcome); err != nil {
		t.Fatal(err)
	}

	live := NewOutboundQueue()
	for _, id := range []string{"event_1", "event_2"} {
		if err := server.PublishEvent("s_1", id, "test", json.RawMessage(`{}`), live); err != nil {
			t.Fatal(err)
		}
	}
	_ = nextTestFrame(t, live)
	gap := nextTestFrame(t, live)
	resumeActions, err := client.HandleIncoming(clientGeneration, gap)
	if err != nil || len(resumeActions) != 1 || client.State() != StateResuming || client.LastSeq() != 0 {
		t.Fatalf("gap did not produce RESUME: actions=%d state=%s seq=%d err=%v", len(resumeActions), client.State(), client.LastSeq(), err)
	}
	resume := resumeActions[0].(SendFrameAction).Frame
	if err := connection.Handle(context.Background(), server, serverGeneration, resume); err != nil {
		t.Fatal(err)
	}
	if connection.State() != ServerConnectionReady {
		t.Fatalf("server connection did not return to READY: %s", connection.State())
	}

	frames := []Envelope{nextTestFrame(t, outbound), nextTestFrame(t, outbound), nextTestFrame(t, outbound)}
	for index, frame := range frames {
		actions, handleErr := client.HandleIncoming(clientGeneration, frame)
		if handleErr != nil {
			t.Fatalf("resume frame %d failed: %v", index, handleErr)
		}
		if index < 2 && len(actions) != 0 {
			t.Fatalf("partial replay leaked actions at frame %d: %#v", index, actions)
		}
		if index == 2 && (len(actions) != 3 || client.State() != StateReady || client.LastSeq() != 2) {
			t.Fatalf("gap recovery incomplete: actions=%d state=%s seq=%d", len(actions), client.State(), client.LastSeq())
		}
	}
}

func TestStreamLaneRecoversFromPanic(t *testing.T) {
	server := &Server{lanes: make(map[string]*streamLane)}
	if err := server.runStream("s", func(*streamLane) error { panic("boom") }); err == nil {
		t.Fatal("expected recovered panic error")
	}
	if err := server.runStream("s", func(*streamLane) error { return nil }); err != nil {
		t.Fatalf("lane remained unusable after panic: %v", err)
	}
}

type reentrantEventAppender struct {
	server   *Server
	delegate EventAppender
	outbound *OutboundQueue
	result   error
}

func (a *reentrantEventAppender) Append(sessionID, eventID, eventType string, content []byte, timestamp int64) (Envelope, error) {
	a.result = a.server.PublishEvent(sessionID, "nested_event", eventType, json.RawMessage(`{}`), a.outbound)
	return a.delegate.Append(sessionID, eventID, eventType, content, timestamp)
}

func TestEventAppenderCallbackReentryFailsWithoutDeadlock(t *testing.T) {
	clock := NewFakeClock(time.Unix(6, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	nestedOutbound := NewOutboundQueue()
	appender := &reentrantEventAppender{delegate: replay, outbound: nestedOutbound}
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, appender, &recordingApp{})
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

func TestOutboundValidationPrecedesClientMutation(t *testing.T) {
	client, _, _ := configuredReadyClient(t, func(config *ClientConfig) {
		config.Limits.MaxPayloadSize = 32
	})
	if _, err := client.EnqueueSend("m", json.RawMessage(`"1234567890123456789012345678"`)); err == nil {
		t.Fatal("expected encoded SEND payload limit error")
	}
	if actions, err := client.RetryPending(); err != nil || len(actions) != 0 {
		t.Fatalf("invalid SEND entered outbox: actions=%d err=%v", len(actions), err)
	}
	pingClient, _, pingGen := configuredReadyClient(t, func(*ClientConfig) {})
	if _, err := pingClient.SendPing(pingGen, string(bytes.Repeat([]byte{'p'}, DefaultLimits().MaxIDLength+1))); err == nil {
		t.Fatal("expected oversized ping ID error")
	}
	if _, err := pingClient.SendPing(pingGen, "valid"); err != nil {
		t.Fatalf("invalid PING mutated heartbeat state: %v", err)
	}
}

func TestErrorBehaviorAndRetryabilityValidation(t *testing.T) {
	cases := []struct {
		code      string
		retryable bool
		close     bool
		abandon   bool
		fullSync  bool
	}{
		{ErrorBadRequest, false, false, false, false},
		{ErrorUnsupportedVersion, false, true, false, false},
		{ErrorUnauthorized, false, true, false, false},
		{ErrorInvalidSession, false, false, true, false},
		{ErrorNotFound, false, false, false, false},
		{ErrorRateLimited, true, false, false, false},
		{ErrorSyncRequired, false, false, false, true},
		{ErrorProtocolViolation, false, true, false, false},
	}
	for _, tc := range cases {
		behavior, ok := BehaviorForErrorCode(tc.code)
		if !ok || behavior.Retryable != tc.retryable || behavior.CloseConnection != tc.close || behavior.AbandonSession != tc.abandon || behavior.FullSyncRequired != tc.fullSync {
			t.Fatalf("%s behavior=%#v ok=%v", tc.code, behavior, ok)
		}
		frame := Envelope{V: WireVersionV2, Type: FrameError, Payload: mustPayload(ErrorPayload{Code: tc.code, Retryable: tc.retryable})}
		if err := ValidateFrame(&frame, DefaultLimits(), true); err != nil {
			t.Fatalf("%s valid ERROR rejected: %v", tc.code, err)
		}
		invalid := copyEnvelope(frame)
		invalid.Payload = mustPayload(ErrorPayload{Code: tc.code, Retryable: !tc.retryable})
		if err := ValidateFrame(&invalid, DefaultLimits(), true); err == nil {
			t.Fatalf("%s conflicting retryability accepted", tc.code)
		}
	}
	internal := Envelope{V: WireVersionV2, Type: FrameError, Payload: mustPayload(ErrorPayload{Code: ErrorInternal, Retryable: true})}
	if err := ValidateFrame(&internal, DefaultLimits(), true); err != nil {
		t.Fatalf("retryable INTERNAL rejected: %v", err)
	}
}

func TestV02BaseFrameValidationMatrix(t *testing.T) {
	valid := []Envelope{
		{V: WireVersionV2, Type: FrameHello, ID: "optional", Payload: mustPayload(HelloPayload{})},
		{V: WireVersionV2, Type: FrameWelcome, SessionID: "s", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})},
		{V: WireVersionV2, Type: FramePing, SessionID: "s", Payload: mustPayload(PingPayload{PingID: "p"})},
		{V: WireVersionV2, Type: FramePong, SessionID: "s", Payload: mustPayload(PongPayload{PingID: "p"})},
		{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})},
		{V: WireVersionV2, Type: FrameAck, SessionID: "s", Payload: mustPayload(AckPayload{RefID: "m"})},
		{V: WireVersionV2, Type: FrameEvent, ID: "e", SessionID: "s", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})},
		{V: WireVersionV2, Type: FrameResume, SessionID: "s", Payload: mustPayload(ResumePayload{LastSeq: 0})},
		{V: WireVersionV2, Type: FrameError, Payload: mustPayload(ErrorPayload{Code: ErrorBadRequest, Retryable: false})},
	}
	for _, frame := range valid {
		if err := ValidateFrame(&frame, DefaultLimits(), true); err != nil {
			t.Fatalf("valid %s rejected: %v", frame.Type, err)
		}
	}

	for _, frame := range valid {
		if frame.Type == FrameEvent {
			frame.Seq = 0
		} else {
			frame.Seq = 1
		}
		if err := ValidateFrame(&frame, DefaultLimits(), true); err == nil {
			t.Fatalf("invalid %s sequence accepted", frame.Type)
		}
	}

	for _, index := range []int{1, 2, 3, 5, 7, 8} {
		frame := valid[index]
		frame.ID = "forbidden"
		if err := ValidateFrame(&frame, DefaultLimits(), true); err == nil {
			t.Fatalf("%s envelope id accepted", frame.Type)
		}
	}
	for _, index := range []int{1, 2, 3, 4, 5, 6, 7} {
		frame := valid[index]
		frame.SessionID = ""
		if err := ValidateFrame(&frame, DefaultLimits(), true); err == nil {
			t.Fatalf("%s without session accepted", frame.Type)
		}
	}
	unknown := Envelope{V: WireVersionV2, Type: FrameType("FUTURE"), Payload: json.RawMessage(`{}`)}
	if err := ValidateFrame(&unknown, DefaultLimits(), true); err == nil {
		t.Fatal("unknown frame type accepted")
	}
}

func TestOnlyEventChangesLastSequence(t *testing.T) {
	client, _, gen := readyClient(t)
	if _, err := client.SendPing(gen, "p"); err != nil {
		t.Fatal(err)
	}
	pong := Envelope{V: WireVersionV2, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{PingID: "p"})}
	if _, err := client.HandleIncoming(gen, pong); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnqueueSend("m", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s_1", Payload: mustPayload(AckPayload{RefID: "m"})}
	if _, err := client.HandleIncoming(gen, ack); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("non-EVENT changed last_seq to %d", client.LastSeq())
	}
	event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: "e", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, event); err != nil {
		t.Fatal(err)
	}
	if client.LastSeq() != 1 {
		t.Fatalf("EVENT did not advance last_seq: %d", client.LastSeq())
	}
}

func TestServerConnectionReplacementFencesLateHandshake(t *testing.T) {
	clock := NewFakeClock(time.Unix(6, 0))
	started := make(chan struct{})
	release := make(chan struct{})
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) {
		close(started)
		<-release
		return "s_old", nil
	}
	replay := NewMemoryReplayStore()
	server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, &recordingApp{})
	if err != nil {
		t.Fatal(err)
	}
	connection := NewServerConnection()
	oldGeneration, _ := connection.Replace()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	done := make(chan error, 1)
	go func() { done <- connection.Handle(context.Background(), server, oldGeneration, hello) }()
	<-started
	newGeneration, _ := connection.Replace()
	close(release)
	_ = <-done
	if connection.State() != ServerConnectionAwaitingHandshake || connection.SessionID() != "" || connection.Generation() != newGeneration {
		t.Fatalf("late handshake mutated replacement: state=%s session=%q", connection.State(), connection.SessionID())
	}
}

func TestJSONCodecFailureClassesAreDeterministic(t *testing.T) {
	codec := NewJSONCodec()
	cases := map[string][]byte{
		"malformed":           []byte(`{"v":`),
		"unknown type":        []byte(`{"v":2,"type":"FUTURE","payload":{}}`),
		"unsupported version": []byte(`{"v":1,"type":"HELLO","payload":{}}`),
		"missing payload":     []byte(`{"v":2,"type":"HELLO"}`),
		"trailing value":      []byte(`{"v":2,"type":"HELLO","payload":{}} {}`),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := codec.Decode(data); err == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}

	limits := DefaultLimits()
	limits.MaxIDLength = 4
	limits.MaxPayloadSize = 16
	limited := &JSONCodec{Limits: limits, Strict: true}
	oversizedID := []byte(`{"v":2,"type":"SEND","id":"12345","session_id":"s","payload":{"content":{}}}`)
	if _, err := limited.Decode(oversizedID); err == nil {
		t.Fatal("oversized ID accepted")
	}
	oversizedPayload := []byte(`{"v":2,"type":"SEND","id":"m","session_id":"s","payload":{"content":"1234567890"}}`)
	if _, err := limited.Decode(oversizedPayload); err == nil {
		t.Fatal("oversized payload accepted")
	}
}

func TestMemoryReplayStoreRejectsInvalidContentWithoutPanic(t *testing.T) {
	store := NewMemoryReplayStore()
	if _, err := store.Append("s", "e", "test", []byte(`not-json`), 0); err == nil {
		t.Fatal("invalid replay content accepted")
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
			server, err := NewServer(config, NewMemorySessionRepository(), NewMemoryDedupStore(clock, config.DedupTTL), replay, replay, app)
			if err != nil {
				t.Fatal(err)
			}
			createTestSession(t, server)
			send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
			if err := server.HandleIncoming(context.Background(), send, NewOutboundQueue()); err == nil {
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
			if err := server.HandleIncoming(context.Background(), send, out); err != nil {
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
	if err := server.HandleIncoming(context.Background(), send, lost); !errors.Is(err, ErrOutboundClosed) {
		t.Fatalf("got %v, want outbound failure", err)
	}
	out := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), send, out); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameAck {
		t.Fatalf("stored ACK not replayed: %#v", frame)
	}
	if app.count() != 1 {
		t.Fatalf("application repeated after lost ACK: %d", app.count())
	}
}
