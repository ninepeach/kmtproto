package kmtproto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

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
		t.Fatal("Session-scoped ClientProtocol data survived INVALID_SESSION")
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

func TestGapResumeAcceptedOnReadyServerAdmission(t *testing.T) {
	server, clock, _ := newTestServer(t, &recordingApp{})
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clock
	client, err := NewClientProtocol(clientConfig)
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

	connection := NewServerAdmission()
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
	if connection.State() != ServerAdmissionReady {
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
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    sessions,
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      &injectedReplayFailure{current: 2, err: errors.New("injected replay failure")},
		Appender:    appender,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	outbound := NewOutboundQueue()
	if err := server.ProcessFrame(context.Background(), resume, outbound); err != nil {
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
