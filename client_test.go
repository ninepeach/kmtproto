package kmtproto

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func readyClient(t *testing.T) (*Client, *FakeClock, ConnectionGeneration) {
	t.Helper()
	clock := NewFakeClock(time.Unix(100, 0))
	config := DefaultClientConfig()
	config.Clock = clock
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
	welcome := Envelope{V: WireVersionV1, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew, ServerTime: clock.Now().UnixMilli()})}
	if _, err := client.HandleIncoming(gen, welcome); err != nil {
		t.Fatal(err)
	}
	return client, clock, gen
}

func TestHandshakeAndStaleGeneration(t *testing.T) {
	client, _, oldGen := readyClient(t)
	newGen := client.BeginConnect()
	if newGen <= oldGen {
		t.Fatal("generation did not increase")
	}
	staleFrames := []Envelope{
		{V: WireVersionV1, Type: FrameEvent, ID: "evt_stale", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})},
		{V: WireVersionV1, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{PingID: "p_stale"})},
		{V: WireVersionV1, Type: FrameError, SessionID: "s_1", Payload: mustPayload(ErrorPayload{Code: ErrorInternal, Retryable: true})},
	}
	for _, stale := range staleFrames {
		if _, err := client.HandleIncoming(oldGen, stale); !errors.Is(err, ErrStaleConnection) {
			t.Fatalf("%s: got %v, want ErrStaleConnection", stale.Type, err)
		}
	}
	if client.LastSeq() != 0 || client.State() != StateConnecting {
		t.Fatal("stale generation mutated client")
	}
}

func TestHeartbeatTimeoutDisconnectsDeterministically(t *testing.T) {
	client, clock, gen := readyClient(t)
	if _, err := client.SendPing(gen, "p_timeout"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Second)
	if actions, err := client.CheckHeartbeat(gen); err != nil || len(actions) != 0 || client.State() != StateSuspect {
		t.Fatalf("suspect transition failed: actions=%d state=%s err=%v", len(actions), client.State(), err)
	}
	clock.Advance(10 * time.Second)
	actions, err := client.CheckHeartbeat(gen)
	if err != nil || len(actions) != 1 || client.State() != StateDisconnected {
		t.Fatalf("disconnect transition failed: actions=%d state=%s err=%v", len(actions), client.State(), err)
	}
}

func TestSyncRequiredStopsAutomaticResume(t *testing.T) {
	client, _, gen := readyClient(t)
	gap := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_2", SessionID: "s_1", Seq: 2, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, gap); err != nil {
		t.Fatal(err)
	}
	frame := Envelope{V: WireVersionV1, Type: FrameError, SessionID: "s_1", Payload: mustPayload(ErrorPayload{Code: ErrorSyncRequired, Retryable: false})}
	actions, err := client.HandleIncoming(gen, frame)
	if err != nil || client.State() != StateDisconnected || len(actions) != 2 {
		t.Fatalf("SYNC_REQUIRED behavior failed: actions=%d state=%s err=%v", len(actions), client.State(), err)
	}
	if _, ok := actions[1].(FullSyncRequiredAction); !ok {
		t.Fatalf("missing FullSyncRequiredAction: %#v", actions)
	}
}

func TestInvalidErrorClosesWithoutErrorLoop(t *testing.T) {
	client, _, gen := readyClient(t)
	invalid := Envelope{V: WireVersionV1, Type: FrameError, SessionID: "s_1", Payload: json.RawMessage(`{"code":"NOT_A_V01_CODE","retryable":false}`)}
	actions, err := client.HandleIncoming(gen, invalid)
	if err != nil || len(actions) != 1 || client.State() != StateDisconnected {
		t.Fatalf("invalid ERROR handling failed: actions=%d state=%s err=%v", len(actions), client.State(), err)
	}
	if _, sendsError := actions[0].(SendFrameAction); sendsError {
		t.Fatal("client created ERROR-about-ERROR")
	}
}

func TestConnectionStateStringUnknownIsSafe(t *testing.T) {
	if got := ConnectionState(255).String(); got != "UNKNOWN" {
		t.Fatalf("got %q", got)
	}
}

func TestHeartbeatLatePongCannotReviveReplacedConnection(t *testing.T) {
	client, clock, gen := readyClient(t)
	if _, err := client.SendPing(gen, "p_1"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(21 * time.Second)
	if _, err := client.CheckHeartbeat(gen); err != nil {
		t.Fatal(err)
	}
	if client.State() != StateSuspect {
		t.Fatal("expected SUSPECT")
	}
	newGen := client.BeginConnect()
	pong := Envelope{V: WireVersionV1, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{PingID: "p_1"})}
	if _, err := client.HandleIncoming(gen, pong); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("got %v, want stale generation", err)
	}
	if client.Generation() != newGen || client.State() != StateConnecting {
		t.Fatal("late PONG revived replaced connection")
	}
}

func TestEventDuplicateConflictAndGapResume(t *testing.T) {
	client, _, gen := readyClient(t)
	event1 := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{"n":1}`)})}
	actions, err := client.HandleIncoming(gen, event1)
	if err != nil || len(actions) != 1 || client.LastSeq() != 1 {
		t.Fatalf("normal event failed: actions=%d err=%v seq=%d", len(actions), err, client.LastSeq())
	}
	if actions, err = client.HandleIncoming(gen, event1); err != nil || len(actions) != 0 {
		t.Fatalf("safe duplicate failed: actions=%d err=%v", len(actions), err)
	}
	conflict := copyEnvelope(event1)
	conflict.ID = "evt_other"
	if _, err := client.HandleIncoming(gen, conflict); !errors.Is(err, ErrProtocolConflict) {
		t.Fatalf("got %v, want identity conflict", err)
	}
	gap := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_3", SessionID: "s_1", Seq: 3, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{"n":3}`)})}
	actions, err = client.HandleIncoming(gen, gap)
	if err != nil || len(actions) != 1 || client.State() != StateResuming || client.LastSeq() != 1 {
		t.Fatalf("gap did not trigger resume: actions=%d state=%s seq=%d err=%v", len(actions), client.State(), client.LastSeq(), err)
	}
}

func TestReplayIsBufferedUntilBoundary(t *testing.T) {
	client, _, gen := readyClient(t)
	first := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, first); err != nil {
		t.Fatal(err)
	}
	gap := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_4", SessionID: "s_1", Seq: 4, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	if _, err := client.HandleIncoming(gen, gap); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{V: WireVersionV1, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ResumeFrom: 2, ReplayTo: 4})}
	if actions, err := client.HandleIncoming(gen, welcome); err != nil || len(actions) != 0 {
		t.Fatalf("resume welcome: actions=%d err=%v", len(actions), err)
	}
	for seq := uint64(2); seq <= 3; seq++ {
		event := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_" + string(rune('0'+seq)), SessionID: "s_1", Seq: seq, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
		actions, err := client.HandleIncoming(gen, event)
		if err != nil || len(actions) != 0 || client.LastSeq() != 1 {
			t.Fatalf("partial replay leaked: seq=%d actions=%d last=%d err=%v", seq, len(actions), client.LastSeq(), err)
		}
	}
	last := Envelope{V: WireVersionV1, Type: FrameEvent, ID: "evt_4", SessionID: "s_1", Seq: 4, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}
	actions, err := client.HandleIncoming(gen, last)
	if err != nil || len(actions) != 4 || client.LastSeq() != 4 || client.State() != StateReady {
		t.Fatalf("replay boundary failed: actions=%d last=%d state=%s err=%v", len(actions), client.LastSeq(), client.State(), err)
	}
}
