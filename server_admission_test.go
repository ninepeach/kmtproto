package kmtproto

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestInvalidSessionAbandonsServerAdmission(t *testing.T) {
	for _, frameType := range []FrameType{FramePing, FrameSend} {
		t.Run(string(frameType), func(t *testing.T) {
			server, clock, _ := newTestServer(t, &recordingApp{})
			connection := NewServerAdmission()
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
			if connection.State() != ServerAdmissionAwaitingHandshake || connection.SessionID() != "" {
				t.Fatalf("server connection retained invalid Session: state=%s session=%q", connection.State(), connection.SessionID())
			}
		})
	}
}

func TestServerAdmissionState(t *testing.T) {
	app := &recordingApp{}
	server, _, _ := newTestServer(t, app)
	connection := NewServerAdmission()
	gen, out := connection.Replace()
	send := Envelope{V: WireVersionV2, Type: FrameSend, ID: "m", SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}
	if err := connection.Handle(context.Background(), server, gen, send); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameError || connection.State() != ServerAdmissionClosed || app.count() != 0 {
		t.Fatalf("pre-handshake SEND was not rejected: frame=%#v state=%s calls=%d", frame, connection.State(), app.count())
	}

	gen, out = connection.Replace()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	if err := connection.Handle(context.Background(), server, gen, hello); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, out)
	if welcome.Type != FrameWelcome || connection.State() != ServerAdmissionReady || connection.SessionID() != "s_1" {
		t.Fatalf("handshake state: frame=%#v state=%s session=%q", welcome, connection.State(), connection.SessionID())
	}
	if err := connection.Handle(context.Background(), server, gen, hello); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameError || connection.State() != ServerAdmissionClosed {
		t.Fatalf("second HELLO accepted: frame=%#v state=%s", frame, connection.State())
	}

	gen, out = connection.Replace()
	resume := Envelope{V: WireVersionV2, Type: FrameResume, SessionID: "s_1", Payload: mustPayload(ResumePayload{LastSeq: 0})}
	if err := connection.Handle(context.Background(), server, gen, resume); err != nil {
		t.Fatal(err)
	}
	if frame := nextTestFrame(t, out); frame.Type != FrameWelcome || connection.State() != ServerAdmissionReady {
		t.Fatalf("resume state: frame=%#v state=%s", frame, connection.State())
	}
}

func TestServerAdmissionReplacementFencesLateHandshake(t *testing.T) {
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
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	connection := NewServerAdmission()
	oldGeneration, _ := connection.Replace()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{})}
	done := make(chan error, 1)
	go func() { done <- connection.Handle(context.Background(), server, oldGeneration, hello) }()
	<-started
	newGeneration, _ := connection.Replace()
	close(release)
	_ = <-done
	if connection.State() != ServerAdmissionAwaitingHandshake || connection.SessionID() != "" || connection.Generation() != newGeneration {
		t.Fatalf("late handshake mutated replacement: state=%s session=%q", connection.State(), connection.SessionID())
	}
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
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    repository,
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	})
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
