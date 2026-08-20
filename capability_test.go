package kmtproto

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCapabilitySerialization(t *testing.T) {
	codec := NewJSONCodec()
	helloPayload := HelloPayload{
		ClientName: "capability-client",
		Capabilities: []CapabilityOffer{
			{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}, Required: true},
			{Name: "presence", Versions: []uint16{1}},
		},
	}
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(helloPayload)}
	encoded, err := codec.Encode(&hello)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"capabilities"`)) {
		t.Fatalf("HELLO did not serialize capabilities: %s", encoded)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var decodedHello HelloPayload
	if err := decodePayload(decoded.Payload, &decodedHello, true); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedHello, helloPayload) {
		t.Fatalf("HELLO capability round trip mismatch: got %#v want %#v", decodedHello, helloPayload)
	}

	welcomePayload := WelcomePayload{
		Mode:                 WelcomeModeNew,
		ServerTime:           123,
		AcceptedCapabilities: []NegotiatedCapability{{Name: CapabilityStateSync, Version: CapabilityStateSyncVersion}},
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_1", Payload: mustPayload(welcomePayload)}
	encoded, err = codec.Encode(&welcome)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"accepted_capabilities"`)) {
		t.Fatalf("WELCOME did not serialize negotiated capabilities: %s", encoded)
	}
	decoded, err = codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var decodedWelcome WelcomePayload
	if err := decodePayload(decoded.Payload, &decodedWelcome, true); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedWelcome, welcomePayload) {
		t.Fatalf("WELCOME capability round trip mismatch: got %#v want %#v", decodedWelcome, welcomePayload)
	}

	if raw := mustPayload(HelloPayload{}); bytes.Contains(raw, []byte(`"capabilities"`)) {
		t.Fatalf("empty HELLO changed its wire shape: %s", raw)
	}
	if raw := mustPayload(WelcomePayload{Mode: WelcomeModeNew}); bytes.Contains(raw, []byte(`"accepted_capabilities"`)) {
		t.Fatalf("empty WELCOME changed its wire shape: %s", raw)
	}
}

func TestCapabilityNegotiationIntersection(t *testing.T) {
	registry, err := NewCapabilityRegistry([]CapabilitySpec{
		{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}},
		{Name: "presence", Versions: []uint16{1, 3}},
	}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	offers := []CapabilityOffer{
		{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}},
		{Name: "compression.zstd", Versions: []uint16{1}},
		{Name: "presence", Versions: []uint16{3, 2, 1}, Required: true},
	}
	got, err := registry.Negotiate(offers, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := []NegotiatedCapability{
		{Name: "presence", Version: 3},
		{Name: CapabilityStateSync, Version: CapabilityStateSyncVersion},
	}
	if !reflect.DeepEqual(got.List(), want) {
		t.Fatalf("negotiation mismatch: got %#v want %#v", got.List(), want)
	}
}

func TestInvalidCapabilityRejected(t *testing.T) {
	tests := []struct {
		name   string
		offers []CapabilityOffer
	}{
		{name: "uppercase name", offers: []CapabilityOffer{{Name: "State-sync", Versions: []uint16{1}}}},
		{name: "invalid separator", offers: []CapabilityOffer{{Name: "state..sync", Versions: []uint16{1}}}},
		{name: "version encoded in name", offers: []CapabilityOffer{{Name: "state-sync-v2", Versions: []uint16{2}}}},
		{name: "empty versions", offers: []CapabilityOffer{{Name: CapabilityStateSync}}},
		{name: "zero version", offers: []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{0}}}},
		{name: "duplicate version", offers: []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{1, 1}}}},
		{name: "duplicate offer", offers: []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{1}}, {Name: CapabilityStateSync, Versions: []uint16{2}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{Capabilities: test.offers})}
			if err := ValidateFrame(&frame, DefaultLimits(), true); err == nil {
				t.Fatal("expected capability validation failure")
			}
		})
	}

	invalidWelcome := Envelope{
		V:         WireVersionV2,
		Type:      FrameWelcome,
		SessionID: "s_1",
		Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew, AcceptedCapabilities: []NegotiatedCapability{
			{Name: CapabilityStateSync, Version: CapabilityStateSyncVersion},
			{Name: "presence", Version: 1},
		}}),
	}
	if err := ValidateFrame(&invalidWelcome, DefaultLimits(), true); err == nil {
		t.Fatal("expected non-canonical WELCOME capabilities to be rejected")
	}
}

func TestCapabilityHandshakeStoresSessionState(t *testing.T) {
	clock := NewFakeClock(time.Unix(800, 0))
	limits := DefaultLimits()
	registry, err := NewCapabilityRegistry([]CapabilitySpec{{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	sessions := NewMemorySessionRepository()
	replay := NewMemoryReplayStore()
	serverConfig := DefaultServerConfig()
	serverConfig.Clock = clock
	serverConfig.Capabilities = registry
	serverConfig.NewSessionID = func() (string, error) { return "s_cap", nil }
	server, err := NewServerProtocol(serverConfig, ServerDependencies{
		Sessions:    sessions,
		Dedup:       NewMemoryDedupStore(clock, serverConfig.DedupTTL),
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}

	offers := []CapabilityOffer{
		{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}, Required: true},
		{Name: "presence", Versions: []uint16{1}},
	}
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clock
	clientConfig.Capabilities = offers
	client, err := NewClientProtocol(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	if client.CapabilityEnabled(CapabilityStateSync) {
		t.Fatal("client exposed advertised capability before WELCOME")
	}
	if _, ok := client.CapabilityVersion(CapabilityStateSync); ok {
		t.Fatal("client exposed capability version before WELCOME")
	}
	clientGeneration := client.BeginConnect()
	if err := client.TransportConnected(clientGeneration); err != nil {
		t.Fatal(err)
	}
	actions, err := client.StartSession(clientGeneration, "capability-client")
	if err != nil {
		t.Fatal(err)
	}
	hello := actions[0].(SendFrameAction).Frame

	connection := NewServerAdmission()
	serverGeneration, outbound := connection.Replace()
	if connection.CapabilityEnabled(CapabilityStateSync) {
		t.Fatal("server connection exposed capability before handshake admission")
	}
	if err := connection.Handle(context.Background(), server, serverGeneration, hello); err != nil {
		t.Fatal(err)
	}
	welcome := nextTestFrame(t, outbound)
	if _, err := client.HandleIncoming(clientGeneration, welcome); err != nil {
		t.Fatal(err)
	}
	want := []NegotiatedCapability{{Name: CapabilityStateSync, Version: CapabilityStateSyncVersion}}
	if got := client.Capabilities(); !reflect.DeepEqual(got, want) {
		t.Fatalf("client capability state mismatch: got %#v want %#v", got, want)
	}
	if got := connection.Capabilities(); !reflect.DeepEqual(got, want) {
		t.Fatalf("server connection capability state mismatch: got %#v want %#v", got, want)
	}
	if version, ok := client.CapabilityVersion(CapabilityStateSync); !ok || version != CapabilityStateSyncVersion {
		t.Fatalf("client capability query mismatch: version=%d enabled=%v", version, ok)
	}
	if version, ok := connection.CapabilityVersion(CapabilityStateSync); !ok || version != CapabilityStateSyncVersion {
		t.Fatalf("server capability query mismatch: version=%d enabled=%v", version, ok)
	}
	if client.CapabilityEnabled("presence") || connection.CapabilityEnabled("presence") {
		t.Fatal("unsupported optional capability was enabled")
	}
	stored, exists, err := sessions.Lookup("s_cap", clock.Now())
	if err != nil || !exists || !reflect.DeepEqual(stored.Capabilities.List(), want) {
		t.Fatalf("stored session capability state mismatch: state=%#v exists=%v err=%v", stored, exists, err)
	}
	if version, ok := stored.CapabilityVersion(CapabilityStateSync); !ok || version != CapabilityStateSyncVersion {
		t.Fatalf("stored Session capability query mismatch: version=%d enabled=%v", version, ok)
	}

	got := client.Capabilities()
	got[0].Version = 99
	if client.Capabilities()[0].Version != CapabilityStateSyncVersion {
		t.Fatal("client exposed mutable capability state")
	}
	storedCapabilities := stored.Capabilities.List()
	storedCapabilities[0].Version = 99
	storedAgain, _, _ := sessions.Lookup("s_cap", clock.Now())
	if storedAgain.Capabilities.List()[0].Version != CapabilityStateSyncVersion {
		t.Fatal("session repository exposed mutable capability state")
	}

	secondWelcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_cap", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew, AcceptedCapabilities: []NegotiatedCapability{{Name: "presence", Version: 1}}})}
	if _, err := client.HandleIncoming(clientGeneration, secondWelcome); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second WELCOME returned %v, want ErrInvalidState", err)
	}
	if !reflect.DeepEqual(client.Capabilities(), want) || client.CapabilityEnabled("presence") {
		t.Fatal("second WELCOME mutated READY Session capabilities")
	}

	clientGeneration = client.BeginConnect()
	if err := client.TransportConnected(clientGeneration); err != nil {
		t.Fatal(err)
	}
	actions, err = client.Resume(clientGeneration)
	if err != nil {
		t.Fatal(err)
	}
	serverGeneration, outbound = connection.Replace()
	if err := connection.Handle(context.Background(), server, serverGeneration, actions[0].(SendFrameAction).Frame); err != nil {
		t.Fatal(err)
	}
	resumedWelcome := nextTestFrame(t, outbound)
	if _, err := client.HandleIncoming(clientGeneration, resumedWelcome); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.Capabilities(), want) || !reflect.DeepEqual(connection.Capabilities(), want) {
		t.Fatal("Resume did not preserve negotiated Session capabilities")
	}
}

func TestRequiredCapabilityFailureDoesNotCreateSession(t *testing.T) {
	clock := NewFakeClock(time.Unix(900, 0))
	registry, err := NewCapabilityRegistry(nil, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	sessions := NewMemorySessionRepository()
	replay := NewMemoryReplayStore()
	config := DefaultServerConfig()
	config.Clock = clock
	config.Capabilities = registry
	created := false
	config.NewSessionID = func() (string, error) {
		created = true
		return "must_not_exist", nil
	}
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    sessions,
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	outbound := NewOutboundQueue()
	hello := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{Capabilities: []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}, Required: true}}})}
	if err := server.ProcessFrame(context.Background(), hello, outbound); err != nil {
		t.Fatal(err)
	}
	frame := nextTestFrame(t, outbound)
	var payload ErrorPayload
	if frame.Type != FrameError || decodePayload(frame.Payload, &payload, true) != nil || payload.Code != ErrorUnsupportedFeature || payload.Retryable {
		t.Fatalf("unexpected required-capability response: frame=%#v payload=%#v", frame, payload)
	}
	if created {
		t.Fatal("server attempted Session creation before required capability negotiation succeeded")
	}
	if _, exists, err := sessions.Lookup("must_not_exist", clock.Now()); err != nil || exists {
		t.Fatalf("failed negotiation created Session: exists=%v err=%v", exists, err)
	}

	_, err = registry.Negotiate([]CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{CapabilityStateSyncVersion}, Required: true}}, DefaultLimits())
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != ErrorUnsupportedFeature || !protocolErr.Close {
		t.Fatalf("unexpected negotiation error: %v", err)
	}
}
