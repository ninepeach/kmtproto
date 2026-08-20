package kmtproto

import (
	"testing"
	"time"
)

func TestStateSyncCapabilityConfigRequiresImplementedNumericVersion(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_000, 0))
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clock
	clientConfig.Capabilities = []CapabilityOffer{{Name: CapabilityStateSync, Versions: []uint16{2}, Required: true}}
	if _, err := NewClientProtocol(clientConfig); protocolErrorCode(err) != ErrorInvalidCapability {
		t.Fatalf("client accepted unimplemented state-sync version: %v", err)
	}

	registry, err := NewCapabilityRegistry([]CapabilitySpec{{Name: CapabilityStateSync, Versions: []uint16{2}}}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := DefaultServerConfig()
	serverConfig.Clock = clock
	serverConfig.Capabilities = registry
	serverConfig.StateStore = &testStateStore{objects: make(map[StateIdentity]StateObject)}
	serverConfig.NewSessionID = func() (string, error) { return "s_v2_cap", nil }
	replay := NewMemoryReplayStore()
	if _, err := NewServerProtocol(serverConfig, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       NewMemoryDedupStore(clock, serverConfig.DedupTTL),
		Replay:      replay,
		Appender:    replay,
		Application: &recordingApp{},
	}); protocolErrorCode(err) != ErrorInvalidCapability {
		t.Fatalf("server accepted unimplemented state-sync version: %v", err)
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
