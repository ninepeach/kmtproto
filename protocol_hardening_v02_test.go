package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func requireProtocolErrorCode(t *testing.T, err error, code string) *ProtocolError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected protocol error %s", code)
	}
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("expected ProtocolError %s, got %T: %v", code, err, err)
	}
	if protocolErr.Code != code {
		t.Fatalf("protocol error code = %s, want %s: %v", protocolErr.Code, code, err)
	}
	return protocolErr
}

func TestV02ErrorBehaviorAndPayloadValidation(t *testing.T) {
	tests := []struct {
		code      string
		retryable bool
		close     bool
	}{
		{ErrorUnsupportedVersion, false, true},
		{ErrorUnsupportedFeature, false, true},
		{ErrorInvalidCapability, false, false},
		{ErrorInvalidStateVersion, false, false},
		{ErrorStateSyncRequired, false, true},
		{ErrorStateUnavailable, true, true},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			behavior, known := BehaviorForErrorCode(test.code)
			if !known {
				t.Fatalf("standard error code %s is unknown", test.code)
			}
			if behavior.Retryable != test.retryable || behavior.CloseConnection != test.close {
				t.Fatalf("behavior for %s = retryable:%v close:%v, want retryable:%v close:%v", test.code, behavior.Retryable, behavior.CloseConnection, test.retryable, test.close)
			}
			protocolErr := NewProtocolError(test.code, "test")
			if protocolErr.Retryable != test.retryable || protocolErr.Close != test.close {
				t.Fatalf("ProtocolError for %s does not use canonical behavior: %#v", test.code, protocolErr)
			}
		})
	}

	valid := Envelope{
		V:    WireVersionV2,
		Type: FrameError,
		Payload: mustPayload(ErrorPayload{
			Code: ErrorInvalidCapability, RefID: "hello_1", Retryable: false,
		}),
	}
	if err := ValidateFrame(&valid, DefaultLimits(), true); err != nil {
		t.Fatalf("valid ERROR rejected: %v", err)
	}

	wrongRetryability := valid
	wrongRetryability.Payload = mustPayload(ErrorPayload{Code: ErrorStateUnavailable, Retryable: false})
	requireProtocolErrorCode(t, ValidateFrame(&wrongRetryability, DefaultLimits(), true), ErrorBadRequest)
}

func TestInvalidCapabilityReturnsPreciseProtocolError(t *testing.T) {
	hello := Envelope{
		V:    WireVersionV2,
		Type: FrameHello,
		ID:   "hello_1",
		Payload: mustPayload(HelloPayload{Capabilities: []CapabilityOffer{
			{Name: CapabilityStateSync, Versions: []uint16{1}},
			{Name: CapabilityStateSync, Versions: []uint16{1}},
		}}),
	}
	requireProtocolErrorCode(t, ValidateFrame(&hello, DefaultLimits(), true), ErrorInvalidCapability)

	server, _, _ := newTestServer(t, &recordingApp{})
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), hello, outbound); err != nil {
		t.Fatal(err)
	}
	response := nextTestFrame(t, outbound)
	if response.Type != FrameError {
		t.Fatalf("invalid capability response = %s, want ERROR", response.Type)
	}
	var payload ErrorPayload
	if err := decodePayload(response.Payload, &payload, true); err != nil {
		t.Fatal(err)
	}
	if payload.Code != ErrorInvalidCapability || payload.RefID != hello.ID || payload.Retryable {
		t.Fatalf("invalid capability ERROR = %#v", payload)
	}
}

func TestInvalidStateVersionReturnsPreciseProtocolError(t *testing.T) {
	invalid := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateUpdate,
		ID:        "update_0",
		SessionID: "s_1",
		Payload: mustPayload(StateUpdatePayload{State: StateObject{
			Namespace: "message", ObjectID: "msg001", Version: 0, Data: json.RawMessage(`{}`),
		}}),
	}
	requireProtocolErrorCode(t, ValidateFrame(&invalid, DefaultLimits(), true), ErrorInvalidStateVersion)

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
	if _, err := client.HandleIncoming(generation, newer); err != nil {
		t.Fatal(err)
	}
	stale := newer
	stale.ID = "update_5"
	stale.Payload = mustPayload(StateUpdatePayload{State: StateObject{
		Namespace: "message", ObjectID: "msg001", Version: 5, Data: json.RawMessage(`{"status":"read"}`),
	}})
	_, err := client.HandleIncoming(generation, stale)
	requireProtocolErrorCode(t, err, ErrorInvalidStateVersion)
	if !errors.Is(err, ErrStateStale) {
		t.Fatalf("stale State cause was not preserved: %v", err)
	}
	if state, found := client.StateObject("message", "msg001"); !found || state.Version != 6 {
		t.Fatalf("stale State changed cache: %#v found=%v", state, found)
	}
	if client.LastSeq() != 0 {
		t.Fatalf("State validation changed EVENT last_seq to %d", client.LastSeq())
	}
}

func TestAllV02FramesRejectMissingRequiredFields(t *testing.T) {
	validState := StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)}
	tests := []struct {
		name  string
		frame Envelope
	}{
		{"HELLO", Envelope{V: WireVersionV2, Type: FrameHello}},
		{"WELCOME", Envelope{V: WireVersionV2, Type: FrameWelcome, Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})}},
		{"PING", Envelope{V: WireVersionV2, Type: FramePing, SessionID: "s_1", Payload: mustPayload(PingPayload{})}},
		{"PONG", Envelope{V: WireVersionV2, Type: FramePong, SessionID: "s_1", Payload: mustPayload(PongPayload{})}},
		{"SEND", Envelope{V: WireVersionV2, Type: FrameSend, SessionID: "s_1", Payload: mustPayload(SendPayload{Content: json.RawMessage(`{}`)})}},
		{"ACK", Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s_1", Payload: mustPayload(AckPayload{})}},
		{"EVENT", Envelope{V: WireVersionV2, Type: FrameEvent, ID: "event_1", SessionID: "s_1", Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})}},
		{"RESUME", Envelope{V: WireVersionV2, Type: FrameResume, Payload: mustPayload(ResumePayload{})}},
		{"STATE_QUERY", Envelope{V: WireVersionV2, Type: FrameStateQuery, ID: "query_1", SessionID: "s_1", Payload: mustPayload(StateQueryPayload{Namespace: "message"})}},
		{"STATE_SNAPSHOT", Envelope{V: WireVersionV2, Type: FrameStateSnapshot, ID: "query_1", SessionID: "s_1", Payload: json.RawMessage(`{}`)}},
		{"STATE_UPDATE", Envelope{V: WireVersionV2, Type: FrameStateUpdate, ID: "update_1", SessionID: "s_1", Payload: mustPayload(StateUpdatePayload{State: StateObject{Namespace: validState.Namespace, ObjectID: validState.ObjectID, Data: validState.Data}})}},
		{"ERROR", Envelope{V: WireVersionV2, Type: FrameError, Payload: mustPayload(ErrorPayload{})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateFrame(&test.frame, DefaultLimits(), true); err == nil {
				t.Fatalf("%s missing required fields was accepted", test.name)
			}
		})
	}

	unknown := Envelope{V: WireVersionV2, Type: FrameType("FUTURE_FRAME"), Payload: json.RawMessage(`{}`)}
	requireProtocolErrorCode(t, ValidateFrame(&unknown, DefaultLimits(), true), ErrorBadRequest)
}

func TestV02MetadataAndIdentifierValidation(t *testing.T) {
	limits := DefaultLimits()
	tests := []Envelope{
		{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{ClientName: strings.Repeat("x", limits.MaxClientNameLength+1)})},
		{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{ClientName: "bad\nclient"})},
		{V: WireVersionV2, Type: FramePing, SessionID: "s_1", Payload: mustPayload(PingPayload{PingID: "bad\nping"})},
		{V: WireVersionV2, Type: FrameAck, SessionID: "s_1", Payload: mustPayload(AckPayload{RefID: "bad\nref"})},
		{V: WireVersionV2, Type: FrameEvent, ID: "event_1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{EventType: strings.Repeat("x", limits.MaxEventTypeLength+1), Content: json.RawMessage(`{}`)})},
		{V: WireVersionV2, Type: FrameEvent, ID: "event\n1", SessionID: "s_1", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})},
		{V: WireVersionV2, Type: FrameEvent, ID: "event_1", SessionID: "bad\nsession", Seq: 1, Payload: mustPayload(EventPayload{Content: json.RawMessage(`{}`)})},
	}
	for i := range tests {
		if err := ValidateFrame(&tests[i], limits, true); err == nil {
			t.Fatalf("invalid metadata case %d was accepted: %#v", i, tests[i])
		}
	}
}

func TestV02ProtocolLimitsApplyBeforeProcessing(t *testing.T) {
	partial := normalizeLimits(Limits{MaxPayloadSize: 17})
	if partial.MaxPayloadSize != 17 || partial.MaxFrameSize != DefaultLimits().MaxFrameSize || partial.MaxStateObjectSize != DefaultMaxStateObjectSize {
		t.Fatalf("partial limits were not normalized field-by-field: %#v", partial)
	}

	frameLimits := DefaultLimits()
	frameLimits.MaxFrameSize = 64
	frame := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{ClientName: strings.Repeat("x", 48)})}
	requireProtocolErrorCode(t, ValidateFrame(&frame, frameLimits, true), ErrorBadRequest)

	payloadLimits := DefaultLimits()
	payloadLimits.MaxPayloadSize = 8
	requireProtocolErrorCode(t, ValidateFrame(&frame, payloadLimits, true), ErrorBadRequest)

	stateLimits := DefaultLimits()
	stateLimits.MaxStateObjectSize = 80
	state := StateObject{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{"status":"` + strings.Repeat("x", 48) + `"}`)}
	requireProtocolErrorCode(t, ValidateStateObject(&state, stateLimits), ErrorBadRequest)

	snapshotLimits := DefaultLimits()
	snapshotLimits.MaxStateSnapshotObjects = 1
	snapshot := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateSnapshot,
		ID:        "query_1",
		SessionID: "s_1",
		Payload: mustPayload(StateSnapshotPayload{States: []StateObject{
			{Namespace: "message", ObjectID: "msg001", Version: 1, Data: json.RawMessage(`{}`)},
			{Namespace: "message", ObjectID: "msg002", Version: 1, Data: json.RawMessage(`{}`)},
		}}),
	}
	requireProtocolErrorCode(t, ValidateFrame(&snapshot, snapshotLimits, true), ErrorBadRequest)

	snapshotByteLimits := DefaultLimits()
	snapshotByteLimits.MaxStateSnapshotBytes = len(snapshot.Payload) - 1
	requireProtocolErrorCode(t, ValidateFrame(&snapshot, snapshotByteLimits, true), ErrorBadRequest)

	codec := NewJSONCodec()
	codec.Limits.MaxFrameSize = 16
	requireProtocolErrorCode(t, func() error {
		_, err := codec.Decode([]byte(strings.Repeat("x", 17)))
		return err
	}(), ErrorBadRequest)
	requireProtocolErrorCode(t, func() error {
		_, err := NewJSONCodec().Decode([]byte(`{"v":2`))
		return err
	}(), ErrorBadRequest)

	clientConfig := DefaultClientConfig()
	clientConfig.Limits.MaxIDLength = -1
	if _, err := NewClientProtocol(clientConfig); err == nil {
		t.Fatal("NewClientProtocol accepted a negative protocol limit")
	}
}

func TestUnknownFrameServerBehaviorIsDeterministic(t *testing.T) {
	server, _, _ := newTestServer(t, &recordingApp{})
	frame := Envelope{V: WireVersionV2, Type: FrameType("FUTURE_FRAME"), ID: "future_1", Payload: json.RawMessage(`{}`)}
	outbound := NewOutboundQueue()
	if err := server.HandleIncoming(context.Background(), frame, outbound); err != nil {
		t.Fatal(err)
	}
	response := nextTestFrame(t, outbound)
	if response.Type != FrameError {
		t.Fatalf("unknown frame response = %s, want ERROR", response.Type)
	}
	var payload ErrorPayload
	if err := decodePayload(response.Payload, &payload, true); err != nil {
		t.Fatal(err)
	}
	if payload.Code != ErrorBadRequest || payload.RefID != frame.ID || payload.Retryable {
		t.Fatalf("unknown frame ERROR = %#v", payload)
	}
}
