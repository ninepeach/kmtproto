package kmtproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode"
	"unicode/utf8"
)

func ValidateEnvelope(e *Envelope, limits Limits) error {
	limits = normalizeLimits(limits)
	if e == nil {
		return NewProtocolError(ErrorBadRequest, "nil envelope")
	}
	if err := validateLimits(limits); err != nil {
		return NewProtocolError(ErrorBadRequest, "invalid protocol limits: "+err.Error())
	}
	if e.V != WireVersionV2 {
		return &ProtocolError{Code: ErrorUnsupportedVersion, Message: fmt.Sprintf("unsupported wire version %d", e.V), Close: true}
	}
	if e.Type == "" {
		return NewProtocolError(ErrorBadRequest, "missing frame type")
	}
	if !validBoundedProtocolString(e.ID, limits.MaxIDLength, true) {
		return NewProtocolError(ErrorBadRequest, "invalid or oversized frame id")
	}
	if !validBoundedProtocolString(e.SessionID, limits.MaxSessionIDLength, true) {
		return NewProtocolError(ErrorBadRequest, "invalid or oversized session id")
	}
	if len(e.Payload) > limits.MaxPayloadSize {
		return NewProtocolError(ErrorBadRequest, "payload exceeds maximum size")
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		return NewProtocolError(ErrorBadRequest, "frame cannot be encoded: "+err.Error())
	}
	if len(encoded) > limits.MaxFrameSize {
		return NewProtocolError(ErrorBadRequest, "frame exceeds maximum size")
	}
	return nil
}

func ValidateFrame(e *Envelope, limits Limits, strict bool) error {
	limits = normalizeLimits(limits)
	if err := ValidateEnvelope(e, limits); err != nil {
		return err
	}
	zeroSeq := func() error {
		if e.Seq != 0 {
			return NewProtocolError(ErrorBadRequest, string(e.Type)+" must not carry sequence")
		}
		return nil
	}
	session := func() error {
		if e.SessionID == "" {
			return NewProtocolError(ErrorBadRequest, string(e.Type)+" requires session_id")
		}
		return nil
	}
	noEnvelopeID := func() error {
		if e.ID != "" {
			return NewProtocolError(ErrorBadRequest, string(e.Type)+" must not carry envelope id")
		}
		return nil
	}
	requireEnvelopeID := func() error {
		if e.ID == "" {
			return NewProtocolError(ErrorBadRequest, string(e.Type)+" requires id")
		}
		return nil
	}
	validContent := func(raw json.RawMessage) error {
		if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || !json.Valid(raw) {
			return NewProtocolError(ErrorBadRequest, "content must contain one valid JSON value")
		}
		return nil
	}

	switch e.Type {
	case FrameHello:
		if e.SessionID != "" || e.Seq != 0 {
			return NewProtocolError(ErrorBadRequest, "HELLO must not carry session_id or sequence")
		}
		var p HelloPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if !validBoundedProtocolString(p.ClientName, limits.MaxClientNameLength, true) {
			return NewProtocolError(ErrorBadRequest, "HELLO carries an invalid or oversized client_name")
		}
		_, err := validateAndCopyCapabilityOffers(p.Capabilities, limits)
		return err
	case FrameWelcome:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := noEnvelopeID(); err != nil {
			return err
		}
		var p WelcomePayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.Mode != WelcomeModeNew && p.Mode != WelcomeModeResumed {
			return NewProtocolError(ErrorBadRequest, "invalid WELCOME mode")
		}
		hasResumeFrom, err := jsonFieldPresent(e.Payload, "resume_from")
		if err != nil {
			return err
		}
		hasReplayTo, err := jsonFieldPresent(e.Payload, "replay_to")
		if err != nil {
			return err
		}
		hasAcceptedCapabilities, err := jsonFieldPresent(e.Payload, "accepted_capabilities")
		if err != nil {
			return err
		}
		hasStateSync, err := jsonFieldPresent(e.Payload, "state_sync")
		if err != nil {
			return err
		}
		hasNonNullStateSync, err := jsonFieldPresentAndNonNull(e.Payload, "state_sync")
		if err != nil {
			return err
		}
		if hasStateSync && !hasNonNullStateSync {
			return NewProtocolError(ErrorBadRequest, "WELCOME state_sync must not be null")
		}
		if p.Mode == WelcomeModeNew && (hasResumeFrom || hasReplayTo) {
			return NewProtocolError(ErrorBadRequest, "NEW WELCOME must not carry replay bounds")
		}
		if p.Mode == WelcomeModeNew && hasStateSync {
			return NewProtocolError(ErrorBadRequest, "NEW WELCOME must not carry state_sync")
		}
		if p.Mode == WelcomeModeResumed && hasAcceptedCapabilities {
			return NewProtocolError(ErrorBadRequest, "RESUMED WELCOME must not renegotiate capabilities")
		}
		if p.Mode == WelcomeModeResumed && (!hasResumeFrom || !hasReplayTo || p.ResumeFrom == 0) {
			return NewProtocolError(ErrorBadRequest, "RESUMED WELCOME requires explicit replay bounds")
		}
		if p.Mode == WelcomeModeResumed && p.ReplayTo < p.ResumeFrom-1 {
			return NewProtocolError(ErrorBadRequest, "invalid replay bounds")
		}
		if p.Mode == WelcomeModeResumed && p.StateSync != nil {
			if err := validateResumeStateSync(p.StateSync, limits); err != nil {
				return err
			}
		}
		return ValidateNegotiatedCapabilities(p.AcceptedCapabilities, limits)
	case FramePing:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := noEnvelopeID(); err != nil {
			return err
		}
		var p PingPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if !validBoundedProtocolString(p.PingID, limits.MaxIDLength, false) {
			return NewProtocolError(ErrorBadRequest, "PING requires a valid ping_id")
		}
		return nil
	case FramePong:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := noEnvelopeID(); err != nil {
			return err
		}
		var p PongPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if !validBoundedProtocolString(p.PingID, limits.MaxIDLength, false) {
			return NewProtocolError(ErrorBadRequest, "PONG requires a valid ping_id")
		}
		return nil
	case FrameSend:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if e.ID == "" {
			return NewProtocolError(ErrorBadRequest, "SEND requires id")
		}
		var p SendPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		return validContent(p.Content)
	case FrameAck:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := noEnvelopeID(); err != nil {
			return err
		}
		var p AckPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if !validBoundedProtocolString(p.RefID, limits.MaxIDLength, false) {
			return NewProtocolError(ErrorBadRequest, "ACK requires a valid ref_id")
		}
		return nil
	case FrameEvent:
		if err := session(); err != nil {
			return err
		}
		if e.ID == "" || e.Seq == 0 {
			return NewProtocolError(ErrorBadRequest, "EVENT requires id and positive sequence")
		}
		var p EventPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if !validBoundedProtocolString(p.EventType, limits.MaxEventTypeLength, true) {
			return NewProtocolError(ErrorBadRequest, "EVENT carries an invalid or oversized event_type")
		}
		return validContent(p.Content)
	case FrameResume:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := noEnvelopeID(); err != nil {
			return err
		}
		var p ResumePayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		hasLastSeq, err := jsonFieldPresent(e.Payload, "last_seq")
		if err != nil {
			return err
		}
		if !hasLastSeq {
			return NewProtocolError(ErrorBadRequest, "RESUME requires explicit last_seq")
		}
		hasStateSync, err := jsonFieldPresent(e.Payload, "state_sync")
		if err != nil {
			return err
		}
		if hasStateSync && p.StateSync == nil {
			return NewProtocolError(ErrorBadRequest, "RESUME state_sync must not be null")
		}
		if p.StateSync != nil {
			return validateResumeStateSync(p.StateSync, limits)
		}
		return nil
	case FrameStateQuery:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := requireEnvelopeID(); err != nil {
			return err
		}
		var p StateQueryPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if len(p.ObjectIDs) == 0 || len(p.ObjectIDs) > limits.MaxStateQueryObjects {
			return NewProtocolError(ErrorBadRequest, "STATE_QUERY requires a bounded non-empty object_ids list")
		}
		seen := make(map[string]struct{}, len(p.ObjectIDs))
		for _, objectID := range p.ObjectIDs {
			if err := ValidateStateIdentity(p.Namespace, objectID, limits); err != nil {
				return err
			}
			if _, duplicate := seen[objectID]; duplicate {
				return NewProtocolError(ErrorBadRequest, "STATE_QUERY contains duplicate object_id")
			}
			seen[objectID] = struct{}{}
		}
		return nil
	case FrameStateSnapshot:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := requireEnvelopeID(); err != nil {
			return err
		}
		if len(e.Payload) > limits.MaxStateSnapshotBytes {
			return NewProtocolError(ErrorBadRequest, "STATE_SNAPSHOT exceeds byte limit")
		}
		hasStates, err := jsonFieldPresentAndNonNull(e.Payload, "states")
		if err != nil {
			return err
		}
		if !hasStates {
			return NewProtocolError(ErrorBadRequest, "STATE_SNAPSHOT requires states")
		}
		var p StateSnapshotPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if len(p.States) > limits.MaxStateSnapshotObjects {
			return NewProtocolError(ErrorBadRequest, "STATE_SNAPSHOT exceeds object limit")
		}
		seen := make(map[StateIdentity]struct{}, len(p.States))
		for i := range p.States {
			if err := ValidateStateObject(&p.States[i], limits); err != nil {
				return err
			}
			identity := p.States[i].Identity()
			if _, duplicate := seen[identity]; duplicate {
				return NewProtocolError(ErrorBadRequest, "STATE_SNAPSHOT contains duplicate object identity")
			}
			seen[identity] = struct{}{}
		}
		return nil
	case FrameStateUpdate:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := requireEnvelopeID(); err != nil {
			return err
		}
		var p StateUpdatePayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		return ValidateStateObject(&p.State, limits)
	case FrameError:
		if err := zeroSeq(); err != nil {
			return err
		}
		if err := noEnvelopeID(); err != nil {
			return err
		}
		var p ErrorPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.Code == "" || len(p.Message) > limits.MaxErrorMessageLength {
			return NewProtocolError(ErrorBadRequest, "ERROR requires code and bounded message")
		}
		hasRetryable, err := jsonFieldPresent(e.Payload, "retryable")
		if err != nil {
			return err
		}
		if !hasRetryable {
			return NewProtocolError(ErrorBadRequest, "ERROR requires explicit retryable")
		}
		behavior, known := BehaviorForErrorCode(p.Code)
		if !known {
			return NewProtocolError(ErrorBadRequest, "unknown ERROR code")
		}
		if behavior.RetryabilityFixed && p.Retryable != behavior.Retryable {
			return NewProtocolError(ErrorBadRequest, "ERROR retryable flag conflicts with code")
		}
		if !validBoundedProtocolString(p.RefID, limits.MaxIDLength, true) {
			return NewProtocolError(ErrorBadRequest, "ERROR carries an invalid ref_id")
		}
		return nil
	default:
		return NewProtocolError(ErrorBadRequest, "unknown frame type")
	}
}

func validateOutboundFrame(e *Envelope, limits Limits, strict bool) error {
	return ValidateFrame(e, normalizeLimits(limits), strict)
}

func jsonFieldPresent(raw json.RawMessage, field string) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false, NewProtocolError(ErrorBadRequest, "invalid payload: "+err.Error())
	}
	_, ok := fields[field]
	return ok, nil
}

func jsonFieldPresentAndNonNull(raw json.RawMessage, field string) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false, NewProtocolError(ErrorBadRequest, "invalid payload: "+err.Error())
	}
	value, ok := fields[field]
	return ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")), nil
}

func validateResumeStateSync(sync *ResumeStateSync, limits Limits) error {
	limits = normalizeLimits(limits)
	if sync == nil || len(sync.Namespaces) == 0 || len(sync.Namespaces) > limits.MaxStateSyncNamespaces {
		return NewProtocolError(ErrorBadRequest, "state_sync requires a bounded non-empty namespace list")
	}
	previous := ""
	for i, namespace := range sync.Namespaces {
		if err := validateStateNamespace(namespace, limits.MaxStateNamespaceLength); err != nil {
			return err
		}
		if i > 0 && namespace <= previous {
			return NewProtocolError(ErrorBadRequest, "state_sync namespaces must be unique and canonically ordered")
		}
		previous = namespace
	}
	return nil
}

func knownErrorCode(code string) bool {
	_, ok := BehaviorForErrorCode(code)
	return ok
}

func validBoundedProtocolString(value string, maxLength int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maxLength || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
