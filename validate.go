package kmtproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

func ValidateEnvelope(e *Envelope, limits Limits) error {
	if e == nil {
		return NewProtocolError(ErrorBadRequest, "nil envelope")
	}
	if e.V != WireVersionV1 {
		return &ProtocolError{Code: ErrorUnsupportedVersion, Message: fmt.Sprintf("unsupported wire version %d", e.V), Close: true}
	}
	if e.Type == "" {
		return NewProtocolError(ErrorBadRequest, "missing frame type")
	}
	if !utf8.ValidString(e.ID) || len(e.ID) > limits.MaxIDLength {
		return NewProtocolError(ErrorBadRequest, "invalid or oversized frame id")
	}
	if !utf8.ValidString(e.SessionID) || len(e.SessionID) > limits.MaxSessionIDLength {
		return NewProtocolError(ErrorBadRequest, "invalid or oversized session id")
	}
	if len(e.Payload) > limits.MaxPayloadSize {
		return NewProtocolError(ErrorBadRequest, "payload exceeds maximum size")
	}
	return nil
}

func ValidateFrame(e *Envelope, limits Limits, strict bool) error {
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
		return decodePayload(e.Payload, &p, strict)
	case FrameWelcome:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		var p WelcomePayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.Mode != WelcomeModeNew && p.Mode != WelcomeModeResumed {
			return NewProtocolError(ErrorBadRequest, "invalid WELCOME mode")
		}
		if p.Mode == WelcomeModeNew && (p.ResumeFrom != 0 || p.ReplayTo != 0) {
			return NewProtocolError(ErrorBadRequest, "NEW WELCOME must not carry replay bounds")
		}
		if p.Mode == WelcomeModeResumed && p.ResumeFrom == 0 {
			return NewProtocolError(ErrorBadRequest, "RESUMED WELCOME requires resume_from")
		}
		if p.Mode == WelcomeModeResumed && p.ReplayTo+1 < p.ResumeFrom {
			return NewProtocolError(ErrorBadRequest, "invalid replay bounds")
		}
		return nil
	case FramePing:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		var p PingPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.PingID == "" || len(p.PingID) > limits.MaxIDLength {
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
		var p PongPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.PingID == "" || len(p.PingID) > limits.MaxIDLength {
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
		var p AckPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.RefID == "" || len(p.RefID) > limits.MaxIDLength {
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
		return validContent(p.Content)
	case FrameResume:
		if err := session(); err != nil {
			return err
		}
		if err := zeroSeq(); err != nil {
			return err
		}
		var p ResumePayload
		return decodePayload(e.Payload, &p, strict)
	case FrameError:
		if err := zeroSeq(); err != nil {
			return err
		}
		var p ErrorPayload
		if err := decodePayload(e.Payload, &p, strict); err != nil {
			return err
		}
		if p.Code == "" || len(p.Message) > limits.MaxErrorMessageLength {
			return NewProtocolError(ErrorBadRequest, "ERROR requires code and bounded message")
		}
		if !knownErrorCode(p.Code) {
			return NewProtocolError(ErrorBadRequest, "unknown ERROR code")
		}
		if len(p.RefID) > limits.MaxIDLength || !utf8.ValidString(p.RefID) {
			return NewProtocolError(ErrorBadRequest, "ERROR carries an invalid ref_id")
		}
		return nil
	default:
		return NewProtocolError(ErrorBadRequest, "unknown frame type")
	}
}

func knownErrorCode(code string) bool {
	switch code {
	case ErrorBadRequest, ErrorUnsupportedVersion, ErrorUnauthorized, ErrorInvalidSession,
		ErrorNotFound, ErrorRateLimited, ErrorSyncRequired, ErrorInternal, ErrorProtocolViolation:
		return true
	default:
		return false
	}
}
