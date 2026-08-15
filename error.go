package kmtproto

import "fmt"

const (
	ErrorBadRequest         = "BAD_REQUEST"
	ErrorUnsupportedVersion = "UNSUPPORTED_VERSION"
	ErrorUnauthorized       = "UNAUTHORIZED"
	ErrorInvalidSession     = "INVALID_SESSION"
	ErrorNotFound           = "NOT_FOUND"
	ErrorRateLimited        = "RATE_LIMITED"
	ErrorSyncRequired       = "SYNC_REQUIRED"
	ErrorInternal           = "INTERNAL"
	ErrorProtocolViolation  = "PROTOCOL_VIOLATION"
)

var (
	ErrStaleConnection  = &ProtocolError{Code: ErrorProtocolViolation, Message: "stale connection generation"}
	ErrInvalidState     = &ProtocolError{Code: ErrorProtocolViolation, Message: "invalid state transition"}
	ErrProtocolConflict = &ProtocolError{Code: ErrorProtocolViolation, Message: "sequence identity conflict"}
)

type ProtocolError struct {
	Code      string
	Message   string
	RefID     string
	Retryable bool
	Close     bool
}

func (e *ProtocolError) Error() string {
	if e.RefID == "" {
		return fmt.Sprintf("kmtproto: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("kmtproto: %s (%s): %s", e.Code, e.RefID, e.Message)
}

func NewProtocolError(code, message string) *ProtocolError {
	return &ProtocolError{Code: code, Message: message}
}
