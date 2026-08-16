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

// ErrorBehavior defines the protocol disposition associated with a standard
// ERROR code. INTERNAL is the only code whose retryability is chosen by the
// implementation at the failure site.
type ErrorBehavior struct {
	Retryable         bool
	RetryabilityFixed bool
	CloseConnection   bool
	AbandonSession    bool
	FullSyncRequired  bool
}

// BehaviorForErrorCode returns the v0.1 behavior for a standard ERROR code.
func BehaviorForErrorCode(code string) (ErrorBehavior, bool) {
	switch code {
	case ErrorBadRequest:
		return ErrorBehavior{RetryabilityFixed: true}, true
	case ErrorUnsupportedVersion:
		return ErrorBehavior{RetryabilityFixed: true, CloseConnection: true}, true
	case ErrorUnauthorized:
		return ErrorBehavior{RetryabilityFixed: true, CloseConnection: true}, true
	case ErrorInvalidSession:
		return ErrorBehavior{RetryabilityFixed: true, AbandonSession: true}, true
	case ErrorNotFound:
		return ErrorBehavior{RetryabilityFixed: true}, true
	case ErrorRateLimited:
		return ErrorBehavior{Retryable: true, RetryabilityFixed: true}, true
	case ErrorSyncRequired:
		return ErrorBehavior{RetryabilityFixed: true, FullSyncRequired: true}, true
	case ErrorInternal:
		return ErrorBehavior{}, true
	case ErrorProtocolViolation:
		return ErrorBehavior{RetryabilityFixed: true, CloseConnection: true}, true
	default:
		return ErrorBehavior{}, false
	}
}

var (
	ErrStaleConnection  = &ProtocolError{Code: ErrorProtocolViolation, Message: "stale connection generation"}
	ErrInvalidState     = &ProtocolError{Code: ErrorProtocolViolation, Message: "invalid state transition"}
	ErrProtocolConflict = &ProtocolError{Code: ErrorProtocolViolation, Message: "sequence identity conflict"}
	ErrIdentityExpired  = &ProtocolError{Code: ErrorProtocolViolation, Message: "event identity is outside the retained verification window"}
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
	behavior, _ := BehaviorForErrorCode(code)
	return &ProtocolError{Code: code, Message: message, Retryable: behavior.Retryable, Close: behavior.CloseConnection}
}
