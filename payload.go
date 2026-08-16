package kmtproto

import "encoding/json"

const (
	WelcomeModeNew     = "NEW"
	WelcomeModeResumed = "RESUMED"
)

type HelloPayload struct {
	ClientName string `json:"client_name,omitempty"`
}

type WelcomePayload struct {
	Mode       string `json:"mode"`
	ServerTime int64  `json:"server_time"`
	ResumeFrom uint64 `json:"resume_from,omitempty"`
	ReplayTo   uint64 `json:"replay_to,omitempty"`
}

// MarshalJSON keeps RESUMED replay boundaries explicit on the wire, including
// a zero replay_to value for an empty event stream.
func (p WelcomePayload) MarshalJSON() ([]byte, error) {
	if p.Mode == WelcomeModeResumed {
		return json.Marshal(struct {
			Mode       string `json:"mode"`
			ServerTime int64  `json:"server_time"`
			ResumeFrom uint64 `json:"resume_from"`
			ReplayTo   uint64 `json:"replay_to"`
		}{p.Mode, p.ServerTime, p.ResumeFrom, p.ReplayTo})
	}
	return json.Marshal(struct {
		Mode       string `json:"mode"`
		ServerTime int64  `json:"server_time"`
	}{p.Mode, p.ServerTime})
}

type PingPayload struct {
	PingID     string `json:"ping_id"`
	ClientTime int64  `json:"client_time"`
}

type PongPayload struct {
	PingID     string `json:"ping_id"`
	ClientTime int64  `json:"client_time"`
	ServerTime int64  `json:"server_time"`
}

type SendPayload struct {
	Content json.RawMessage `json:"content"`
}

type AckPayload struct {
	RefID string `json:"ref_id"`
}

type EventPayload struct {
	EventType string          `json:"event_type,omitempty"`
	Content   json.RawMessage `json:"content"`
}

type ResumePayload struct {
	LastSeq uint64 `json:"last_seq"`
}

type ErrorPayload struct {
	Code      string `json:"code"`
	Message   string `json:"message,omitempty"`
	RefID     string `json:"ref_id,omitempty"`
	Retryable bool   `json:"retryable"`
}
