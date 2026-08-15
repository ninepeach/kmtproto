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
