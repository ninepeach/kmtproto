package kmtproto

import "encoding/json"

const (
	WelcomeModeNew     = "NEW"
	WelcomeModeResumed = "RESUMED"
)

type HelloPayload struct {
	ClientName   string            `json:"client_name,omitempty"`
	Capabilities []CapabilityOffer `json:"capabilities,omitempty"`
}

type WelcomePayload struct {
	Mode                 string                 `json:"mode"`
	ServerTime           int64                  `json:"server_time"`
	ResumeFrom           uint64                 `json:"resume_from,omitempty"`
	ReplayTo             uint64                 `json:"replay_to,omitempty"`
	StateSync            *ResumeStateSync       `json:"state_sync,omitempty"`
	AcceptedCapabilities []NegotiatedCapability `json:"accepted_capabilities,omitempty"`
}

// MarshalJSON keeps RESUMED replay boundaries explicit on the wire, including
// a zero replay_to value for an empty event stream.
func (p WelcomePayload) MarshalJSON() ([]byte, error) {
	if p.Mode == WelcomeModeResumed {
		return json.Marshal(struct {
			Mode       string           `json:"mode"`
			ServerTime int64            `json:"server_time"`
			ResumeFrom uint64           `json:"resume_from"`
			ReplayTo   uint64           `json:"replay_to"`
			StateSync  *ResumeStateSync `json:"state_sync,omitempty"`
		}{p.Mode, p.ServerTime, p.ResumeFrom, p.ReplayTo, cloneResumeStateSync(p.StateSync)})
	}
	return json.Marshal(struct {
		Mode                 string                 `json:"mode"`
		ServerTime           int64                  `json:"server_time"`
		AcceptedCapabilities []NegotiatedCapability `json:"accepted_capabilities,omitempty"`
	}{p.Mode, p.ServerTime, p.AcceptedCapabilities})
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
	LastSeq   uint64           `json:"last_seq"`
	StateSync *ResumeStateSync `json:"state_sync,omitempty"`
}

// ResumeStateSync requests one complete post-replay State snapshot for the
// listed namespaces. Namespaces use canonical lexical order on the wire.
type ResumeStateSync struct {
	Namespaces []string `json:"namespaces"`
}

// StateQueryPayload requests the authoritative current State for a bounded
// set of object identities in one namespace.
type StateQueryPayload struct {
	Namespace string   `json:"namespace"`
	ObjectIDs []string `json:"object_ids"`
}

// StateSnapshotPayload is the authoritative response to one STATE_QUERY.
// Missing requested objects are omitted from States.
type StateSnapshotPayload struct {
	States []StateObject `json:"states"`
}

// StateUpdatePayload carries one unsolicited complete State replacement.
type StateUpdatePayload struct {
	State StateObject `json:"state"`
}

type ErrorPayload struct {
	Code      string `json:"code"`
	Message   string `json:"message,omitempty"`
	RefID     string `json:"ref_id,omitempty"`
	Retryable bool   `json:"retryable"`
}

func cloneResumeStateSync(sync *ResumeStateSync) *ResumeStateSync {
	if sync == nil {
		return nil
	}
	return &ResumeStateSync{Namespaces: append([]string(nil), sync.Namespaces...)}
}
