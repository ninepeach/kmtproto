package kmtproto

import "encoding/json"

const WireVersionV1 uint16 = 1

type FrameType string

const (
	FrameHello   FrameType = "HELLO"
	FrameWelcome FrameType = "WELCOME"
	FramePing    FrameType = "PING"
	FramePong    FrameType = "PONG"
	FrameSend    FrameType = "SEND"
	FrameAck     FrameType = "ACK"
	FrameEvent   FrameType = "EVENT"
	FrameResume  FrameType = "RESUME"
	FrameError   FrameType = "ERROR"
)

type Envelope struct {
	V         uint16          `json:"v"`
	Type      FrameType       `json:"type"`
	ID        string          `json:"id,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	Seq       uint64          `json:"seq,omitempty"`
	Timestamp int64           `json:"timestamp,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type ConnectionState uint8

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateHandshaking
	StateResuming
	StateReady
	StateSuspect
)

func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "DISCONNECTED"
	case StateConnecting:
		return "CONNECTING"
	case StateConnected:
		return "CONNECTED"
	case StateHandshaking:
		return "HANDSHAKING"
	case StateResuming:
		return "RESUMING"
	case StateReady:
		return "READY"
	case StateSuspect:
		return "SUSPECT"
	default:
		return "UNKNOWN"
	}
}

type ConnectionGeneration uint64

type DedupState uint8

const (
	DedupProcessing DedupState = iota + 1
	DedupCompleted
)

type DedupRecord struct {
	State DedupState
	Ack   *Envelope
}
