package kmtproto

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type ClientConfig struct {
	Clock               Clock
	Limits              Limits
	HeartbeatTimeout    time.Duration
	DisconnectGrace     time.Duration
	StrictValidation    bool
	MaxReplayEvents     uint64
	MaxReplayBytes      int
	EventIdentityWindow uint64
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Clock:               RealClock{},
		Limits:              DefaultLimits(),
		HeartbeatTimeout:    20 * time.Second,
		DisconnectGrace:     10 * time.Second,
		StrictValidation:    true,
		MaxReplayEvents:     DefaultMaxReplayEvents,
		MaxReplayBytes:      DefaultMaxReplayBytes,
		EventIdentityWindow: DefaultEventIdentityWindow,
	}
}

type PendingPing struct {
	ID         string
	SentAt     time.Time
	Generation ConnectionGeneration
}

// Client is a concurrency-safe protocol state machine. It does not execute the
// actions it returns and never calls application or transport code while
// holding its state lock.
type Client struct {
	mu sync.Mutex

	config      ClientConfig
	state       ConnectionState
	generation  ConnectionGeneration
	sessionID   string
	lastSeq     uint64
	eventIDs    map[uint64]string
	outbox      map[string]Envelope
	pendingPing *PendingPing
	suspectAt   time.Time

	replayTo     uint64
	replayBuffer []Envelope
	replayBytes  int
}

// NewClient creates a client protocol state machine. Client methods are safe
// for concurrent use. They never perform network I/O or invoke user callbacks;
// callers execute the returned actions after each transition completes.
func NewClient(config ClientConfig) (*Client, error) {
	if config.Clock == nil {
		config.Clock = RealClock{}
	}
	if config.Limits.MaxFrameSize == 0 {
		config.Limits = DefaultLimits()
	}
	if config.MaxReplayEvents == 0 {
		config.MaxReplayEvents = DefaultMaxReplayEvents
	}
	if config.MaxReplayBytes == 0 {
		config.MaxReplayBytes = DefaultMaxReplayBytes
	}
	if config.EventIdentityWindow == 0 {
		config.EventIdentityWindow = DefaultEventIdentityWindow
	}
	if config.HeartbeatTimeout <= 0 || config.DisconnectGrace <= 0 {
		return nil, fmt.Errorf("kmtproto: heartbeat durations must be positive")
	}
	if config.MaxReplayBytes < 0 {
		return nil, fmt.Errorf("kmtproto: replay byte limit must be positive")
	}
	return &Client{config: config, state: StateDisconnected, eventIDs: make(map[uint64]string), outbox: make(map[string]Envelope)}, nil
}

func (c *Client) State() ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Client) Generation() ConnectionGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *Client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *Client) LastSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSeq
}

func (c *Client) BeginConnect() ConnectionGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	c.state = StateConnecting
	c.pendingPing = nil
	c.suspectAt = time.Time{}
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	return c.generation
}

func (c *Client) TransportConnected(generation ConnectionGeneration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return ErrStaleConnection
	}
	if c.state != StateConnecting {
		return ErrInvalidState
	}
	c.state = StateConnected
	return nil
}

func (c *Client) StartSession(generation ConnectionGeneration, clientName string) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.state != StateConnected || c.sessionID != "" {
		return nil, ErrInvalidState
	}
	frame := Envelope{V: WireVersionV1, Type: FrameHello, Payload: mustPayload(HelloPayload{ClientName: clientName})}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.state = StateHandshaking
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *Client) Resume(generation ConnectionGeneration) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.state != StateConnected || c.sessionID == "" {
		return nil, ErrInvalidState
	}
	frame := c.resumeFrameLocked()
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.state = StateResuming
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *Client) Disconnect(generation ConnectionGeneration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return ErrStaleConnection
	}
	c.state = StateDisconnected
	c.pendingPing = nil
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	return nil
}

func (c *Client) EnqueueSend(messageID string, content json.RawMessage) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateReady || c.sessionID == "" {
		return nil, ErrInvalidState
	}
	if messageID == "" || len(messageID) > c.config.Limits.MaxIDLength || !json.Valid(content) {
		return nil, NewProtocolError(ErrorBadRequest, "invalid SEND id or content")
	}
	if len(content) > c.config.Limits.MaxPayloadSize {
		return nil, NewProtocolError(ErrorBadRequest, "SEND content exceeds maximum payload size")
	}
	if _, exists := c.outbox[messageID]; exists {
		return nil, NewProtocolError(ErrorProtocolViolation, "duplicate local message id")
	}
	frame := Envelope{V: WireVersionV1, Type: FrameSend, ID: messageID, SessionID: c.sessionID, Timestamp: c.config.Clock.Now().UnixMilli(), Payload: mustPayload(SendPayload{Content: append([]byte(nil), content...)})}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.outbox[messageID] = copyEnvelope(frame)
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *Client) RetryPending() ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateReady {
		return nil, ErrInvalidState
	}
	ids := make([]string, 0, len(c.outbox))
	for id := range c.outbox {
		ids = append(ids, id)
	}
	sortStrings(ids)
	actions := make([]Action, 0, len(ids))
	for _, id := range ids {
		actions = append(actions, SendFrameAction{Frame: copyEnvelope(c.outbox[id])})
	}
	return actions, nil
}

func (c *Client) SendPing(generation ConnectionGeneration, pingID string) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.state != StateReady || c.pendingPing != nil || pingID == "" {
		return nil, ErrInvalidState
	}
	now := c.config.Clock.Now()
	frame := Envelope{V: WireVersionV1, Type: FramePing, SessionID: c.sessionID, Timestamp: now.UnixMilli(), Payload: mustPayload(PingPayload{PingID: pingID, ClientTime: now.UnixMilli()})}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.pendingPing = &PendingPing{ID: pingID, SentAt: now, Generation: generation}
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *Client) CheckHeartbeat(generation ConnectionGeneration) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.pendingPing == nil {
		return nil, nil
	}
	now := c.config.Clock.Now()
	if c.state == StateReady && now.Sub(c.pendingPing.SentAt) >= c.config.HeartbeatTimeout {
		c.state = StateSuspect
		c.suspectAt = now
		return nil, nil
	}
	if c.state == StateSuspect && now.Sub(c.suspectAt) >= c.config.DisconnectGrace {
		c.state = StateDisconnected
		c.pendingPing = nil
		return []Action{CloseConnectionAction{Reason: "heartbeat timeout"}}, nil
	}
	return nil, nil
}

func (c *Client) HandleIncoming(generation ConnectionGeneration, frame Envelope) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if err := ValidateFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		if frame.Type == FrameError {
			c.state = StateDisconnected
			return []Action{CloseConnectionAction{Reason: "invalid ERROR frame"}}, nil
		}
		return nil, err
	}
	if frame.Type != FrameWelcome && frame.Type != FrameError && frame.SessionID != c.sessionID {
		return nil, NewProtocolError(ErrorProtocolViolation, "frame belongs to another session")
	}

	switch frame.Type {
	case FrameWelcome:
		return c.handleWelcomeLocked(frame)
	case FramePong:
		return c.handlePongLocked(generation, frame)
	case FrameAck:
		return c.handleAckLocked(frame)
	case FrameEvent:
		return c.handleEventLocked(frame)
	case FrameError:
		return c.handleErrorLocked(frame)
	default:
		return nil, NewProtocolError(ErrorProtocolViolation, "unexpected server frame type")
	}
}

func (c *Client) handleWelcomeLocked(frame Envelope) ([]Action, error) {
	var payload WelcomePayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	switch c.state {
	case StateHandshaking:
		if payload.Mode != WelcomeModeNew || frame.SessionID == "" {
			return nil, NewProtocolError(ErrorProtocolViolation, "expected NEW WELCOME")
		}
		c.sessionID = frame.SessionID
		c.state = StateReady
		return []Action{SessionReadyAction{SessionID: c.sessionID}}, nil
	case StateResuming:
		if payload.Mode != WelcomeModeResumed || frame.SessionID != c.sessionID || payload.ResumeFrom != c.lastSeq+1 || payload.ReplayTo < c.lastSeq {
			return nil, NewProtocolError(ErrorProtocolViolation, "invalid resume acknowledgement")
		}
		c.replayTo = payload.ReplayTo
		c.replayBuffer = nil
		c.replayBytes = 0
		if c.replayTo-c.lastSeq > c.config.MaxReplayEvents {
			return c.stopReplayLocked("replay exceeds configured event limit"), nil
		}
		if c.replayTo == c.lastSeq {
			c.state = StateReady
			return []Action{SessionReadyAction{SessionID: c.sessionID}}, nil
		}
		return nil, nil
	default:
		return nil, ErrInvalidState
	}
}

func (c *Client) handlePongLocked(generation ConnectionGeneration, frame Envelope) ([]Action, error) {
	if c.state != StateReady && c.state != StateSuspect {
		return nil, ErrInvalidState
	}
	var payload PongPayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	if c.pendingPing == nil || payload.PingID != c.pendingPing.ID || generation != c.pendingPing.Generation {
		return nil, nil
	}
	c.pendingPing = nil
	c.suspectAt = time.Time{}
	if c.state == StateSuspect {
		c.state = StateReady
	}
	return nil, nil
}

func (c *Client) handleAckLocked(frame Envelope) ([]Action, error) {
	if c.state != StateReady && c.state != StateSuspect {
		return nil, ErrInvalidState
	}
	var payload AckPayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	if _, ok := c.outbox[payload.RefID]; !ok {
		return nil, nil
	}
	delete(c.outbox, payload.RefID)
	return []Action{AckedAction{MessageID: payload.RefID}}, nil
}

func (c *Client) handleEventLocked(frame Envelope) ([]Action, error) {
	if c.state != StateReady && c.state != StateResuming {
		return nil, ErrInvalidState
	}
	if frame.Seq <= c.lastSeq {
		knownID, known := c.eventIDs[frame.Seq]
		if known && knownID == frame.ID {
			return nil, nil
		}
		if !known {
			return nil, ErrIdentityExpired
		}
		return nil, ErrProtocolConflict
	}
	if c.state == StateResuming {
		if c.replayTo == 0 {
			return nil, NewProtocolError(ErrorProtocolViolation, "EVENT arrived before RESUMED WELCOME")
		}
		expected := c.lastSeq + uint64(len(c.replayBuffer)) + 1
		if frame.Seq != expected || frame.Seq > c.replayTo {
			return nil, NewProtocolError(ErrorProtocolViolation, "invalid replay sequence")
		}
		frameBytes := replayEventBytes(frame)
		if uint64(len(c.replayBuffer))+1 > c.config.MaxReplayEvents || frameBytes > c.config.MaxReplayBytes-c.replayBytes {
			return c.stopReplayLocked("replay exceeds configured memory limit"), nil
		}
		c.replayBuffer = append(c.replayBuffer, copyEnvelope(frame))
		c.replayBytes += frameBytes
		if frame.Seq != c.replayTo {
			return nil, nil
		}
		actions := make([]Action, 0, len(c.replayBuffer)+1)
		for _, event := range c.replayBuffer {
			c.lastSeq = event.Seq
			c.rememberEventLocked(event.Seq, event.ID)
			actions = append(actions, DeliverEventAction{Event: copyEnvelope(event)})
		}
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		c.state = StateReady
		actions = append(actions, SessionReadyAction{SessionID: c.sessionID})
		return actions, nil
	}

	if frame.Seq > c.lastSeq+1 {
		c.state = StateResuming
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		return []Action{SendFrameAction{Frame: c.resumeFrameLocked()}}, nil
	}
	c.lastSeq = frame.Seq
	c.rememberEventLocked(frame.Seq, frame.ID)
	return []Action{DeliverEventAction{Event: copyEnvelope(frame)}}, nil
}

func (c *Client) handleErrorLocked(frame Envelope) ([]Action, error) {
	var payload ErrorPayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		// An invalid ERROR never causes another ERROR frame.
		c.state = StateDisconnected
		return []Action{CloseConnectionAction{Reason: "invalid ERROR frame"}}, nil
	}
	actions := []Action{ProtocolErrorAction{Error: payload}}
	behavior, _ := BehaviorForErrorCode(payload.Code)
	if c.state == StateResuming && behavior.FullSyncRequired {
		c.state = StateDisconnected
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		actions = append(actions, FullSyncRequiredAction{SessionID: c.sessionID})
	}
	if c.state == StateResuming && behavior.AbandonSession {
		c.state = StateDisconnected
		c.sessionID = ""
		c.lastSeq = 0
		c.eventIDs = make(map[uint64]string)
		c.outbox = make(map[string]Envelope)
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
	}
	if behavior.CloseConnection {
		c.state = StateDisconnected
		actions = append(actions, CloseConnectionAction{Reason: payload.Code})
	}
	return actions, nil
}

func (c *Client) resumeFrameLocked() Envelope {
	return Envelope{V: WireVersionV1, Type: FrameResume, SessionID: c.sessionID, Timestamp: c.config.Clock.Now().UnixMilli(), Payload: mustPayload(ResumePayload{LastSeq: c.lastSeq})}
}

func (c *Client) rememberEventLocked(seq uint64, eventID string) {
	c.eventIDs[seq] = eventID
	if seq > c.config.EventIdentityWindow {
		delete(c.eventIDs, seq-c.config.EventIdentityWindow)
	}
}

func (c *Client) stopReplayLocked(reason string) []Action {
	c.state = StateDisconnected
	c.pendingPing = nil
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	return []Action{
		ProtocolErrorAction{Error: ErrorPayload{Code: ErrorSyncRequired, Message: reason, Retryable: false}},
		FullSyncRequiredAction{SessionID: c.sessionID},
		CloseConnectionAction{Reason: reason},
	}
}

func replayEventBytes(frame Envelope) int {
	return len(frame.ID) + len(frame.SessionID) + len(frame.Payload) + 32
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
