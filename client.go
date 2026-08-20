package kmtproto

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ClientConfig struct {
	Clock               Clock
	Limits              Limits
	Capabilities        []CapabilityOffer
	HeartbeatTimeout    time.Duration
	DisconnectGrace     time.Duration
	StrictValidation    bool
	MaxReplayEvents     uint64
	MaxReplayBytes      int
	EventIdentityWindow uint64
	MaxStateObjects     int
	MaxStateBytes       int
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
		MaxStateObjects:     DefaultMaxStateCacheObjects,
		MaxStateBytes:       DefaultMaxStateCacheBytes,
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

	config       ClientConfig
	state        ConnectionState
	generation   ConnectionGeneration
	sessionID    string
	lastSeq      uint64
	eventIDs     map[uint64]string
	outbox       map[string]Envelope
	pendingPing  *PendingPing
	suspectAt    time.Time
	capabilities SessionCapabilities
	stateObjects map[StateIdentity]StateObject
	stateBytes   int
	stateQueries map[string]StateQueryPayload

	replayTo              uint64
	replayBuffer          []Envelope
	replayBytes           int
	resumeStateNamespaces []string
	resumeEventsComplete  bool
}

// NewClient creates a client protocol state machine. Client methods are safe
// for concurrent use. They never perform network I/O or invoke user callbacks;
// callers execute the returned actions after each transition completes.
func NewClient(config ClientConfig) (*Client, error) {
	if config.Clock == nil {
		config.Clock = RealClock{}
	}
	config.Limits = normalizeLimits(config.Limits)
	if err := validateLimits(config.Limits); err != nil {
		return nil, err
	}
	offers, err := validateAndCopyCapabilityOffers(config.Capabilities, config.Limits)
	if err != nil {
		return nil, fmt.Errorf("kmtproto: invalid client capabilities: %w", err)
	}
	config.Capabilities = offers
	if config.MaxReplayEvents == 0 {
		config.MaxReplayEvents = DefaultMaxReplayEvents
	}
	if config.MaxReplayBytes == 0 {
		config.MaxReplayBytes = DefaultMaxReplayBytes
	}
	if config.EventIdentityWindow == 0 {
		config.EventIdentityWindow = DefaultEventIdentityWindow
	}
	if config.MaxStateObjects == 0 {
		config.MaxStateObjects = DefaultMaxStateCacheObjects
	}
	if config.MaxStateBytes == 0 {
		config.MaxStateBytes = DefaultMaxStateCacheBytes
	}
	if config.HeartbeatTimeout <= 0 || config.DisconnectGrace <= 0 {
		return nil, fmt.Errorf("kmtproto: heartbeat durations must be positive")
	}
	if config.MaxReplayBytes < 0 || config.MaxStateObjects < 0 || config.MaxStateBytes < 0 {
		return nil, fmt.Errorf("kmtproto: replay and State limits must be positive")
	}
	return &Client{
		config:       config,
		state:        StateDisconnected,
		eventIDs:     make(map[uint64]string),
		outbox:       make(map[string]Envelope),
		stateObjects: make(map[StateIdentity]StateObject),
		stateQueries: make(map[string]StateQueryPayload),
	}, nil
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

// Capabilities returns a defensive copy of the capabilities negotiated for
// the current logical Session. The result is immutable until the Session is
// abandoned; reconnect Resume does not renegotiate it.
func (c *Client) Capabilities() []NegotiatedCapability {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.List()
}

// CapabilityEnabled reports whether the current Session negotiated the named
// capability. It returns false before WELCOME and after Session abandonment.
func (c *Client) CapabilityEnabled(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Enabled(name)
}

// CapabilityVersion returns the version enabled for the current Session. It
// returns ok=false before WELCOME, after abandonment, or for an unsupported
// capability.
func (c *Client) CapabilityVersion(name string) (version uint16, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Version(name)
}

// StateObject returns a defensive copy of the latest accepted State Object.
// The client cache is process-local protocol state, not persistent storage.
func (c *Client) StateObject(namespace, objectID string) (StateObject, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	object, found := c.stateObjects[StateIdentity{Namespace: namespace, ObjectID: objectID}]
	if !found {
		return StateObject{}, false
	}
	return cloneStateObject(object), true
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
	c.resumeStateNamespaces = nil
	c.resumeEventsComplete = false
	c.stateQueries = make(map[string]StateQueryPayload)
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
	frame := Envelope{V: WireVersionV2, Type: FrameHello, Payload: mustPayload(HelloPayload{ClientName: clientName, Capabilities: cloneCapabilityOffers(c.config.Capabilities)})}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.state = StateHandshaking
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *Client) Resume(generation ConnectionGeneration) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startResumeLocked(generation, nil)
}

// ResumeWithState restores the EVENT stream and then requires one complete
// State snapshot for the requested namespaces before the Session becomes
// READY. The state-sync capability must already belong to the Session.
func (c *Client) ResumeWithState(generation ConnectionGeneration, namespaces []string) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if !c.capabilities.Enabled(CapabilityStateSync) {
		return nil, NewProtocolError(ErrorProtocolViolation, "state-sync capability was not negotiated")
	}
	canonical := append([]string(nil), namespaces...)
	sortStrings(canonical)
	if err := validateResumeStateSync(&ResumeStateSync{Namespaces: canonical}, c.config.Limits); err != nil {
		return nil, err
	}
	return c.startResumeLocked(generation, canonical)
}

func (c *Client) startResumeLocked(generation ConnectionGeneration, namespaces []string) ([]Action, error) {
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.state != StateConnected || c.sessionID == "" {
		return nil, ErrInvalidState
	}
	frame := c.resumeFrameLocked(namespaces)
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.state = StateResuming
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	c.resumeStateNamespaces = append([]string(nil), namespaces...)
	c.resumeEventsComplete = false
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
	c.resumeStateNamespaces = nil
	c.resumeEventsComplete = false
	c.stateQueries = make(map[string]StateQueryPayload)
	return nil
}

// QueryState creates a correlated STATE_QUERY for the current READY Session.
// Query responses are not replayable; a reconnect clears pending queries.
func (c *Client) QueryState(queryID, namespace string, objectIDs []string) ([]Action, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateReady || c.sessionID == "" {
		return nil, ErrInvalidState
	}
	if !c.capabilities.Enabled(CapabilityStateSync) {
		return nil, NewProtocolError(ErrorProtocolViolation, "state-sync capability was not negotiated")
	}
	ids := append([]string(nil), objectIDs...)
	sortStrings(ids)
	payload := StateQueryPayload{Namespace: namespace, ObjectIDs: ids}
	frame := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateQuery,
		ID:        queryID,
		SessionID: c.sessionID,
		Timestamp: c.config.Clock.Now().UnixMilli(),
		Payload:   mustPayload(payload),
	}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	if _, exists := c.stateQueries[queryID]; exists {
		return nil, NewProtocolError(ErrorProtocolViolation, "duplicate pending STATE_QUERY id")
	}
	c.stateQueries[queryID] = cloneStateQueryPayload(payload)
	return []Action{SendFrameAction{Frame: frame}}, nil
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
	frame := Envelope{V: WireVersionV2, Type: FrameSend, ID: messageID, SessionID: c.sessionID, Timestamp: c.config.Clock.Now().UnixMilli(), Payload: mustPayload(SendPayload{Content: append([]byte(nil), content...)})}
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
	frame := Envelope{V: WireVersionV2, Type: FramePing, SessionID: c.sessionID, Timestamp: now.UnixMilli(), Payload: mustPayload(PingPayload{PingID: pingID, ClientTime: now.UnixMilli()})}
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
	case FrameStateSnapshot:
		return c.handleStateSnapshotLocked(frame)
	case FrameStateUpdate:
		return c.handleStateUpdateLocked(frame)
	case FrameError:
		return c.handleErrorLocked(frame)
	default:
		return nil, NewProtocolError(ErrorProtocolViolation, "unexpected server frame type")
	}
}

func (c *Client) handleStateSnapshotLocked(frame Envelope) ([]Action, error) {
	if c.state == StateResuming {
		return c.handleResumeStateSnapshotLocked(frame)
	}
	if c.state != StateReady {
		return nil, ErrInvalidState
	}
	if !c.capabilities.Enabled(CapabilityStateSync) {
		return nil, NewProtocolError(ErrorProtocolViolation, "STATE_SNAPSHOT requires state-sync capability")
	}
	query, pending := c.stateQueries[frame.ID]
	if !pending {
		return nil, NewProtocolError(ErrorProtocolViolation, "STATE_SNAPSHOT does not match a pending query")
	}
	var payload StateSnapshotPayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	requested := make(map[string]struct{}, len(query.ObjectIDs))
	for _, objectID := range query.ObjectIDs {
		requested[objectID] = struct{}{}
	}
	for _, object := range payload.States {
		if object.Namespace != query.Namespace {
			return nil, NewProtocolError(ErrorProtocolViolation, "STATE_SNAPSHOT returned another namespace")
		}
		if _, ok := requested[object.ObjectID]; !ok {
			return nil, NewProtocolError(ErrorProtocolViolation, "STATE_SNAPSHOT returned an unrequested object")
		}
	}
	actions, err := c.applyStateObjectsLocked(payload.States)
	if err != nil {
		return nil, err
	}
	delete(c.stateQueries, frame.ID)
	return actions, nil
}

func (c *Client) handleResumeStateSnapshotLocked(frame Envelope) ([]Action, error) {
	if !c.capabilities.Enabled(CapabilityStateSync) || len(c.resumeStateNamespaces) == 0 {
		return nil, NewProtocolError(ErrorProtocolViolation, "unexpected resume STATE_SNAPSHOT")
	}
	if !c.resumeEventsComplete {
		return nil, NewProtocolError(ErrorProtocolViolation, "resume STATE_SNAPSHOT arrived before EVENT replay completed")
	}
	var payload StateSnapshotPayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(c.resumeStateNamespaces))
	for _, namespace := range c.resumeStateNamespaces {
		allowed[namespace] = struct{}{}
	}
	for _, object := range payload.States {
		if _, ok := allowed[object.Namespace]; !ok {
			return nil, NewProtocolError(ErrorProtocolViolation, "resume STATE_SNAPSHOT returned an unrequested namespace")
		}
	}
	stateActions, err := c.applyStateObjectsLocked(payload.States)
	if err != nil {
		return c.stopStateResumeLocked("State snapshot conflicts with retained State"), nil
	}
	actions := make([]Action, 0, len(c.replayBuffer)+len(stateActions)+1)
	for _, event := range c.replayBuffer {
		c.lastSeq = event.Seq
		c.rememberEventLocked(event.Seq, event.ID)
		actions = append(actions, DeliverEventAction{Event: copyEnvelope(event)})
	}
	actions = append(actions, stateActions...)
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	c.resumeStateNamespaces = nil
	c.resumeEventsComplete = false
	c.state = StateReady
	actions = append(actions, SessionReadyAction{SessionID: c.sessionID})
	return actions, nil
}

func (c *Client) handleStateUpdateLocked(frame Envelope) ([]Action, error) {
	if c.state != StateReady {
		return nil, ErrInvalidState
	}
	if !c.capabilities.Enabled(CapabilityStateSync) {
		return nil, NewProtocolError(ErrorProtocolViolation, "STATE_UPDATE requires state-sync capability")
	}
	var payload StateUpdatePayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	return c.applyStateObjectsLocked([]StateObject{payload.State})
}

func (c *Client) applyStateObjectsLocked(objects []StateObject) ([]Action, error) {
	next := make(map[StateIdentity]StateObject, len(c.stateObjects)+len(objects))
	for identity, object := range c.stateObjects {
		next[identity] = object
	}
	nextBytes := c.stateBytes
	actions := make([]Action, 0, len(objects))
	for _, incoming := range objects {
		identity := incoming.Identity()
		current, exists := next[identity]
		var currentPointer *StateObject
		if exists {
			currentPointer = &current
		}
		committed, result, err := ApplyStateObject(currentPointer, incoming, c.config.Limits)
		if err != nil {
			if errors.Is(err, ErrStateStale) || errors.Is(err, ErrStateConflict) {
				return nil, newProtocolErrorWithCause(ErrorInvalidStateVersion, "State version is stale or conflicting", err)
			}
			return nil, err
		}
		if result == StateApplyDuplicate {
			continue
		}
		if !exists && len(next)+1 > c.config.MaxStateObjects {
			return nil, NewProtocolError(ErrorSyncRequired, "State cache exceeds configured object limit")
		}
		if exists {
			nextBytes -= stateObjectBytes(current)
		}
		nextBytes += stateObjectBytes(committed)
		if nextBytes > c.config.MaxStateBytes {
			return nil, NewProtocolError(ErrorSyncRequired, "State cache exceeds configured byte limit")
		}
		next[identity] = committed
		actions = append(actions, StateChangedAction{State: cloneStateObject(committed), Result: result})
	}
	c.stateObjects = next
	c.stateBytes = nextBytes
	return actions, nil
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
		if err := validateNegotiatedAgainstOffers(c.config.Capabilities, payload.AcceptedCapabilities, c.config.Limits); err != nil {
			return nil, err
		}
		capabilities, err := NewSessionCapabilities(payload.AcceptedCapabilities, c.config.Limits)
		if err != nil {
			return nil, err
		}
		c.sessionID = frame.SessionID
		c.capabilities = capabilities
		c.state = StateReady
		return []Action{SessionReadyAction{SessionID: c.sessionID}}, nil
	case StateResuming:
		if payload.Mode != WelcomeModeResumed || frame.SessionID != c.sessionID || payload.ResumeFrom != c.lastSeq+1 || payload.ReplayTo < c.lastSeq {
			return nil, NewProtocolError(ErrorProtocolViolation, "invalid resume acknowledgement")
		}
		if len(c.resumeStateNamespaces) == 0 {
			if payload.StateSync != nil {
				return nil, NewProtocolError(ErrorProtocolViolation, "unexpected resume state_sync acknowledgement")
			}
		} else if payload.StateSync == nil || !equalStrings(payload.StateSync.Namespaces, c.resumeStateNamespaces) {
			return nil, NewProtocolError(ErrorProtocolViolation, "resume state_sync acknowledgement mismatch")
		}
		c.replayTo = payload.ReplayTo
		c.replayBuffer = nil
		c.replayBytes = 0
		c.resumeEventsComplete = c.replayTo == c.lastSeq
		if c.replayTo-c.lastSeq > c.config.MaxReplayEvents {
			return c.stopReplayLocked("replay exceeds configured event limit"), nil
		}
		if c.replayTo == c.lastSeq {
			if len(c.resumeStateNamespaces) > 0 {
				return nil, nil
			}
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
		if len(c.resumeStateNamespaces) > 0 {
			c.resumeEventsComplete = true
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
		c.resumeEventsComplete = false
		c.state = StateReady
		actions = append(actions, SessionReadyAction{SessionID: c.sessionID})
		return actions, nil
	}

	if frame.Seq > c.lastSeq+1 {
		c.state = StateResuming
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		c.resumeStateNamespaces = nil
		c.resumeEventsComplete = false
		return []Action{SendFrameAction{Frame: c.resumeFrameLocked(nil)}}, nil
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
	if payload.RefID != "" {
		delete(c.stateQueries, payload.RefID)
	}
	actions := []Action{ProtocolErrorAction{Error: payload}}
	behavior, _ := BehaviorForErrorCode(payload.Code)
	if c.state == StateResuming && behavior.FullSyncRequired {
		c.state = StateDisconnected
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		c.resumeStateNamespaces = nil
		c.resumeEventsComplete = false
		actions = append(actions, FullSyncRequiredAction{SessionID: c.sessionID})
	}
	if c.state == StateResuming && behavior.AbandonSession {
		c.state = StateDisconnected
		c.sessionID = ""
		c.lastSeq = 0
		c.eventIDs = make(map[uint64]string)
		c.outbox = make(map[string]Envelope)
		c.capabilities = SessionCapabilities{}
		c.stateObjects = make(map[StateIdentity]StateObject)
		c.stateBytes = 0
		c.stateQueries = make(map[string]StateQueryPayload)
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		c.resumeStateNamespaces = nil
		c.resumeEventsComplete = false
	}
	if behavior.CloseConnection {
		c.state = StateDisconnected
		c.pendingPing = nil
		c.replayBuffer = nil
		c.replayTo = 0
		c.replayBytes = 0
		c.resumeStateNamespaces = nil
		c.resumeEventsComplete = false
		actions = append(actions, CloseConnectionAction{Reason: payload.Code})
	}
	return actions, nil
}

func (c *Client) resumeFrameLocked(namespaces []string) Envelope {
	payload := ResumePayload{LastSeq: c.lastSeq}
	if len(namespaces) > 0 {
		payload.StateSync = &ResumeStateSync{Namespaces: append([]string(nil), namespaces...)}
	}
	return Envelope{V: WireVersionV2, Type: FrameResume, SessionID: c.sessionID, Timestamp: c.config.Clock.Now().UnixMilli(), Payload: mustPayload(payload)}
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
	c.resumeStateNamespaces = nil
	c.resumeEventsComplete = false
	return []Action{
		ProtocolErrorAction{Error: ErrorPayload{Code: ErrorSyncRequired, Message: reason, Retryable: false}},
		FullSyncRequiredAction{SessionID: c.sessionID},
		CloseConnectionAction{Reason: reason},
	}
}

func (c *Client) stopStateResumeLocked(reason string) []Action {
	c.state = StateDisconnected
	c.pendingPing = nil
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	c.resumeStateNamespaces = nil
	c.resumeEventsComplete = false
	return []Action{
		ProtocolErrorAction{Error: ErrorPayload{Code: ErrorStateSyncRequired, Message: reason, Retryable: false}},
		CloseConnectionAction{Reason: reason},
	}
}

func replayEventBytes(frame Envelope) int {
	return len(frame.ID) + len(frame.SessionID) + len(frame.Payload) + 32
}

func stateObjectBytes(object StateObject) int {
	return len(object.Namespace) + len(object.ObjectID) + len(object.Data) + 8
}

func cloneStateQueryPayload(payload StateQueryPayload) StateQueryPayload {
	payload.ObjectIDs = append([]string(nil), payload.ObjectIDs...)
	return payload
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
