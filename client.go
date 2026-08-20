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

// ClientProtocol is a concurrency-safe client-side protocol state machine. It
// does not own network connections, open WebSocket/TCP/QUIC connections, or
// manage transport lifecycle. It only produces protocol actions for its caller
// to execute and never calls application or transport code while holding its
// state lock.
type ClientProtocol struct {
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
	gapResume             bool
}

// NewClientProtocol creates a client-side protocol state machine.
// ClientProtocol methods are safe for concurrent use. They never perform
// network I/O or invoke user callbacks; transport lifecycle remains
// caller-owned and callers execute returned actions after transitions finish.
func NewClientProtocol(config ClientConfig) (*ClientProtocol, error) {
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
	if err := validateImplementedCapabilityOffers(offers); err != nil {
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
	return &ClientProtocol{
		config:       config,
		state:        StateDisconnected,
		eventIDs:     make(map[uint64]string),
		outbox:       make(map[string]Envelope),
		stateObjects: make(map[StateIdentity]StateObject),
		stateQueries: make(map[string]StateQueryPayload),
	}, nil
}

func (c *ClientProtocol) State() ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *ClientProtocol) Generation() ConnectionGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *ClientProtocol) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *ClientProtocol) LastSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSeq
}

// Capabilities returns a defensive copy of the capabilities negotiated for
// the current logical Session. The result is immutable until the Session is
// abandoned; reconnect Resume does not renegotiate it.
func (c *ClientProtocol) Capabilities() []NegotiatedCapability {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.List()
}

// CapabilityEnabled reports whether the current Session negotiated the named
// capability. It returns false before WELCOME and after Session abandonment.
func (c *ClientProtocol) CapabilityEnabled(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Enabled(name)
}

// CapabilityVersion returns the version enabled for the current Session. It
// returns ok=false before WELCOME, after abandonment, or for an unsupported
// capability.
func (c *ClientProtocol) CapabilityVersion(name string) (version uint16, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Version(name)
}

// StateObject returns a defensive copy of the latest accepted State Object.
// The client cache is process-local protocol state, not persistent storage.
func (c *ClientProtocol) StateObject(namespace, objectID string) (StateObject, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	object, found := c.stateObjects[StateIdentity{Namespace: namespace, ObjectID: objectID}]
	if !found {
		return StateObject{}, false
	}
	return cloneStateObject(object), true
}

func (c *ClientProtocol) BeginConnect() ConnectionGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	c.state = StateConnecting
	c.resetConnectionTransientLocked()
	return c.generation
}

func (c *ClientProtocol) TransportConnected(generation ConnectionGeneration) error {
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

func (c *ClientProtocol) StartSession(generation ConnectionGeneration, clientName string) ([]Action, error) {
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

func (c *ClientProtocol) Resume(generation ConnectionGeneration) ([]Action, error) {
	now := c.config.Clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startResumeLocked(generation, nil, now)
}

// ResumeWithState restores the EVENT stream and then requires one complete
// State snapshot for the requested namespaces before the Session becomes
// READY. The state-sync capability must already belong to the Session.
func (c *ClientProtocol) ResumeWithState(generation ConnectionGeneration, namespaces []string) ([]Action, error) {
	now := c.config.Clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if !stateSyncEnabled(c.capabilities) {
		return nil, NewProtocolError(ErrorProtocolViolation, "state-sync capability version 1 was not negotiated")
	}
	canonical := append([]string(nil), namespaces...)
	sortStrings(canonical)
	if err := validateResumeStateSync(&ResumeStateSync{Namespaces: canonical}, c.config.Limits); err != nil {
		return nil, err
	}
	return c.startResumeLocked(generation, canonical, now)
}

func (c *ClientProtocol) startResumeLocked(generation ConnectionGeneration, namespaces []string, now time.Time) ([]Action, error) {
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.state != StateConnected || c.sessionID == "" {
		return nil, ErrInvalidState
	}
	frame := c.resumeFrameLocked(namespaces, now)
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.beginRecoveryLocked(namespaces, false)
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *ClientProtocol) Disconnect(generation ConnectionGeneration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return ErrStaleConnection
	}
	c.state = StateDisconnected
	c.resetConnectionTransientLocked()
	return nil
}

// QueryState creates a correlated STATE_QUERY for the current READY Session.
// Query responses are not replayable; a reconnect clears pending queries.
func (c *ClientProtocol) QueryState(queryID, namespace string, objectIDs []string) ([]Action, error) {
	now := c.config.Clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateReady || c.sessionID == "" {
		return nil, ErrInvalidState
	}
	if !stateSyncEnabled(c.capabilities) {
		return nil, NewProtocolError(ErrorProtocolViolation, "state-sync capability version 1 was not negotiated")
	}
	ids := append([]string(nil), objectIDs...)
	sortStrings(ids)
	payload := StateQueryPayload{Namespace: namespace, ObjectIDs: ids}
	frame := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateQuery,
		ID:        queryID,
		SessionID: c.sessionID,
		Timestamp: now.UnixMilli(),
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

func (c *ClientProtocol) EnqueueSend(messageID string, content json.RawMessage) ([]Action, error) {
	now := c.config.Clock.Now()
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
	frame := Envelope{V: WireVersionV2, Type: FrameSend, ID: messageID, SessionID: c.sessionID, Timestamp: now.UnixMilli(), Payload: mustPayload(SendPayload{Content: append([]byte(nil), content...)})}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.outbox[messageID] = copyEnvelope(frame)
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *ClientProtocol) RetryPending() ([]Action, error) {
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

func (c *ClientProtocol) SendPing(generation ConnectionGeneration, pingID string) ([]Action, error) {
	now := c.config.Clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.state != StateReady || c.pendingPing != nil || pingID == "" {
		return nil, ErrInvalidState
	}
	frame := Envelope{V: WireVersionV2, Type: FramePing, SessionID: c.sessionID, Timestamp: now.UnixMilli(), Payload: mustPayload(PingPayload{PingID: pingID, ClientTime: now.UnixMilli()})}
	if err := validateOutboundFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		return nil, err
	}
	c.pendingPing = &PendingPing{ID: pingID, SentAt: now, Generation: generation}
	return []Action{SendFrameAction{Frame: frame}}, nil
}

func (c *ClientProtocol) CheckHeartbeat(generation ConnectionGeneration) ([]Action, error) {
	now := c.config.Clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if c.pendingPing == nil {
		return nil, nil
	}
	if c.state == StateReady && now.Sub(c.pendingPing.SentAt) >= c.config.HeartbeatTimeout {
		c.state = StateSuspect
		c.suspectAt = now
		return nil, nil
	}
	if c.state == StateSuspect && now.Sub(c.suspectAt) >= c.config.DisconnectGrace {
		c.state = StateDisconnected
		c.resetConnectionTransientLocked()
		return []Action{CloseConnectionAction{Reason: "heartbeat timeout"}}, nil
	}
	return nil, nil
}

func (c *ClientProtocol) HandleIncoming(generation ConnectionGeneration, frame Envelope) ([]Action, error) {
	now := c.config.Clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return nil, ErrStaleConnection
	}
	if err := ValidateFrame(&frame, c.config.Limits, c.config.StrictValidation); err != nil {
		if frame.Type == FrameError {
			c.state = StateDisconnected
			c.resetConnectionTransientLocked()
			return []Action{CloseConnectionAction{Reason: "invalid ERROR frame"}}, nil
		}
		return c.handleIncomingFailureLocked(err)
	}
	switch frame.Type {
	case FrameWelcome:
		// NEW WELCOME establishes the Session; RESUMED WELCOME is correlated by
		// handleWelcomeLocked after decoding its mode and bounds.
	case FrameError:
		if frame.SessionID != "" && frame.SessionID != c.sessionID {
			return c.handleIncomingFailureLocked(NewProtocolError(ErrorProtocolViolation, "ERROR belongs to another session"))
		}
	default:
		if frame.SessionID != c.sessionID {
			return c.handleIncomingFailureLocked(NewProtocolError(ErrorProtocolViolation, "frame belongs to another session"))
		}
	}

	var actions []Action
	var err error
	switch frame.Type {
	case FrameWelcome:
		actions, err = c.handleWelcomeLocked(frame)
	case FramePong:
		actions, err = c.handlePongLocked(generation, frame)
	case FrameAck:
		actions, err = c.handleAckLocked(frame)
	case FrameEvent:
		actions, err = c.handleEventLocked(frame, now)
	case FrameStateSnapshot:
		actions, err = c.handleStateSnapshotLocked(frame)
	case FrameStateUpdate:
		actions, err = c.handleStateUpdateLocked(frame)
	case FrameError:
		actions, err = c.handleErrorLocked(frame)
	default:
		err = NewProtocolError(ErrorProtocolViolation, "unexpected server frame type")
	}
	if err != nil {
		return c.handleIncomingFailureLocked(err)
	}
	return actions, nil
}

func (c *ClientProtocol) handleStateSnapshotLocked(frame Envelope) ([]Action, error) {
	if c.state == StateResuming {
		return c.handleResumeStateSnapshotLocked(frame)
	}
	if c.state != StateReady {
		return nil, ErrInvalidState
	}
	if !stateSyncEnabled(c.capabilities) {
		return nil, NewProtocolError(ErrorProtocolViolation, "STATE_SNAPSHOT requires state-sync capability version 1")
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

func (c *ClientProtocol) handleResumeStateSnapshotLocked(frame Envelope) ([]Action, error) {
	if !stateSyncEnabled(c.capabilities) || len(c.resumeStateNamespaces) == 0 {
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
	c.resetConnectionTransientLocked()
	c.state = StateReady
	actions = append(actions, SessionReadyAction{SessionID: c.sessionID})
	return actions, nil
}

func (c *ClientProtocol) handleStateUpdateLocked(frame Envelope) ([]Action, error) {
	if c.state != StateReady {
		return nil, ErrInvalidState
	}
	if !stateSyncEnabled(c.capabilities) {
		return nil, NewProtocolError(ErrorProtocolViolation, "STATE_UPDATE requires state-sync capability version 1")
	}
	var payload StateUpdatePayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		return nil, err
	}
	return c.applyStateObjectsLocked([]StateObject{payload.State})
}

func (c *ClientProtocol) applyStateObjectsLocked(objects []StateObject) ([]Action, error) {
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

func (c *ClientProtocol) handleWelcomeLocked(frame Envelope) ([]Action, error) {
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
		c.gapResume = false
		if c.replayTo-c.lastSeq > c.config.MaxReplayEvents {
			return c.stopReplayLocked("replay exceeds configured event limit"), nil
		}
		if c.replayTo == c.lastSeq {
			if len(c.resumeStateNamespaces) > 0 {
				return nil, nil
			}
			c.resetConnectionTransientLocked()
			c.state = StateReady
			return []Action{SessionReadyAction{SessionID: c.sessionID}}, nil
		}
		return nil, nil
	default:
		return nil, ErrInvalidState
	}
}

func (c *ClientProtocol) handlePongLocked(generation ConnectionGeneration, frame Envelope) ([]Action, error) {
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

func (c *ClientProtocol) handleAckLocked(frame Envelope) ([]Action, error) {
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

func (c *ClientProtocol) handleEventLocked(frame Envelope, now time.Time) ([]Action, error) {
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
			if c.gapResume {
				// EVENTs already in flight on the connection that exposed the Gap
				// are superseded by the fixed Replay requested from lastSeq. They
				// must not advance sequence or reach the Application.
				return nil, nil
			}
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
		c.resetConnectionTransientLocked()
		c.state = StateReady
		actions = append(actions, SessionReadyAction{SessionID: c.sessionID})
		return actions, nil
	}

	if frame.Seq > c.lastSeq+1 {
		resume := c.resumeFrameLocked(nil, now)
		if err := validateOutboundFrame(&resume, c.config.Limits, c.config.StrictValidation); err != nil {
			return nil, err
		}
		c.beginRecoveryLocked(nil, true)
		return []Action{SendFrameAction{Frame: resume}}, nil
	}
	c.lastSeq = frame.Seq
	c.rememberEventLocked(frame.Seq, frame.ID)
	return []Action{DeliverEventAction{Event: copyEnvelope(frame)}}, nil
}

func (c *ClientProtocol) handleErrorLocked(frame Envelope) ([]Action, error) {
	var payload ErrorPayload
	if err := decodePayload(frame.Payload, &payload, c.config.StrictValidation); err != nil {
		// An invalid ERROR never causes another ERROR frame.
		c.state = StateDisconnected
		c.resetConnectionTransientLocked()
		return []Action{CloseConnectionAction{Reason: "invalid ERROR frame"}}, nil
	}
	if payload.RefID != "" {
		delete(c.stateQueries, payload.RefID)
	}
	actions := []Action{ProtocolErrorAction{Error: payload}}
	behavior, _ := BehaviorForErrorCode(payload.Code)
	wasResuming := c.state == StateResuming
	stateRecovery := len(c.resumeStateNamespaces) > 0
	sessionID := c.sessionID
	needClose := behavior.CloseConnection
	if wasResuming {
		c.state = StateDisconnected
		c.resetConnectionTransientLocked()
		if behavior.FullSyncRequired || behavior.AbandonSession || (stateRecovery && payload.Code == ErrorStateSyncRequired) {
			actions = append(actions, FullSyncRequiredAction{SessionID: sessionID})
		} else if !needClose {
			// Every rejected Resume attempt is terminal. Retryable errors may be
			// retried only after the caller establishes a new connection.
			needClose = true
		}
	}
	if behavior.AbandonSession {
		c.abandonSessionLocked()
	}
	if needClose {
		c.state = StateDisconnected
		c.resetConnectionTransientLocked()
		actions = append(actions, CloseConnectionAction{Reason: payload.Code})
	}
	return actions, nil
}

func (c *ClientProtocol) abandonSessionLocked() {
	c.state = StateDisconnected
	c.sessionID = ""
	c.lastSeq = 0
	c.eventIDs = make(map[uint64]string)
	c.outbox = make(map[string]Envelope)
	c.capabilities = SessionCapabilities{}
	c.stateObjects = make(map[StateIdentity]StateObject)
	c.stateBytes = 0
	c.resetConnectionTransientLocked()
}

func (c *ClientProtocol) handleIncomingFailureLocked(err error) ([]Action, error) {
	if c.state != StateResuming {
		return nil, err
	}
	reason := "resume failed: " + err.Error()
	if len(c.resumeStateNamespaces) > 0 {
		return c.stopStateResumeLocked(reason), nil
	}
	return c.stopReplayLocked(reason), nil
}

func (c *ClientProtocol) beginRecoveryLocked(namespaces []string, gap bool) {
	c.resetConnectionTransientLocked()
	c.state = StateResuming
	c.resumeStateNamespaces = append([]string(nil), namespaces...)
	c.gapResume = gap
}

func (c *ClientProtocol) resetConnectionTransientLocked() {
	c.pendingPing = nil
	c.suspectAt = time.Time{}
	c.stateQueries = make(map[string]StateQueryPayload)
	c.replayBuffer = nil
	c.replayTo = 0
	c.replayBytes = 0
	c.resumeStateNamespaces = nil
	c.resumeEventsComplete = false
	c.gapResume = false
}

func (c *ClientProtocol) resumeFrameLocked(namespaces []string, now time.Time) Envelope {
	payload := ResumePayload{LastSeq: c.lastSeq}
	if len(namespaces) > 0 {
		payload.StateSync = &ResumeStateSync{Namespaces: append([]string(nil), namespaces...)}
	}
	return Envelope{V: WireVersionV2, Type: FrameResume, SessionID: c.sessionID, Timestamp: now.UnixMilli(), Payload: mustPayload(payload)}
}

func (c *ClientProtocol) rememberEventLocked(seq uint64, eventID string) {
	c.eventIDs[seq] = eventID
	if seq > c.config.EventIdentityWindow {
		delete(c.eventIDs, seq-c.config.EventIdentityWindow)
	}
}

func (c *ClientProtocol) stopReplayLocked(reason string) []Action {
	c.state = StateDisconnected
	c.resetConnectionTransientLocked()
	return []Action{
		ProtocolErrorAction{Error: ErrorPayload{Code: ErrorSyncRequired, Message: reason, Retryable: false}},
		FullSyncRequiredAction{SessionID: c.sessionID},
		CloseConnectionAction{Reason: reason},
	}
}

func (c *ClientProtocol) stopStateResumeLocked(reason string) []Action {
	c.state = StateDisconnected
	c.resetConnectionTransientLocked()
	return []Action{
		ProtocolErrorAction{Error: ErrorPayload{Code: ErrorStateSyncRequired, Message: reason, Retryable: false}},
		FullSyncRequiredAction{SessionID: c.sessionID},
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
