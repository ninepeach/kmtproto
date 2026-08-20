package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	FailAfterClaim       = "after_claim"
	FailAfterApplication = "after_application"
	FailAfterComplete    = "after_complete"
)

type FailureInjector interface {
	Fail(point string) error
}

type ServerConfig struct {
	Clock            Clock
	Limits           Limits
	Capabilities     *CapabilityRegistry
	StateStore       StateStore
	StateSnapshots   StateSnapshotProvider
	SessionResumeTTL time.Duration
	ReplayTTL        time.Duration
	DedupTTL         time.Duration
	ClientRetryTTL   time.Duration
	StrictValidation bool
	MaxReplayEvents  uint64
	MaxReplayBytes   int
	NewSessionID     func() (string, error)
	NewFrameID       func() (string, error)
	FailureInjector  FailureInjector
}

func DefaultServerConfig() ServerConfig {
	clock := RealClock{}
	return ServerConfig{
		Clock:            clock,
		Limits:           DefaultLimits(),
		SessionResumeTTL: 24 * time.Hour,
		ReplayTTL:        24 * time.Hour,
		DedupTTL:         24 * time.Hour,
		ClientRetryTTL:   time.Hour,
		StrictValidation: true,
		MaxReplayEvents:  DefaultMaxReplayEvents,
		MaxReplayBytes:   DefaultMaxReplayBytes,
		NewSessionID:     DefaultSessionIDGenerator(clock),
		NewFrameID:       DefaultFrameIDGenerator(clock),
	}
}

type streamRequest struct {
	fn   func() error
	done chan error
}

type streamLane struct {
	mu             sync.Mutex
	running        bool
	callbackActive bool
	queue          []streamRequest
}

// ErrStreamCallbackActive means a same-Session stream operation was attempted
// while that lane was executing an injected callback. Callers may retry after
// the callback returns. Failing fast prevents callback self-deadlock without
// weakening per-session serialization.
var ErrStreamCallbackActive = errors.New("kmtproto: session stream callback is active")

// Server is a transport-independent frame processor and is safe for concurrent
// use when its injected stores and application satisfy their concurrency
// contracts. It performs no transport I/O and owns no connection lifecycle.
type Server struct {
	config   ServerConfig
	sessions SessionRepository
	dedup    ServerSessionStore
	replay   ReplayStore
	appender EventAppender
	app      ApplicationHandler

	flightMu sync.Mutex
	flights  map[string]chan struct{}
	laneMu   sync.Mutex
	lanes    map[string]*streamLane
}

type serverHandleResult struct {
	readySessionID string
	capabilities   SessionCapabilities
	close          bool
	abandonSession bool
}

func NewServer(config ServerConfig, sessions SessionRepository, dedup ServerSessionStore, replay ReplayStore, appender EventAppender, app ApplicationHandler) (*Server, error) {
	if config.Clock == nil {
		config.Clock = RealClock{}
	}
	config.Limits = normalizeLimits(config.Limits)
	if err := validateLimits(config.Limits); err != nil {
		return nil, err
	}
	if config.Capabilities == nil {
		config.Capabilities = emptyCapabilityRegistry()
	}
	if err := config.Capabilities.validate(config.Limits); err != nil {
		return nil, fmt.Errorf("kmtproto: invalid server capabilities: %w", err)
	}
	if config.MaxReplayEvents == 0 {
		config.MaxReplayEvents = DefaultMaxReplayEvents
	}
	if config.MaxReplayBytes == 0 {
		config.MaxReplayBytes = DefaultMaxReplayBytes
	}
	if config.MaxReplayBytes < 0 {
		return nil, errors.New("kmtproto: MaxReplayBytes must be positive")
	}
	if config.SessionResumeTTL <= 0 || config.ReplayTTL <= 0 || config.DedupTTL <= 0 || config.ClientRetryTTL <= 0 {
		return nil, errors.New("kmtproto: TTL values must be positive")
	}
	if config.DedupTTL < config.ClientRetryTTL || config.DedupTTL < config.SessionResumeTTL {
		return nil, errors.New("kmtproto: DedupTTL must cover ClientRetryTTL and SessionResumeTTL")
	}
	if config.ReplayTTL < config.SessionResumeTTL {
		return nil, errors.New("kmtproto: ReplayTTL must cover SessionResumeTTL")
	}
	if config.NewSessionID == nil {
		config.NewSessionID = DefaultSessionIDGenerator(config.Clock)
	}
	if config.NewFrameID == nil {
		config.NewFrameID = DefaultFrameIDGenerator(config.Clock)
	}
	if sessions == nil || dedup == nil || replay == nil || appender == nil || app == nil {
		return nil, errors.New("kmtproto: server dependencies are required")
	}
	return &Server{config: config, sessions: sessions, dedup: dedup, replay: replay, appender: appender, app: app, flights: make(map[string]chan struct{}), lanes: make(map[string]*streamLane)}, nil
}

func (s *Server) HandleIncoming(ctx context.Context, frame Envelope, outbound *OutboundQueue) error {
	_, err := s.handleIncoming(ctx, frame, outbound)
	return err
}

func (s *Server) handleIncoming(ctx context.Context, frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
	if outbound == nil {
		return serverHandleResult{}, errors.New("kmtproto: nil outbound queue")
	}
	if err := ValidateFrame(&frame, s.config.Limits, s.config.StrictValidation); err != nil {
		if frame.Type == FrameError {
			// Never create an ERROR-about-ERROR loop.
			outbound.Close()
			return serverHandleResult{close: true}, nil
		}
		var pe *ProtocolError
		if errors.As(err, &pe) {
			_ = s.enqueueError(outbound, frame.SessionID, frame.ID, pe.Code, pe.Message, pe.Retryable)
			behavior, _ := BehaviorForErrorCode(pe.Code)
			return serverHandleResult{close: pe.Close || behavior.CloseConnection}, nil
		}
		return serverHandleResult{}, err
	}

	switch frame.Type {
	case FrameHello:
		return s.handleHello(frame, outbound)
	case FramePing:
		return serverHandleResult{}, s.handlePing(frame, outbound)
	case FrameSend:
		return serverHandleResult{}, s.handleSend(ctx, frame, outbound)
	case FrameStateQuery:
		return s.handleStateQuery(ctx, frame, outbound)
	case FrameResume:
		return s.handleResume(ctx, frame, outbound)
	case FrameError:
		// Never answer ERROR with ERROR.
		outbound.Close()
		return serverHandleResult{close: true}, nil
	default:
		return serverHandleResult{close: true}, s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorProtocolViolation, "unexpected client frame type", false)
	}
}

func (s *Server) handleStateQuery(ctx context.Context, frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
	session, exists, err := s.sessions.Lookup(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return serverHandleResult{}, err
	}
	if !exists {
		return serverHandleResult{abandonSession: true}, s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInvalidSession, "session expired or unknown", false)
	}
	if !session.CapabilityEnabled(CapabilityStateSync) {
		return serverHandleResult{close: true}, s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorProtocolViolation, "STATE_QUERY requires state-sync capability", false)
	}
	if s.config.StateStore == nil {
		return serverHandleResult{}, s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInternal, "State store is unavailable", true)
	}
	var query StateQueryPayload
	if err := decodePayload(frame.Payload, &query, s.config.StrictValidation); err != nil {
		return serverHandleResult{}, err
	}
	snapshot := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateSnapshot,
		ID:        frame.ID,
		SessionID: frame.SessionID,
		Timestamp: s.config.Clock.Now().UnixMilli(),
	}
	snapshotLimits, err := s.stateSnapshotLimits(snapshot)
	if err != nil {
		return serverHandleResult{}, err
	}
	accumulator, err := newStateSnapshotAccumulator(s.config.Limits, snapshotLimits)
	if err != nil {
		return serverHandleResult{}, err
	}
	objectIDs := append([]string(nil), query.ObjectIDs...)
	sortStrings(objectIDs)
	for _, objectID := range objectIDs {
		object, found, err := s.config.StateStore.Get(ctx, query.Namespace, objectID)
		if err != nil {
			return serverHandleResult{}, err
		}
		if !found {
			continue
		}
		if object.Namespace != query.Namespace || object.ObjectID != objectID {
			return serverHandleResult{}, errors.New("kmtproto: StateStore returned the wrong object identity")
		}
		if err := ValidateStateObject(&object, s.config.Limits); err != nil {
			return serverHandleResult{}, fmt.Errorf("kmtproto: StateStore returned invalid State: %w", err)
		}
		if err := accumulator.Add(object); err != nil {
			if errors.Is(err, ErrStateSnapshotLimitExceeded) {
				return serverHandleResult{}, s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorBadRequest, "State snapshot exceeds configured limit; split the query", false)
			}
			return serverHandleResult{}, err
		}
	}
	snapshot.Payload, err = accumulator.Payload()
	if err != nil {
		if errors.Is(err, ErrStateSnapshotLimitExceeded) {
			return serverHandleResult{}, s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorBadRequest, "State snapshot exceeds configured limit; split the query", false)
		}
		return serverHandleResult{}, err
	}
	return serverHandleResult{}, s.enqueueFrame(outbound, snapshot)
}

func (s *Server) PublishEvent(sessionID, eventID, eventType string, content json.RawMessage, outbound *OutboundQueue) error {
	if outbound == nil || sessionID == "" || eventID == "" || !json.Valid(content) || !utf8.ValidString(eventID) {
		return NewProtocolError(ErrorBadRequest, "invalid event publication")
	}
	if len(eventID) > s.config.Limits.MaxIDLength || len(sessionID) > s.config.Limits.MaxSessionIDLength || len(content) > s.config.Limits.MaxPayloadSize {
		return NewProtocolError(ErrorBadRequest, "event publication exceeds protocol limits")
	}
	probe := Envelope{V: WireVersionV2, Type: FrameEvent, ID: eventID, SessionID: sessionID, Seq: ^uint64(0), Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(EventPayload{EventType: eventType, Content: append([]byte(nil), content...)})}
	if err := validateOutboundFrame(&probe, s.config.Limits, s.config.StrictValidation); err != nil {
		return err
	}
	return s.runStream(sessionID, func(lane *streamLane) error {
		var exists bool
		err := lane.invokeCallback(func() error {
			_, found, lookupErr := s.sessions.Lookup(sessionID, s.config.Clock.Now())
			exists = found
			return lookupErr
		})
		if err != nil {
			return err
		}
		if !exists {
			return NewProtocolError(ErrorInvalidSession, "session is not resumable")
		}
		var event Envelope
		err = lane.invokeCallback(func() error {
			var appendErr error
			event, appendErr = s.appender.Append(sessionID, eventID, eventType, content, s.config.Clock.Now().UnixMilli())
			return appendErr
		})
		if err != nil {
			return err
		}
		return s.enqueueFrame(outbound, event)
	})
}

// PublishStateUpdate emits one already-committed complete State replacement.
// It neither writes StateStore nor participates in the EVENT stream or replay.
func (s *Server) PublishStateUpdate(sessionID, updateID string, object StateObject, outbound *OutboundQueue) error {
	if outbound == nil {
		return errors.New("kmtproto: nil outbound queue")
	}
	if err := ValidateStateObject(&object, s.config.Limits); err != nil {
		return err
	}
	return s.runStream(sessionID, func(lane *streamLane) error {
		var session SessionState
		var exists bool
		err := lane.invokeCallback(func() error {
			var lookupErr error
			session, exists, lookupErr = s.sessions.Lookup(sessionID, s.config.Clock.Now())
			return lookupErr
		})
		if err != nil {
			return err
		}
		if !exists {
			return NewProtocolError(ErrorInvalidSession, "session expired or unknown")
		}
		if !session.CapabilityEnabled(CapabilityStateSync) {
			return NewProtocolError(ErrorProtocolViolation, "STATE_UPDATE requires state-sync capability")
		}
		frame := Envelope{
			V:         WireVersionV2,
			Type:      FrameStateUpdate,
			ID:        updateID,
			SessionID: sessionID,
			Timestamp: s.config.Clock.Now().UnixMilli(),
			Payload:   mustPayload(StateUpdatePayload{State: cloneStateObject(object)}),
		}
		return s.enqueueFrame(outbound, frame)
	})
}

func (s *Server) handleHello(frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
	var hello HelloPayload
	if err := decodePayload(frame.Payload, &hello, s.config.StrictValidation); err != nil {
		return serverHandleResult{}, err
	}
	accepted, err := s.config.Capabilities.Negotiate(hello.Capabilities, s.config.Limits)
	if err != nil {
		var protocolErr *ProtocolError
		if errors.As(err, &protocolErr) {
			enqueueErr := s.enqueueError(outbound, "", frame.ID, protocolErr.Code, protocolErr.Message, protocolErr.Retryable)
			return serverHandleResult{close: protocolErr.Close}, enqueueErr
		}
		return serverHandleResult{}, err
	}
	sessionID, err := s.config.NewSessionID()
	if err != nil || sessionID == "" {
		return serverHandleResult{}, s.enqueueError(outbound, "", frame.ID, ErrorInternal, "cannot create session", true)
	}
	if len(sessionID) > s.config.Limits.MaxSessionIDLength || !utf8.ValidString(sessionID) {
		return serverHandleResult{}, fmt.Errorf("kmtproto: generated session id exceeds limit")
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: sessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew, ServerTime: s.config.Clock.Now().UnixMilli(), AcceptedCapabilities: accepted.List()})}
	if err := validateOutboundFrame(&welcome, s.config.Limits, s.config.StrictValidation); err != nil {
		return serverHandleResult{}, err
	}
	state := SessionState{SessionID: sessionID, ExpiresAt: s.config.Clock.Now().Add(s.config.SessionResumeTTL), Capabilities: accepted}
	if err := s.sessions.Create(state); err != nil {
		return serverHandleResult{}, err
	}
	if err := s.enqueueFrame(outbound, welcome); err != nil {
		return serverHandleResult{}, err
	}
	return serverHandleResult{readySessionID: sessionID, capabilities: accepted}, nil
}

func (s *Server) handlePing(frame Envelope, outbound *OutboundQueue) error {
	_, exists, err := s.sessions.Lookup(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return err
	}
	if !exists {
		return s.enqueueError(outbound, frame.SessionID, "", ErrorInvalidSession, "session expired or unknown", false)
	}
	var ping PingPayload
	if err := decodePayload(frame.Payload, &ping, s.config.StrictValidation); err != nil {
		return err
	}
	pong := Envelope{V: WireVersionV2, Type: FramePong, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(PongPayload{PingID: ping.PingID, ClientTime: ping.ClientTime, ServerTime: s.config.Clock.Now().UnixMilli()})}
	return s.enqueueFrame(outbound, pong)
}

func (s *Server) handleSend(ctx context.Context, frame Envelope, outbound *OutboundQueue) error {
	_, exists, err := s.sessions.Lookup(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return err
	}
	if !exists {
		return s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInvalidSession, "session expired or unknown", false)
	}
	key := dedupKey(frame.SessionID, frame.ID)
	done, leader := s.acquireFlight(key)
	if !leader {
		return s.waitForOriginal(ctx, done, frame, outbound)
	}
	defer s.finishFlight(key, done)

	claimed, existing, err := s.dedup.Claim(frame.SessionID, frame.ID)
	if err != nil {
		return err
	}
	if !claimed {
		if existing != nil && existing.State == DedupCompleted && existing.Ack != nil {
			return s.enqueueFrame(outbound, *existing.Ack)
		}
		return s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInternal, "SEND is still processing; retry with the same id", true)
	}

	if err := s.inject(FailAfterClaim); err != nil {
		return err
	}
	var payload SendPayload
	if err := decodePayload(frame.Payload, &payload, s.config.StrictValidation); err != nil {
		_ = s.dedup.Abort(frame.SessionID, frame.ID)
		return err
	}
	if err := s.app.HandleSend(ctx, frame.ID, append([]byte(nil), payload.Content...)); err != nil {
		_ = s.dedup.Abort(frame.SessionID, frame.ID)
		return s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInternal, "application rejected SEND", true)
	}
	if err := s.inject(FailAfterApplication); err != nil {
		return err
	}
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(AckPayload{RefID: frame.ID})}
	if err := validateOutboundFrame(&ack, s.config.Limits, s.config.StrictValidation); err != nil {
		return err
	}
	if err := s.dedup.Complete(frame.SessionID, frame.ID, &ack); err != nil {
		return err
	}
	if err := s.inject(FailAfterComplete); err != nil {
		return err
	}
	return s.enqueueFrame(outbound, ack)
}

func (s *Server) waitForOriginal(ctx context.Context, done <-chan struct{}, frame Envelope, outbound *OutboundQueue) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	claimed, existing, err := s.dedup.Claim(frame.SessionID, frame.ID)
	if err != nil {
		return err
	}
	if claimed {
		_ = s.dedup.Abort(frame.SessionID, frame.ID)
		return s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInternal, "original SEND did not complete; retry", true)
	}
	if existing != nil && existing.State == DedupCompleted && existing.Ack != nil {
		return s.enqueueFrame(outbound, *existing.Ack)
	}
	return s.enqueueError(outbound, frame.SessionID, frame.ID, ErrorInternal, "original SEND did not complete; retry", true)
}

func (s *Server) handleResume(ctx context.Context, frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
	var resume ResumePayload
	if err := decodePayload(frame.Payload, &resume, s.config.StrictValidation); err != nil {
		return serverHandleResult{}, err
	}
	state, exists, err := s.sessions.Lookup(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return serverHandleResult{}, err
	}
	if !exists {
		return serverHandleResult{abandonSession: true}, s.enqueueError(outbound, frame.SessionID, "", ErrorInvalidSession, "session expired or unknown", false)
	}
	if resume.StateSync != nil {
		if !state.CapabilityEnabled(CapabilityStateSync) {
			return serverHandleResult{close: true}, s.enqueueError(outbound, frame.SessionID, "", ErrorUnsupportedFeature, "RESUME state_sync capability was not negotiated", false)
		}
		if s.config.StateSnapshots == nil {
			return serverHandleResult{close: true}, s.enqueueError(outbound, frame.SessionID, "", ErrorStateUnavailable, "State snapshot provider is unavailable", true)
		}
	}
	result := serverHandleResult{capabilities: state.Capabilities}
	err = s.runStream(frame.SessionID, func(lane *streamLane) error {
		var replayTo uint64
		err := lane.invokeCallback(func() error {
			var currentErr error
			replayTo, currentErr = s.replay.CurrentSeq(frame.SessionID)
			return currentErr
		})
		if err != nil {
			return err
		}
		if resume.LastSeq > replayTo {
			result.close = true
			return s.enqueueError(outbound, frame.SessionID, "", ErrorProtocolViolation, "last_seq is ahead of server stream", false)
		}
		if resume.LastSeq == math.MaxUint64 {
			return s.enqueueError(outbound, frame.SessionID, "", ErrorSyncRequired, "event sequence is exhausted", false)
		}
		if replayTo-resume.LastSeq > s.config.MaxReplayEvents {
			return s.enqueueError(outbound, frame.SessionID, "", ErrorSyncRequired, "replay exceeds configured event limit", false)
		}
		var events []Envelope
		err = lane.invokeCallback(func() error {
			var replayErr error
			events, replayErr = s.replay.Replay(frame.SessionID, resume.LastSeq, replayTo, ReplayLimits{
				MaxEvents: s.config.MaxReplayEvents,
				MaxBytes:  s.config.MaxReplayBytes,
			})
			return replayErr
		})
		if errors.Is(err, ErrReplayLimitExceeded) {
			return s.enqueueError(outbound, frame.SessionID, "", ErrorSyncRequired, "replay exceeds configured byte limit", false)
		}
		if errors.Is(err, ErrReplayUnavailable) {
			return s.enqueueError(outbound, frame.SessionID, "", ErrorSyncRequired, "replay window no longer covers last_seq", false)
		}
		if err != nil {
			return err
		}
		expectedSeq := resume.LastSeq + 1
		for i := range events {
			if err := validateOutboundFrame(&events[i], s.config.Limits, s.config.StrictValidation); err != nil {
				return fmt.Errorf("kmtproto: invalid replay event: %w", err)
			}
			if events[i].Type != FrameEvent || events[i].SessionID != frame.SessionID || events[i].Seq != expectedSeq {
				return errors.New("kmtproto: replay store returned a non-contiguous event range")
			}
			expectedSeq++
		}
		if expectedSeq != replayTo+1 {
			return errors.New("kmtproto: replay store returned an incomplete event range")
		}
		var stateSnapshot *Envelope
		if resume.StateSync != nil {
			snapshot, err := s.buildResumeStateSnapshot(ctx, lane, frame.SessionID, resume.StateSync.Namespaces)
			if err != nil {
				result.close = true
				return s.enqueueError(outbound, frame.SessionID, "", ErrorStateUnavailable, "State snapshot could not be produced", true)
			}
			stateSnapshot = &snapshot
		}
		welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ServerTime: s.config.Clock.Now().UnixMilli(), ResumeFrom: resume.LastSeq + 1, ReplayTo: replayTo, StateSync: cloneResumeStateSync(resume.StateSync)})}
		if err := validateOutboundFrame(&welcome, s.config.Limits, s.config.StrictValidation); err != nil {
			return err
		}
		batch := make([]Envelope, 0, len(events)+2)
		batch = append(batch, welcome)
		batch = append(batch, events...)
		if stateSnapshot != nil {
			batch = append(batch, *stateSnapshot)
		}
		if err := outbound.EnqueueBatch(batch); err != nil {
			return err
		}
		result.readySessionID = frame.SessionID
		return nil
	})
	return result, err
}

func (s *Server) buildResumeStateSnapshot(ctx context.Context, lane *streamLane, sessionID string, namespaces []string) (Envelope, error) {
	frameID, err := s.config.NewFrameID()
	if err != nil {
		return Envelope{}, err
	}
	snapshot := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateSnapshot,
		ID:        frameID,
		SessionID: sessionID,
		Timestamp: s.config.Clock.Now().UnixMilli(),
	}
	snapshotLimits, err := s.stateSnapshotLimits(snapshot)
	if err != nil {
		return Envelope{}, err
	}
	var states []StateObject
	err = lane.invokeCallback(func() error {
		var snapshotErr error
		states, snapshotErr = s.config.StateSnapshots.Snapshot(ctx, append([]string(nil), namespaces...), snapshotLimits)
		return snapshotErr
	})
	if err != nil {
		return Envelope{}, err
	}
	if len(states) > snapshotLimits.MaxObjects {
		return Envelope{}, ErrStateSnapshotLimitExceeded
	}
	allowed := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		allowed[namespace] = struct{}{}
	}
	copyStates := make([]StateObject, len(states))
	seen := make(map[StateIdentity]struct{}, len(states))
	for i := range states {
		if err := ValidateStateObject(&states[i], s.config.Limits); err != nil {
			return Envelope{}, err
		}
		if _, ok := allowed[states[i].Namespace]; !ok {
			return Envelope{}, errors.New("kmtproto: State snapshot returned an unrequested namespace")
		}
		identity := states[i].Identity()
		if _, duplicate := seen[identity]; duplicate {
			return Envelope{}, errors.New("kmtproto: State snapshot returned a duplicate identity")
		}
		seen[identity] = struct{}{}
		copyStates[i] = cloneStateObject(states[i])
	}
	sort.Slice(copyStates, func(i, j int) bool {
		if copyStates[i].Namespace != copyStates[j].Namespace {
			return copyStates[i].Namespace < copyStates[j].Namespace
		}
		return copyStates[i].ObjectID < copyStates[j].ObjectID
	})
	accumulator, err := newStateSnapshotAccumulator(s.config.Limits, snapshotLimits)
	if err != nil {
		return Envelope{}, err
	}
	for _, object := range copyStates {
		if err := accumulator.Add(object); err != nil {
			return Envelope{}, err
		}
	}
	snapshot.Payload, err = accumulator.Payload()
	if err != nil {
		return Envelope{}, err
	}
	if err := validateOutboundFrame(&snapshot, s.config.Limits, s.config.StrictValidation); err != nil {
		return Envelope{}, err
	}
	return snapshot, nil
}

func (s *Server) stateSnapshotLimits(frame Envelope) (StateSnapshotLimits, error) {
	probe := copyEnvelope(frame)
	probe.Payload = json.RawMessage(`{}`)
	encoded, err := json.Marshal(probe)
	if err != nil {
		return StateSnapshotLimits{}, err
	}
	overhead := len(encoded) - len(probe.Payload)
	maxBytes := minInt(s.config.Limits.MaxStateSnapshotBytes, s.config.Limits.MaxPayloadSize)
	maxBytes = minInt(maxBytes, s.config.Limits.MaxFrameSize-overhead)
	if maxBytes <= 0 {
		return StateSnapshotLimits{}, ErrStateSnapshotLimitExceeded
	}
	return StateSnapshotLimits{MaxObjects: s.config.Limits.MaxStateSnapshotObjects, MaxBytes: maxBytes}, nil
}

func (s *Server) errorFrame(sessionID, refID, code, message string, retryable bool) Envelope {
	if len(sessionID) > s.config.Limits.MaxSessionIDLength || !utf8.ValidString(sessionID) {
		sessionID = ""
	}
	if len(refID) > s.config.Limits.MaxIDLength || !utf8.ValidString(refID) {
		refID = ""
	}
	message = truncateUTF8(message, s.config.Limits.MaxErrorMessageLength)
	return Envelope{V: WireVersionV2, Type: FrameError, SessionID: sessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(ErrorPayload{Code: code, Message: message, RefID: refID, Retryable: retryable})}
}

func truncateUTF8(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(value) <= limit {
		return value
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

func (s *Server) enqueueError(outbound *OutboundQueue, sessionID, refID, code, message string, retryable bool) error {
	if err := s.enqueueFrame(outbound, s.errorFrame(sessionID, refID, code, message, retryable)); err != nil {
		return err
	}
	if behavior, ok := BehaviorForErrorCode(code); ok && behavior.CloseConnection {
		outbound.Close()
	}
	return nil
}

func (s *Server) enqueueFrame(outbound *OutboundQueue, frame Envelope) error {
	if err := validateOutboundFrame(&frame, s.config.Limits, s.config.StrictValidation); err != nil {
		return err
	}
	return outbound.Enqueue(frame)
}

func (s *Server) inject(point string) error {
	if s.config.FailureInjector == nil {
		return nil
	}
	return s.config.FailureInjector.Fail(point)
}

func (s *Server) acquireFlight(key string) (chan struct{}, bool) {
	s.flightMu.Lock()
	defer s.flightMu.Unlock()
	if done := s.flights[key]; done != nil {
		return done, false
	}
	done := make(chan struct{})
	s.flights[key] = done
	return done, true
}

func (s *Server) finishFlight(key string, done chan struct{}) {
	s.flightMu.Lock()
	if s.flights[key] == done {
		delete(s.flights, key)
		close(done)
	}
	s.flightMu.Unlock()
}

func (s *Server) runStream(sessionID string, fn func(*streamLane) error) error {
	s.laneMu.Lock()
	lane := s.lanes[sessionID]
	if lane == nil {
		lane = &streamLane{}
		s.lanes[sessionID] = lane
	}
	s.laneMu.Unlock()
	return lane.run(func() error { return fn(lane) })
}

func (l *streamLane) run(fn func() error) error {
	done := make(chan error, 1)
	l.mu.Lock()
	if l.callbackActive {
		l.mu.Unlock()
		return ErrStreamCallbackActive
	}
	l.queue = append(l.queue, streamRequest{fn: fn, done: done})
	if l.running {
		l.mu.Unlock()
		return <-done
	}
	l.running = true
	l.mu.Unlock()

	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.running = false
			l.mu.Unlock()
			break
		}
		request := l.queue[0]
		l.queue[0] = streamRequest{}
		l.queue = l.queue[1:]
		l.mu.Unlock()

		request.done <- invokeStreamRequest(request.fn)
		close(request.done)
	}
	return <-done
}

func (l *streamLane) invokeCallback(fn func() error) (err error) {
	l.mu.Lock()
	if l.callbackActive {
		l.mu.Unlock()
		return ErrStreamCallbackActive
	}
	l.callbackActive = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.callbackActive = false
		l.mu.Unlock()
	}()
	return fn()
}

func invokeStreamRequest(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("kmtproto: stream operation panicked: %v", recovered)
		}
	}()
	return fn()
}

// ServerConnection is a concurrency-safe reference admission gate with
// generation fencing. It does not own transport lifecycle or I/O.
type ServerConnection struct {
	mu           sync.Mutex
	generation   ConnectionGeneration
	outbound     *OutboundQueue
	state        ServerConnectionState
	sessionID    string
	capabilities SessionCapabilities
	handshake    bool
}

// ServerConnectionState is the state of the reference server-side connection
// gate. Transport ownership remains with the caller.
type ServerConnectionState uint8

const (
	ServerConnectionClosed ServerConnectionState = iota
	ServerConnectionAwaitingHandshake
	ServerConnectionReady
	ServerConnectionResuming
)

func (s ServerConnectionState) String() string {
	switch s {
	case ServerConnectionClosed:
		return "CLOSED"
	case ServerConnectionAwaitingHandshake:
		return "AWAITING_HANDSHAKE"
	case ServerConnectionReady:
		return "READY"
	case ServerConnectionResuming:
		return "RESUMING"
	default:
		return "UNKNOWN"
	}
}

// NewServerConnection creates a concurrency-safe reference admission gate. It
// does not own a transport, reader, writer, or connection registry.
func NewServerConnection() *ServerConnection { return &ServerConnection{} }

func (c *ServerConnection) Replace() (ConnectionGeneration, *OutboundQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outbound != nil {
		c.outbound.Close()
	}
	c.generation++
	c.outbound = NewOutboundQueue()
	c.state = ServerConnectionAwaitingHandshake
	c.sessionID = ""
	c.capabilities = SessionCapabilities{}
	c.handshake = false
	return c.generation, c.outbound
}

func (c *ServerConnection) State() ServerConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *ServerConnection) Generation() ConnectionGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *ServerConnection) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// Capabilities returns a defensive copy of the capabilities negotiated for
// the admitted logical Session on this connection generation.
func (c *ServerConnection) Capabilities() []NegotiatedCapability {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.List()
}

// CapabilityEnabled reports whether the admitted Session negotiated the named
// capability on this connection. It returns false before admission.
func (c *ServerConnection) CapabilityEnabled(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Enabled(name)
}

// CapabilityVersion returns the capability version enabled for the admitted
// Session on this connection.
func (c *ServerConnection) CapabilityVersion(name string) (version uint16, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Version(name)
}

func (c *ServerConnection) Handle(ctx context.Context, server *Server, generation ConnectionGeneration, frame Envelope) error {
	if server == nil {
		return errors.New("kmtproto: nil server")
	}
	c.mu.Lock()
	if generation != c.generation || c.outbound == nil {
		c.mu.Unlock()
		return ErrStaleConnection
	}
	outbound := c.outbound
	state := c.state
	sessionID := c.sessionID
	if state == ServerConnectionClosed {
		c.mu.Unlock()
		return ErrInvalidState
	}
	if err := ValidateFrame(&frame, server.config.Limits, server.config.StrictValidation); err != nil {
		c.mu.Unlock()
		result, handleErr := server.handleIncoming(ctx, frame, outbound)
		c.applyResult(generation, outbound, result)
		return handleErr
	}
	allowed := frame.Type == FrameError ||
		(state == ServerConnectionAwaitingHandshake && (frame.Type == FrameHello || frame.Type == FrameResume)) ||
		(state == ServerConnectionReady && (frame.Type == FramePing || frame.Type == FrameSend || frame.Type == FrameStateQuery || frame.Type == FrameResume))
	if !allowed || (state == ServerConnectionReady && frame.SessionID != sessionID) ||
		(state == ServerConnectionAwaitingHandshake && c.handshake) {
		c.state = ServerConnectionClosed
		c.capabilities = SessionCapabilities{}
		c.handshake = false
		c.mu.Unlock()
		return server.enqueueError(outbound, frame.SessionID, frame.ID, ErrorProtocolViolation, "frame is invalid for server connection state", false)
	}
	if state == ServerConnectionAwaitingHandshake && frame.Type != FrameError {
		c.handshake = true
	}
	if state == ServerConnectionReady && frame.Type == FrameResume {
		c.state = ServerConnectionResuming
	}
	c.mu.Unlock()

	result, err := server.handleIncoming(ctx, frame, outbound)
	c.applyResult(generation, outbound, result)
	return err
}

func (c *ServerConnection) applyResult(generation ConnectionGeneration, outbound *OutboundQueue, result serverHandleResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation || outbound != c.outbound {
		return
	}
	c.handshake = false
	if c.state == ServerConnectionClosed {
		return
	}
	if result.close {
		c.state = ServerConnectionClosed
		c.capabilities = SessionCapabilities{}
		return
	}
	if result.abandonSession {
		c.state = ServerConnectionAwaitingHandshake
		c.sessionID = ""
		c.capabilities = SessionCapabilities{}
		return
	}
	if result.readySessionID != "" {
		c.state = ServerConnectionReady
		c.sessionID = result.readySessionID
		c.capabilities = result.capabilities
		return
	}
	if c.state == ServerConnectionResuming {
		// Resume is successful only when the Server returns an explicit
		// readySessionID. Any other result leaves the connection unusable.
		c.state = ServerConnectionClosed
		c.capabilities = SessionCapabilities{}
	}
}
