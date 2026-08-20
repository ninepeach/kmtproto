package kmtproto

import (
	"bytes"
	"context"
	"crypto/sha256"
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

// ServerDependencies contains the required protocol-facing collaborators for
// ServerProtocol. Implementations decide their own persistence and runtime
// policies; ServerProtocol only relies on their documented contracts.
type ServerDependencies struct {
	Sessions    SessionRepository
	Dedup       ServerSessionStore
	Replay      ReplayStore
	Appender    EventAppender
	Application ApplicationHandler
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Clock:            RealClock{},
		Limits:           DefaultLimits(),
		SessionResumeTTL: 24 * time.Hour,
		ReplayTTL:        24 * time.Hour,
		DedupTTL:         24 * time.Hour,
		ClientRetryTTL:   time.Hour,
		StrictValidation: true,
		MaxReplayEvents:  DefaultMaxReplayEvents,
		MaxReplayBytes:   DefaultMaxReplayBytes,
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
	users          int // guarded by ServerProtocol.laneMu
}

type sendFlight struct {
	done           chan struct{}
	fingerprint    SendFingerprint
	callbackActive bool
	ack            *Envelope
}

// ErrStreamCallbackActive means a same-Session stream operation was attempted
// while that lane was executing an injected callback. Callers may retry after
// the callback returns. Failing fast prevents callback self-deadlock without
// weakening per-session serialization.
var ErrStreamCallbackActive = errors.New("kmtproto: session stream callback is active")

// ServerProtocol is a transport-independent server-side protocol frame
// processor/state machine. It is safe for concurrent use when its injected
// stores and application satisfy their concurrency contracts. It does not own
// network connections, open WebSocket/TCP/QUIC connections, perform transport
// I/O, or manage transport lifecycle; those responsibilities remain with the
// caller.
type ServerProtocol struct {
	config   ServerConfig
	sessions SessionRepository
	dedup    ServerSessionStore
	replay   ReplayStore
	appender EventAppender
	app      ApplicationHandler

	flightMu sync.Mutex
	flights  map[string]*sendFlight
	laneMu   sync.Mutex
	lanes    map[string]*streamLane
}

type serverHandleResult struct {
	readySessionID string
	capabilities   SessionCapabilities
	close          bool
	abandonSession bool
}

// NewServerProtocol creates a low-level server-side protocol frame processor.
// It does not create or own a network listener or connection. Normal transport
// integrations should pass inbound frames through ServerAdmission.
func NewServerProtocol(config ServerConfig, deps ServerDependencies) (*ServerProtocol, error) {
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
	if err := validateImplementedCapabilityRegistry(config.Capabilities); err != nil {
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
	if deps.Sessions == nil {
		return nil, errors.New("kmtproto: ServerDependencies.Sessions is required")
	}
	if deps.Dedup == nil {
		return nil, errors.New("kmtproto: ServerDependencies.Dedup is required")
	}
	if deps.Replay == nil {
		return nil, errors.New("kmtproto: ServerDependencies.Replay is required")
	}
	if deps.Appender == nil {
		return nil, errors.New("kmtproto: ServerDependencies.Appender is required")
	}
	if deps.Application == nil {
		return nil, errors.New("kmtproto: ServerDependencies.Application is required")
	}
	if reporter, ok := deps.Dedup.(DedupRetentionReporter); ok {
		retention := reporter.DedupRetentionTTL()
		if retention < config.ClientRetryTTL || retention < config.SessionResumeTTL {
			return nil, errors.New("kmtproto: dedup store retention must cover ClientRetryTTL and SessionResumeTTL")
		}
	}
	return &ServerProtocol{
		config:   config,
		sessions: deps.Sessions,
		dedup:    deps.Dedup,
		replay:   deps.Replay,
		appender: deps.Appender,
		app:      deps.Application,
		flights:  make(map[string]*sendFlight),
		lanes:    make(map[string]*streamLane),
	}, nil
}

// ProcessFrame processes one already-admitted protocol Frame. It is a low-level
// processor: it does not own a network connection, manage transport lifecycle,
// enforce HELLO-first connection state, or apply connection-generation
// fencing. Normal transport integrations should call ServerAdmission.Handle,
// which performs per-connection admission before invoking ServerProtocol.
func (s *ServerProtocol) ProcessFrame(ctx context.Context, frame Envelope, outbound *OutboundQueue) error {
	_, err := s.processFrame(ctx, frame, outbound)
	return err
}

func (s *ServerProtocol) processFrame(ctx context.Context, frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
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
		result := serverHandleResult{}
		err := s.handlePing(frame, outbound, &result)
		return result, err
	case FrameSend:
		result := serverHandleResult{}
		err := s.handleSend(ctx, frame, outbound, &result)
		return result, err
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

func (s *ServerProtocol) handleStateQuery(ctx context.Context, frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
	now := s.config.Clock.Now()
	timestamp := now.UnixMilli()
	result := serverHandleResult{}
	err := s.runStream(frame.SessionID, func(lane *streamLane) error {
		var session SessionState
		var exists bool
		if err := lane.invokeCallback(func() error {
			var lookupErr error
			session, exists, lookupErr = s.sessions.Lookup(frame.SessionID, now)
			return lookupErr
		}); err != nil {
			return err
		}
		if !exists {
			result.abandonSession = true
			return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInvalidSession, "session expired or unknown", false, timestamp)
		}
		if !stateSyncEnabled(session.Capabilities) {
			result.close = true
			return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorProtocolViolation, "STATE_QUERY requires state-sync capability version 1", false, timestamp)
		}
		if s.config.StateStore == nil {
			return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInternal, "State store is unavailable", true, timestamp)
		}
		var query StateQueryPayload
		if err := decodePayload(frame.Payload, &query, s.config.StrictValidation); err != nil {
			return err
		}
		snapshot := Envelope{
			V:         WireVersionV2,
			Type:      FrameStateSnapshot,
			ID:        frame.ID,
			SessionID: frame.SessionID,
			Timestamp: timestamp,
		}
		snapshotLimits, err := s.stateSnapshotLimits(snapshot)
		if err != nil {
			return err
		}
		accumulator, err := newStateSnapshotAccumulator(s.config.Limits, snapshotLimits)
		if err != nil {
			return err
		}
		objectIDs := append([]string(nil), query.ObjectIDs...)
		sortStrings(objectIDs)
		for _, objectID := range objectIDs {
			var object StateObject
			var found bool
			if err := lane.invokeCallback(func() error {
				var getErr error
				object, found, getErr = s.config.StateStore.Get(ctx, query.Namespace, objectID)
				return getErr
			}); err != nil {
				return err
			}
			if !found {
				continue
			}
			if object.Namespace != query.Namespace || object.ObjectID != objectID {
				return errors.New("kmtproto: StateStore returned the wrong object identity")
			}
			if err := ValidateStateObject(&object, s.config.Limits); err != nil {
				return fmt.Errorf("kmtproto: StateStore returned invalid State: %w", err)
			}
			if err := accumulator.Add(object); err != nil {
				if errors.Is(err, ErrStateSnapshotLimitExceeded) {
					return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorBadRequest, "State snapshot exceeds configured limit; split the query", false, timestamp)
				}
				return err
			}
		}
		snapshot.Payload, err = accumulator.Payload()
		if err != nil {
			if errors.Is(err, ErrStateSnapshotLimitExceeded) {
				return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorBadRequest, "State snapshot exceeds configured limit; split the query", false, timestamp)
			}
			return err
		}
		return s.enqueueFrame(outbound, snapshot)
	})
	return result, err
}

func (s *ServerProtocol) PublishEvent(sessionID, eventID, eventType string, content json.RawMessage, outbound *OutboundQueue) error {
	if outbound == nil || sessionID == "" || eventID == "" || !json.Valid(content) || !utf8.ValidString(eventID) {
		return NewProtocolError(ErrorBadRequest, "invalid event publication")
	}
	if len(eventID) > s.config.Limits.MaxIDLength || len(sessionID) > s.config.Limits.MaxSessionIDLength || len(content) > s.config.Limits.MaxPayloadSize {
		return NewProtocolError(ErrorBadRequest, "event publication exceeds protocol limits")
	}
	now := s.config.Clock.Now()
	contentCopy := append(json.RawMessage(nil), content...)
	probe := Envelope{V: WireVersionV2, Type: FrameEvent, ID: eventID, SessionID: sessionID, Seq: ^uint64(0), Timestamp: now.UnixMilli(), Payload: mustPayload(EventPayload{EventType: eventType, Content: contentCopy})}
	if err := validateOutboundFrame(&probe, s.config.Limits, s.config.StrictValidation); err != nil {
		return err
	}
	return s.runStream(sessionID, func(lane *streamLane) error {
		var exists bool
		err := lane.invokeCallback(func() error {
			_, found, lookupErr := s.sessions.Lookup(sessionID, now)
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
			event, appendErr = s.appender.Append(sessionID, eventID, eventType, append([]byte(nil), contentCopy...), now.UnixMilli())
			return appendErr
		})
		if err != nil {
			return err
		}
		if err := validateOutboundFrame(&event, s.config.Limits, s.config.StrictValidation); err != nil {
			return fmt.Errorf("kmtproto: EventAppender returned invalid EVENT: %w", err)
		}
		var payload EventPayload
		if err := decodePayload(event.Payload, &payload, s.config.StrictValidation); err != nil {
			return fmt.Errorf("kmtproto: EventAppender returned invalid EVENT payload: %w", err)
		}
		if event.Type != FrameEvent || event.SessionID != sessionID || event.ID != eventID ||
			payload.EventType != eventType || !bytes.Equal(payload.Content, contentCopy) {
			return errors.New("kmtproto: EventAppender returned an EVENT that does not match the append request")
		}
		return s.enqueueFrame(outbound, event)
	})
}

// PublishStateUpdate emits one already-committed complete State replacement.
// It neither writes StateStore nor participates in the EVENT stream or replay.
func (s *ServerProtocol) PublishStateUpdate(sessionID, updateID string, object StateObject, outbound *OutboundQueue) error {
	if outbound == nil {
		return errors.New("kmtproto: nil outbound queue")
	}
	if err := ValidateStateObject(&object, s.config.Limits); err != nil {
		return err
	}
	now := s.config.Clock.Now()
	return s.runStream(sessionID, func(lane *streamLane) error {
		var session SessionState
		var exists bool
		err := lane.invokeCallback(func() error {
			var lookupErr error
			session, exists, lookupErr = s.sessions.Lookup(sessionID, now)
			return lookupErr
		})
		if err != nil {
			return err
		}
		if !exists {
			return NewProtocolError(ErrorInvalidSession, "session expired or unknown")
		}
		if !stateSyncEnabled(session.Capabilities) {
			return NewProtocolError(ErrorProtocolViolation, "STATE_UPDATE requires state-sync capability version 1")
		}
		frame := Envelope{
			V:         WireVersionV2,
			Type:      FrameStateUpdate,
			ID:        updateID,
			SessionID: sessionID,
			Timestamp: now.UnixMilli(),
			Payload:   mustPayload(StateUpdatePayload{State: cloneStateObject(object)}),
		}
		return s.enqueueFrame(outbound, frame)
	})
}

func (s *ServerProtocol) handleHello(frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
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
		if errors.Is(err, ErrSessionExists) {
			return serverHandleResult{}, s.enqueueError(outbound, "", frame.ID, ErrorInternal, "cannot allocate a unique session", true)
		}
		return serverHandleResult{}, err
	}
	if err := s.enqueueFrame(outbound, welcome); err != nil {
		return serverHandleResult{}, err
	}
	return serverHandleResult{readySessionID: sessionID, capabilities: accepted}, nil
}

func (s *ServerProtocol) handlePing(frame Envelope, outbound *OutboundQueue, result *serverHandleResult) error {
	_, exists, err := s.sessions.Lookup(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return err
	}
	if !exists {
		result.abandonSession = true
		return s.enqueueError(outbound, frame.SessionID, "", ErrorInvalidSession, "session expired or unknown", false)
	}
	var ping PingPayload
	if err := decodePayload(frame.Payload, &ping, s.config.StrictValidation); err != nil {
		return err
	}
	pong := Envelope{V: WireVersionV2, Type: FramePong, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(PongPayload{PingID: ping.PingID, ClientTime: ping.ClientTime, ServerTime: s.config.Clock.Now().UnixMilli()})}
	return s.enqueueFrame(outbound, pong)
}

func (s *ServerProtocol) handleSend(ctx context.Context, frame Envelope, outbound *OutboundQueue, result *serverHandleResult) error {
	now := s.config.Clock.Now()
	timestamp := now.UnixMilli()
	var payload SendPayload
	if err := decodePayload(frame.Payload, &payload, s.config.StrictValidation); err != nil {
		return err
	}
	content := append([]byte(nil), payload.Content...)
	fingerprint := SendFingerprint(sha256.Sum256(content))
	key := dedupKey(frame.SessionID, frame.ID)
	flight, leader, conflict, callbackActive := s.acquireFlight(key, fingerprint)
	if conflict {
		return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorBadRequest, "SEND id was reused with different content", false, timestamp)
	}
	if !leader {
		if callbackActive {
			if ack := s.flightACK(flight); ack != nil {
				if err := s.validateStoredACK(*ack, frame.SessionID, frame.ID); err != nil {
					return err
				}
				return s.enqueueFrame(outbound, *ack)
			}
			return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInternal, "SEND callback is active; retry with the same id", true, timestamp)
		}
		return s.waitForOriginal(ctx, flight, frame, outbound, timestamp)
	}
	defer s.finishFlight(key, flight)

	var exists bool
	if err := s.invokeSendCallback(key, flight, func() error {
		_, found, lookupErr := s.sessions.Lookup(frame.SessionID, now)
		exists = found
		return lookupErr
	}); err != nil {
		return err
	}
	if !exists {
		result.abandonSession = true
		return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInvalidSession, "session expired or unknown", false, timestamp)
	}

	var claimed bool
	var existing *DedupRecord
	if err := s.invokeSendCallback(key, flight, func() error {
		var claimErr error
		claimed, existing, claimErr = s.dedup.Claim(frame.SessionID, frame.ID, fingerprint)
		return claimErr
	}); err != nil {
		return err
	}
	if !claimed {
		if existing != nil && existing.Fingerprint != fingerprint {
			return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorBadRequest, "SEND id was reused with different content", false, timestamp)
		}
		if existing != nil && existing.State == DedupCompleted && existing.Ack != nil {
			if err := s.validateStoredACK(*existing.Ack, frame.SessionID, frame.ID); err != nil {
				return err
			}
			s.setFlightACK(flight, *existing.Ack)
			return s.enqueueFrame(outbound, *existing.Ack)
		}
		return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInternal, "SEND is still processing; retry with the same id", true, timestamp)
	}

	if err := s.invokeSendCallback(key, flight, func() error { return s.inject(FailAfterClaim) }); err != nil {
		return err
	}
	if err := s.invokeSendCallback(key, flight, func() error {
		return s.app.HandleSend(ctx, frame.ID, append([]byte(nil), content...))
	}); err != nil {
		// Application failure may be an indeterminate commit. Leave the Claim in
		// PROCESSING so elapsed time or an ordinary retry cannot execute it again.
		return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInternal, "application rejected SEND", true, timestamp)
	}
	if err := s.invokeSendCallback(key, flight, func() error { return s.inject(FailAfterApplication) }); err != nil {
		return err
	}
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: frame.SessionID, Timestamp: timestamp, Payload: mustPayload(AckPayload{RefID: frame.ID})}
	if err := validateOutboundFrame(&ack, s.config.Limits, s.config.StrictValidation); err != nil {
		return err
	}
	if err := s.invokeSendCallback(key, flight, func() error {
		return s.dedup.Complete(frame.SessionID, frame.ID, &ack)
	}); err != nil {
		return err
	}
	s.setFlightACK(flight, ack)
	if err := s.invokeSendCallback(key, flight, func() error { return s.inject(FailAfterComplete) }); err != nil {
		return err
	}
	return s.enqueueFrame(outbound, ack)
}

func (s *ServerProtocol) waitForOriginal(ctx context.Context, flight *sendFlight, frame Envelope, outbound *OutboundQueue, timestamp int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flight.done:
	}
	ack := s.flightACK(flight)
	if ack != nil {
		if err := s.validateStoredACK(*ack, frame.SessionID, frame.ID); err != nil {
			return err
		}
		return s.enqueueFrame(outbound, *ack)
	}
	return s.enqueueErrorAt(outbound, frame.SessionID, frame.ID, ErrorInternal, "original SEND did not complete; retry", true, timestamp)
}

func (s *ServerProtocol) handleResume(ctx context.Context, frame Envelope, outbound *OutboundQueue) (serverHandleResult, error) {
	now := s.config.Clock.Now()
	timestamp := now.UnixMilli()
	var resume ResumePayload
	if err := decodePayload(frame.Payload, &resume, s.config.StrictValidation); err != nil {
		return serverHandleResult{}, err
	}
	state, exists, err := s.sessions.Lookup(frame.SessionID, now)
	if err != nil {
		return serverHandleResult{}, err
	}
	if !exists {
		return serverHandleResult{abandonSession: true}, s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorInvalidSession, "session expired or unknown", false, timestamp)
	}
	stateSnapshotID := ""
	if resume.StateSync != nil {
		if !stateSyncEnabled(state.Capabilities) {
			return serverHandleResult{close: true}, s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorUnsupportedFeature, "RESUME requires state-sync capability version 1", false, timestamp)
		}
		if s.config.StateSnapshots == nil {
			return serverHandleResult{close: true}, s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorStateUnavailable, "State snapshot provider is unavailable", true, timestamp)
		}
		stateSnapshotID, err = s.config.NewFrameID()
		if err != nil || stateSnapshotID == "" {
			return serverHandleResult{close: true}, s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorStateUnavailable, "State snapshot identity could not be produced", true, timestamp)
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
			result.close = true
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorInternal, "replay high-water could not be read", true, timestamp)
		}
		if resume.LastSeq > replayTo {
			result.close = true
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorProtocolViolation, "last_seq is ahead of server stream", false, timestamp)
		}
		if resume.LastSeq == math.MaxUint64 {
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorSyncRequired, "event sequence is exhausted", false, timestamp)
		}
		if replayTo-resume.LastSeq > s.config.MaxReplayEvents {
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorSyncRequired, "replay exceeds configured event limit", false, timestamp)
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
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorSyncRequired, "replay exceeds configured byte limit", false, timestamp)
		}
		if errors.Is(err, ErrReplayUnavailable) {
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorSyncRequired, "replay window no longer covers last_seq", false, timestamp)
		}
		if err != nil {
			result.close = true
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorInternal, "replay could not be produced", true, timestamp)
		}
		expectedSeq := resume.LastSeq + 1
		for i := range events {
			if err := validateOutboundFrame(&events[i], s.config.Limits, s.config.StrictValidation); err != nil {
				result.close = true
				return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorInternal, "replay store returned an invalid EVENT", true, timestamp)
			}
			if events[i].Type != FrameEvent || events[i].SessionID != frame.SessionID || events[i].Seq != expectedSeq {
				result.close = true
				return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorInternal, "replay store returned a non-contiguous event range", true, timestamp)
			}
			expectedSeq++
		}
		if expectedSeq != replayTo+1 {
			result.close = true
			return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorInternal, "replay store returned an incomplete event range", true, timestamp)
		}
		var stateSnapshot *Envelope
		if resume.StateSync != nil {
			snapshot, err := s.buildResumeStateSnapshot(ctx, lane, frame.SessionID, stateSnapshotID, timestamp, resume.StateSync.Namespaces)
			if err != nil {
				result.close = true
				return s.enqueueErrorAt(outbound, frame.SessionID, "", ErrorStateUnavailable, "State snapshot could not be produced", true, timestamp)
			}
			stateSnapshot = &snapshot
		}
		welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: frame.SessionID, Timestamp: timestamp, Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ServerTime: timestamp, ResumeFrom: resume.LastSeq + 1, ReplayTo: replayTo, StateSync: cloneResumeStateSync(resume.StateSync)})}
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

func (s *ServerProtocol) buildResumeStateSnapshot(ctx context.Context, lane *streamLane, sessionID, frameID string, timestamp int64, namespaces []string) (Envelope, error) {
	snapshot := Envelope{
		V:         WireVersionV2,
		Type:      FrameStateSnapshot,
		ID:        frameID,
		SessionID: sessionID,
		Timestamp: timestamp,
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

func (s *ServerProtocol) stateSnapshotLimits(frame Envelope) (StateSnapshotLimits, error) {
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

func (s *ServerProtocol) errorFrame(sessionID, refID, code, message string, retryable bool) Envelope {
	return s.errorFrameAt(sessionID, refID, code, message, retryable, s.config.Clock.Now().UnixMilli())
}

func (s *ServerProtocol) errorFrameAt(sessionID, refID, code, message string, retryable bool, timestamp int64) Envelope {
	if len(sessionID) > s.config.Limits.MaxSessionIDLength || !utf8.ValidString(sessionID) {
		sessionID = ""
	}
	if len(refID) > s.config.Limits.MaxIDLength || !utf8.ValidString(refID) {
		refID = ""
	}
	message = truncateUTF8(message, s.config.Limits.MaxErrorMessageLength)
	return Envelope{V: WireVersionV2, Type: FrameError, SessionID: sessionID, Timestamp: timestamp, Payload: mustPayload(ErrorPayload{Code: code, Message: message, RefID: refID, Retryable: retryable})}
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

func (s *ServerProtocol) enqueueError(outbound *OutboundQueue, sessionID, refID, code, message string, retryable bool) error {
	return s.enqueueErrorAt(outbound, sessionID, refID, code, message, retryable, s.config.Clock.Now().UnixMilli())
}

func (s *ServerProtocol) enqueueErrorAt(outbound *OutboundQueue, sessionID, refID, code, message string, retryable bool, timestamp int64) error {
	if err := s.enqueueFrame(outbound, s.errorFrameAt(sessionID, refID, code, message, retryable, timestamp)); err != nil {
		return err
	}
	if behavior, ok := BehaviorForErrorCode(code); ok && behavior.CloseConnection {
		outbound.Close()
	}
	return nil
}

func (s *ServerProtocol) enqueueFrame(outbound *OutboundQueue, frame Envelope) error {
	if err := validateOutboundFrame(&frame, s.config.Limits, s.config.StrictValidation); err != nil {
		return err
	}
	return outbound.Enqueue(frame)
}

func (s *ServerProtocol) validateStoredACK(ack Envelope, sessionID, messageID string) error {
	if err := validateOutboundFrame(&ack, s.config.Limits, s.config.StrictValidation); err != nil {
		return fmt.Errorf("kmtproto: dedup store returned invalid ACK: %w", err)
	}
	if ack.Type != FrameAck || ack.SessionID != sessionID {
		return errors.New("kmtproto: dedup store returned ACK for another SEND")
	}
	var payload AckPayload
	if err := decodePayload(ack.Payload, &payload, s.config.StrictValidation); err != nil {
		return fmt.Errorf("kmtproto: dedup store returned invalid ACK payload: %w", err)
	}
	if payload.RefID != messageID {
		return errors.New("kmtproto: dedup store returned ACK for another SEND")
	}
	return nil
}

func (s *ServerProtocol) inject(point string) error {
	if s.config.FailureInjector == nil {
		return nil
	}
	return s.config.FailureInjector.Fail(point)
}

func (s *ServerProtocol) acquireFlight(key string, fingerprint SendFingerprint) (flight *sendFlight, leader, conflict, callbackActive bool) {
	s.flightMu.Lock()
	defer s.flightMu.Unlock()
	if existing := s.flights[key]; existing != nil {
		return existing, false, existing.fingerprint != fingerprint, existing.callbackActive
	}
	flight = &sendFlight{done: make(chan struct{}), fingerprint: fingerprint}
	s.flights[key] = flight
	return flight, true, false, false
}

func (s *ServerProtocol) finishFlight(key string, flight *sendFlight) {
	s.flightMu.Lock()
	if s.flights[key] == flight {
		delete(s.flights, key)
		close(flight.done)
	}
	s.flightMu.Unlock()
}

func (s *ServerProtocol) invokeSendCallback(key string, flight *sendFlight, fn func() error) (err error) {
	s.flightMu.Lock()
	if s.flights[key] != flight || flight.callbackActive {
		s.flightMu.Unlock()
		return errors.New("kmtproto: SEND flight callback is already active")
	}
	flight.callbackActive = true
	s.flightMu.Unlock()
	defer func() {
		s.flightMu.Lock()
		flight.callbackActive = false
		s.flightMu.Unlock()
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("kmtproto: SEND dependency callback panicked: %v", recovered)
		}
	}()
	return fn()
}

func (s *ServerProtocol) setFlightACK(flight *sendFlight, ack Envelope) {
	s.flightMu.Lock()
	copyACK := copyEnvelope(ack)
	flight.ack = &copyACK
	s.flightMu.Unlock()
}

func (s *ServerProtocol) flightACK(flight *sendFlight) *Envelope {
	s.flightMu.Lock()
	defer s.flightMu.Unlock()
	if flight.ack == nil {
		return nil
	}
	ack := copyEnvelope(*flight.ack)
	return &ack
}

func (s *ServerProtocol) runStream(sessionID string, fn func(*streamLane) error) error {
	s.laneMu.Lock()
	lane := s.lanes[sessionID]
	if lane == nil {
		lane = &streamLane{}
		s.lanes[sessionID] = lane
	}
	lane.users++
	s.laneMu.Unlock()
	defer func() {
		s.laneMu.Lock()
		lane.users--
		if lane.users == 0 && s.lanes[sessionID] == lane {
			delete(s.lanes, sessionID)
		}
		s.laneMu.Unlock()
	}()
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

// ServerAdmission is a concurrency-safe per-connection protocol admission gate
// with generation fencing. It enforces HELLO-first, connection protocol state,
// Session correlation, and allowed-frame validation before invoking the
// low-level ServerProtocol processor. It tracks state for a caller-owned
// transport connection generation, but does not represent or own a network
// connection and performs no transport I/O; transport lifecycle remains
// caller-owned.
type ServerAdmission struct {
	mu           sync.Mutex
	generation   ConnectionGeneration
	outbound     *OutboundQueue
	state        ServerAdmissionState
	sessionID    string
	capabilities SessionCapabilities
	handshake    bool
}

// ServerAdmissionState is the state of the reference server-side protocol
// admission gate. Transport ownership and lifecycle remain with the caller.
type ServerAdmissionState uint8

const (
	ServerAdmissionClosed ServerAdmissionState = iota
	ServerAdmissionAwaitingHandshake
	ServerAdmissionReady
	ServerAdmissionResuming
)

func (s ServerAdmissionState) String() string {
	switch s {
	case ServerAdmissionClosed:
		return "CLOSED"
	case ServerAdmissionAwaitingHandshake:
		return "AWAITING_HANDSHAKE"
	case ServerAdmissionReady:
		return "READY"
	case ServerAdmissionResuming:
		return "RESUMING"
	default:
		return "UNKNOWN"
	}
}

// NewServerAdmission creates a concurrency-safe reference protocol admission
// gate. It does not own a transport, reader, writer, or connection registry.
func NewServerAdmission() *ServerAdmission { return &ServerAdmission{} }

func (c *ServerAdmission) Replace() (ConnectionGeneration, *OutboundQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outbound != nil {
		c.outbound.Close()
	}
	c.generation++
	c.outbound = NewOutboundQueue()
	c.state = ServerAdmissionAwaitingHandshake
	c.sessionID = ""
	c.capabilities = SessionCapabilities{}
	c.handshake = false
	return c.generation, c.outbound
}

func (c *ServerAdmission) State() ServerAdmissionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *ServerAdmission) Generation() ConnectionGeneration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *ServerAdmission) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// Capabilities returns a defensive copy of the capabilities negotiated for
// the admitted logical Session on this connection generation.
func (c *ServerAdmission) Capabilities() []NegotiatedCapability {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.List()
}

// CapabilityEnabled reports whether the admitted Session negotiated the named
// capability on this connection. It returns false before admission.
func (c *ServerAdmission) CapabilityEnabled(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Enabled(name)
}

// CapabilityVersion returns the capability version enabled for the admitted
// Session on this connection.
func (c *ServerAdmission) CapabilityVersion(name string) (version uint16, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capabilities.Version(name)
}

// Handle admits and processes one inbound Frame for the current caller-owned
// transport generation. It enforces HELLO-first connection protocol state,
// Session correlation, allowed-frame rules, and generation fencing before
// invoking ServerProtocol. It performs no network I/O and does not own the
// transport lifecycle.
func (c *ServerAdmission) Handle(ctx context.Context, server *ServerProtocol, generation ConnectionGeneration, frame Envelope) error {
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
	if state == ServerAdmissionClosed {
		c.mu.Unlock()
		return ErrInvalidState
	}
	if err := ValidateFrame(&frame, server.config.Limits, server.config.StrictValidation); err != nil {
		c.mu.Unlock()
		result, handleErr := invokeServerProcess(ctx, server, frame, outbound)
		c.applyResult(generation, outbound, result)
		return handleErr
	}
	allowed := frame.Type == FrameError ||
		(state == ServerAdmissionAwaitingHandshake && (frame.Type == FrameHello || frame.Type == FrameResume)) ||
		(state == ServerAdmissionReady && (frame.Type == FramePing || frame.Type == FrameSend || frame.Type == FrameStateQuery || frame.Type == FrameResume))
	if !allowed || (state == ServerAdmissionReady && frame.SessionID != sessionID) ||
		(state == ServerAdmissionAwaitingHandshake && c.handshake) {
		c.state = ServerAdmissionClosed
		c.capabilities = SessionCapabilities{}
		c.handshake = false
		c.mu.Unlock()
		return server.enqueueError(outbound, frame.SessionID, frame.ID, ErrorProtocolViolation, "frame is invalid for server connection state", false)
	}
	if state == ServerAdmissionAwaitingHandshake && frame.Type != FrameError {
		c.handshake = true
	}
	if state == ServerAdmissionReady && frame.Type == FrameResume {
		c.state = ServerAdmissionResuming
	}
	c.mu.Unlock()

	result, err := invokeServerProcess(ctx, server, frame, outbound)
	c.applyResult(generation, outbound, result)
	return err
}

func invokeServerProcess(ctx context.Context, server *ServerProtocol, frame Envelope, outbound *OutboundQueue) (result serverHandleResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			outbound.Close()
			result = serverHandleResult{close: true}
			err = fmt.Errorf("kmtproto: server dependency panicked: %v", recovered)
		}
	}()
	return server.processFrame(ctx, frame, outbound)
}

func (c *ServerAdmission) applyResult(generation ConnectionGeneration, outbound *OutboundQueue, result serverHandleResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation || outbound != c.outbound {
		return
	}
	c.handshake = false
	if c.state == ServerAdmissionClosed {
		return
	}
	if result.close {
		c.state = ServerAdmissionClosed
		c.capabilities = SessionCapabilities{}
		return
	}
	if result.abandonSession {
		c.state = ServerAdmissionAwaitingHandshake
		c.sessionID = ""
		c.capabilities = SessionCapabilities{}
		return
	}
	if result.readySessionID != "" {
		c.state = ServerAdmissionReady
		c.sessionID = result.readySessionID
		c.capabilities = result.capabilities
		return
	}
	if c.state == ServerAdmissionResuming {
		// Resume is successful only when ServerProtocol returns an explicit
		// readySessionID. Any other result leaves the connection unusable.
		c.state = ServerAdmissionClosed
		c.capabilities = SessionCapabilities{}
	}
}
