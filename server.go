package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	SessionResumeTTL time.Duration
	ReplayTTL        time.Duration
	DedupTTL         time.Duration
	ClientRetryTTL   time.Duration
	StrictValidation bool
	NewSessionID     func() (string, error)
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
		NewSessionID:     DefaultSessionIDGenerator(clock),
	}
}

type streamRequest struct {
	fn   func() error
	done chan error
}

type streamLane struct{ requests chan streamRequest }

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

func NewServer(config ServerConfig, sessions SessionRepository, dedup ServerSessionStore, replay ReplayStore, appender EventAppender, app ApplicationHandler) (*Server, error) {
	if config.Clock == nil {
		config.Clock = RealClock{}
	}
	if config.Limits.MaxFrameSize == 0 {
		config.Limits = DefaultLimits()
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
		return nil, errors.New("kmtproto: NewSessionID is required")
	}
	if sessions == nil || dedup == nil || replay == nil || appender == nil || app == nil {
		return nil, errors.New("kmtproto: server dependencies are required")
	}
	return &Server{config: config, sessions: sessions, dedup: dedup, replay: replay, appender: appender, app: app, flights: make(map[string]chan struct{}), lanes: make(map[string]*streamLane)}, nil
}

func (s *Server) HandleIncoming(ctx context.Context, frame Envelope, outbound *OutboundQueue) error {
	if outbound == nil {
		return errors.New("kmtproto: nil outbound queue")
	}
	if err := ValidateFrame(&frame, s.config.Limits, s.config.StrictValidation); err != nil {
		if frame.Type == FrameError {
			// Never create an ERROR-about-ERROR loop.
			outbound.Close()
			return nil
		}
		var pe *ProtocolError
		if errors.As(err, &pe) {
			_ = outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, pe.Code, pe.Message, pe.Retryable))
			if pe.Close {
				outbound.Close()
			}
			return nil
		}
		return err
	}

	switch frame.Type {
	case FrameHello:
		return s.handleHello(outbound)
	case FramePing:
		return s.handlePing(frame, outbound)
	case FrameSend:
		return s.handleSend(ctx, frame, outbound)
	case FrameResume:
		return s.handleResume(frame, outbound)
	case FrameError:
		// Never answer ERROR with ERROR.
		outbound.Close()
		return nil
	default:
		return outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, ErrorProtocolViolation, "unexpected client frame type", false))
	}
}

func (s *Server) PublishEvent(sessionID, eventID, eventType string, content json.RawMessage, outbound *OutboundQueue) error {
	if outbound == nil || sessionID == "" || eventID == "" || !json.Valid(content) || !utf8.ValidString(eventID) {
		return NewProtocolError(ErrorBadRequest, "invalid event publication")
	}
	if len(eventID) > s.config.Limits.MaxIDLength || len(sessionID) > s.config.Limits.MaxSessionIDLength || len(content) > s.config.Limits.MaxPayloadSize {
		return NewProtocolError(ErrorBadRequest, "event publication exceeds protocol limits")
	}
	return s.runStream(sessionID, func() error {
		exists, err := s.sessions.Exists(sessionID, s.config.Clock.Now())
		if err != nil {
			return err
		}
		if !exists {
			return NewProtocolError(ErrorInvalidSession, "session is not resumable")
		}
		event, err := s.appender.Append(sessionID, eventID, eventType, content, s.config.Clock.Now().UnixMilli())
		if err != nil {
			return err
		}
		return outbound.Enqueue(event)
	})
}

func (s *Server) handleHello(outbound *OutboundQueue) error {
	sessionID, err := s.config.NewSessionID()
	if err != nil || sessionID == "" {
		return outbound.Enqueue(s.errorFrame("", "", ErrorInternal, "cannot create session", true))
	}
	if len(sessionID) > s.config.Limits.MaxSessionIDLength {
		return fmt.Errorf("kmtproto: generated session id exceeds limit")
	}
	if err := s.sessions.Create(sessionID, s.config.Clock.Now().Add(s.config.SessionResumeTTL)); err != nil {
		return err
	}
	welcome := Envelope{V: WireVersionV1, Type: FrameWelcome, SessionID: sessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew, ServerTime: s.config.Clock.Now().UnixMilli()})}
	return outbound.Enqueue(welcome)
}

func (s *Server) handlePing(frame Envelope, outbound *OutboundQueue) error {
	exists, err := s.sessions.Exists(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return err
	}
	if !exists {
		return outbound.Enqueue(s.errorFrame(frame.SessionID, "", ErrorInvalidSession, "session expired or unknown", false))
	}
	var ping PingPayload
	if err := decodePayload(frame.Payload, &ping, s.config.StrictValidation); err != nil {
		return err
	}
	pong := Envelope{V: WireVersionV1, Type: FramePong, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(PongPayload{PingID: ping.PingID, ClientTime: ping.ClientTime, ServerTime: s.config.Clock.Now().UnixMilli()})}
	return outbound.Enqueue(pong)
}

func (s *Server) handleSend(ctx context.Context, frame Envelope, outbound *OutboundQueue) error {
	exists, err := s.sessions.Exists(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return err
	}
	if !exists {
		return outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, ErrorInvalidSession, "session expired or unknown", false))
	}
	claimed, existing, err := s.dedup.Claim(frame.SessionID, frame.ID)
	if err != nil {
		return err
	}
	key := dedupKey(frame.SessionID, frame.ID)
	if !claimed {
		if existing != nil && existing.State == DedupCompleted && existing.Ack != nil {
			return outbound.Enqueue(*existing.Ack)
		}
		return s.waitForOriginal(ctx, key, frame, outbound)
	}

	done := s.registerFlight(key)
	defer s.finishFlight(key, done)
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
		return outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, ErrorInternal, "application rejected SEND", true))
	}
	if err := s.inject(FailAfterApplication); err != nil {
		return err
	}
	ack := Envelope{V: WireVersionV1, Type: FrameAck, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(AckPayload{RefID: frame.ID})}
	if err := s.dedup.Complete(frame.SessionID, frame.ID, &ack); err != nil {
		return err
	}
	if err := s.inject(FailAfterComplete); err != nil {
		return err
	}
	return outbound.Enqueue(ack)
}

func (s *Server) waitForOriginal(ctx context.Context, key string, frame Envelope, outbound *OutboundQueue) error {
	s.flightMu.Lock()
	done := s.flights[key]
	s.flightMu.Unlock()
	if done == nil {
		return outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, ErrorInternal, "SEND is still processing; retry with the same id", true))
	}
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
		return outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, ErrorInternal, "original SEND did not complete; retry", true))
	}
	if existing != nil && existing.State == DedupCompleted && existing.Ack != nil {
		return outbound.Enqueue(*existing.Ack)
	}
	return outbound.Enqueue(s.errorFrame(frame.SessionID, frame.ID, ErrorInternal, "original SEND did not complete; retry", true))
}

func (s *Server) handleResume(frame Envelope, outbound *OutboundQueue) error {
	var resume ResumePayload
	if err := decodePayload(frame.Payload, &resume, s.config.StrictValidation); err != nil {
		return err
	}
	exists, err := s.sessions.Exists(frame.SessionID, s.config.Clock.Now())
	if err != nil {
		return err
	}
	if !exists {
		return outbound.Enqueue(s.errorFrame(frame.SessionID, "", ErrorInvalidSession, "session expired or unknown", false))
	}
	return s.runStream(frame.SessionID, func() error {
		replayTo, err := s.replay.CurrentSeq(frame.SessionID)
		if err != nil {
			return err
		}
		if resume.LastSeq > replayTo {
			return outbound.Enqueue(s.errorFrame(frame.SessionID, "", ErrorProtocolViolation, "last_seq is ahead of server stream", false))
		}
		events, err := s.replay.Replay(frame.SessionID, resume.LastSeq, replayTo)
		if errors.Is(err, ErrReplayUnavailable) {
			return outbound.Enqueue(s.errorFrame(frame.SessionID, "", ErrorSyncRequired, "replay window no longer covers last_seq", false))
		}
		if err != nil {
			return err
		}
		welcome := Envelope{V: WireVersionV1, Type: FrameWelcome, SessionID: frame.SessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(WelcomePayload{Mode: WelcomeModeResumed, ServerTime: s.config.Clock.Now().UnixMilli(), ResumeFrom: resume.LastSeq + 1, ReplayTo: replayTo})}
		batch := make([]Envelope, 0, len(events)+1)
		batch = append(batch, welcome)
		batch = append(batch, events...)
		return outbound.EnqueueBatch(batch)
	})
}

func (s *Server) errorFrame(sessionID, refID, code, message string, retryable bool) Envelope {
	return Envelope{V: WireVersionV1, Type: FrameError, SessionID: sessionID, Timestamp: s.config.Clock.Now().UnixMilli(), Payload: mustPayload(ErrorPayload{Code: code, Message: message, RefID: refID, Retryable: retryable})}
}

func (s *Server) inject(point string) error {
	if s.config.FailureInjector == nil {
		return nil
	}
	return s.config.FailureInjector.Fail(point)
}

func (s *Server) registerFlight(key string) chan struct{} {
	done := make(chan struct{})
	s.flightMu.Lock()
	s.flights[key] = done
	s.flightMu.Unlock()
	return done
}

func (s *Server) finishFlight(key string, done chan struct{}) {
	s.flightMu.Lock()
	if s.flights[key] == done {
		delete(s.flights, key)
		close(done)
	}
	s.flightMu.Unlock()
}

func (s *Server) runStream(sessionID string, fn func() error) error {
	s.laneMu.Lock()
	lane := s.lanes[sessionID]
	if lane == nil {
		lane = &streamLane{requests: make(chan streamRequest)}
		s.lanes[sessionID] = lane
		go func() {
			for request := range lane.requests {
				request.done <- request.fn()
				close(request.done)
			}
		}()
	}
	s.laneMu.Unlock()
	done := make(chan error, 1)
	lane.requests <- streamRequest{fn: fn, done: done}
	return <-done
}

type ServerConnection struct {
	mu         sync.Mutex
	generation ConnectionGeneration
	outbound   *OutboundQueue
}

func NewServerConnection() *ServerConnection { return &ServerConnection{} }

func (c *ServerConnection) Replace() (ConnectionGeneration, *OutboundQueue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outbound != nil {
		c.outbound.Close()
	}
	c.generation++
	c.outbound = NewOutboundQueue()
	return c.generation, c.outbound
}

func (c *ServerConnection) Handle(ctx context.Context, server *Server, generation ConnectionGeneration, frame Envelope) error {
	c.mu.Lock()
	if generation != c.generation || c.outbound == nil {
		c.mu.Unlock()
		return ErrStaleConnection
	}
	outbound := c.outbound
	c.mu.Unlock()
	return server.HandleIncoming(ctx, frame, outbound)
}
