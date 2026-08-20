package kmtproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

var ErrReplayUnavailable = errors.New("kmtproto: replay range unavailable")
var ErrReplayLimitExceeded = errors.New("kmtproto: replay range exceeds configured limits")
var ErrSequenceExhausted = errors.New("kmtproto: event sequence exhausted")
var ErrStateSnapshotLimitExceeded = errors.New("kmtproto: State snapshot exceeds configured limits")

// ReplayLimits are hard safety ceilings applied by ReplayStore before it
// materializes a replay result. MaxBytes counts encoded EVENT Envelopes.
type ReplayLimits struct {
	MaxEvents uint64
	MaxBytes  int
}

// StateSnapshotLimits are hard safety ceilings applied by a snapshot provider
// while it constructs a result. MaxBytes counts the encoded snapshot payload.
type StateSnapshotLimits struct {
	MaxObjects int
	MaxBytes   int
}

// ServerSessionStore implementations must be safe for concurrent use.
// Claim is atomic for (sessionID, msgID) and binds that identity to fingerprint.
// A returned existing record must preserve the originally claimed fingerprint.
// Once Claim returns claimed=true, no duplicate may claim the same identity
// until Complete or Abort (or an explicit store-specific crash-recovery
// procedure) resolves the PROCESSING record. Completed records must be retained
// for at least the configured client retry and Session Resume windows.
type ServerSessionStore interface {
	Claim(sessionID, msgID string, fingerprint SendFingerprint) (claimed bool, existing *DedupRecord, err error)
	Complete(sessionID, msgID string, ack *Envelope) error
	Abort(sessionID, msgID string) error
}

// DedupRetentionReporter lets a store expose its completed-record retention so
// NewServerProtocol can verify it against configured retry and Resume windows. Stores
// that do not implement this interface still MUST satisfy ServerSessionStore's
// retention contract.
type DedupRetentionReporter interface {
	DedupRetentionTTL() time.Duration
}

// SessionState is protocol metadata retained for one resumable Session.
// Capabilities are an immutable negotiated snapshot, not application state.
type SessionState struct {
	SessionID    string
	ExpiresAt    time.Time
	Capabilities SessionCapabilities
}

// CapabilityEnabled reports whether this logical Session has the named
// protocol capability enabled.
func (s SessionState) CapabilityEnabled(name string) bool {
	return s.Capabilities.Enabled(name)
}

// CapabilityVersion returns the capability version enabled for this Session.
func (s SessionState) CapabilityVersion(name string) (uint16, bool) {
	return s.Capabilities.Version(name)
}

// SessionRepository implementations must be safe for concurrent use. Create
// must atomically reject an existing Session ID with ErrSessionExists. The
// interface describes process-local protocol semantics only; implementations
// decide their own persistence and distributed-safety guarantees.
type SessionRepository interface {
	Create(state SessionState) error
	Lookup(sessionID string, now time.Time) (state SessionState, exists bool, err error)
}

// StateStore is the protocol-facing atomic State contract. Implementations
// must be safe for concurrent use, return defensive StateObject copies, and
// atomically enforce ApplyStateObject semantics per identity. The interface
// does not imply persistence, database transactions, or multi-process safety.
type StateStore interface {
	Get(ctx context.Context, namespace, objectID string) (object StateObject, found bool, err error)
	Apply(ctx context.Context, incoming StateObject) (committed StateObject, result StateApplyResult, err error)
}

// StateSnapshotProvider returns one authoritative, internally consistent
// process-local protocol snapshot for the requested namespaces. It must enforce
// limits while materializing the result, be safe for concurrent use, and return
// defensive StateObject copies. Storage, persistence, and distributed snapshot
// coordination are caller concerns.
type StateSnapshotProvider interface {
	Snapshot(ctx context.Context, namespaces []string, limits StateSnapshotLimits) ([]StateObject, error)
}

// ReplayStore implementations must enforce limits while materializing a result,
// be safe for concurrent use, and return original EVENT envelopes in strictly
// contiguous sequence order.
type ReplayStore interface {
	CurrentSeq(sessionID string) (uint64, error)
	Replay(sessionID string, afterSeq, throughSeq uint64, limits ReplayLimits) ([]Envelope, error)
}

// EventAppender atomically appends exactly the requested EVENT and returns its
// original replayable Envelope. The returned Session ID, event ID, event type,
// and content must match the request; the sequence must be the next positive
// Session stream position. Implementations must be safe for concurrent use.
type EventAppender interface {
	Append(sessionID, eventID, eventType string, content []byte, timestamp int64) (Envelope, error)
}

// ApplicationHandler may be called concurrently for different SEND identities.
// The idempotencyKey is the globally unique SEND ID. Applications that require
// end-to-end side-effect deduplication must durably honor that key. A returned
// error is treated as an indeterminate commit and leaves the protocol Claim in
// PROCESSING; ordinary retries do not execute the Application again.
type ApplicationHandler interface {
	HandleSend(ctx context.Context, idempotencyKey string, payload []byte) error
}

type memoryDedupEntry struct {
	record    DedupRecord
	expiresAt time.Time
}

// MemoryDedupStore is a concurrency-safe, process-local reference store.
type MemoryDedupStore struct {
	mu      sync.Mutex
	clock   Clock
	ttl     time.Duration
	records map[string]memoryDedupEntry
}

// NewMemoryDedupStore returns a process-local deduplication store. The returned
// store is safe for concurrent use. A PROCESSING claim does not expire: the
// claim owner must call Complete or Abort. TTL applies after completion.
func NewMemoryDedupStore(clock Clock, ttl time.Duration) *MemoryDedupStore {
	if clock == nil {
		clock = RealClock{}
	}
	return &MemoryDedupStore{clock: clock, ttl: ttl, records: make(map[string]memoryDedupEntry)}
}

func (s *MemoryDedupStore) DedupRetentionTTL() time.Duration {
	if s == nil {
		return 0
	}
	return s.ttl
}

func dedupKey(sessionID, msgID string) string { return sessionID + "\x00" + msgID }

func (s *MemoryDedupStore) Claim(sessionID, msgID string, fingerprint SendFingerprint) (bool, *DedupRecord, error) {
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := dedupKey(sessionID, msgID)
	if entry, ok := s.records[key]; ok {
		if entry.record.State == DedupProcessing || now.Before(entry.expiresAt) {
			r := entry.record
			if r.Ack != nil {
				ack := copyEnvelope(*r.Ack)
				r.Ack = &ack
			}
			return false, &r, nil
		}
		delete(s.records, key)
	}
	s.records[key] = memoryDedupEntry{record: DedupRecord{State: DedupProcessing, Fingerprint: fingerprint}, expiresAt: now.Add(s.ttl)}
	return true, nil, nil
}

func (s *MemoryDedupStore) Complete(sessionID, msgID string, ack *Envelope) error {
	if ack == nil {
		return errors.New("kmtproto: nil ACK")
	}
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := dedupKey(sessionID, msgID)
	entry, ok := s.records[key]
	if !ok || entry.record.State != DedupProcessing {
		return errors.New("kmtproto: message is not claimed")
	}
	copyAck := copyEnvelope(*ack)
	entry.record = DedupRecord{State: DedupCompleted, Fingerprint: entry.record.Fingerprint, Ack: &copyAck}
	entry.expiresAt = now.Add(s.ttl)
	s.records[key] = entry
	return nil
}

func (s *MemoryDedupStore) Abort(sessionID, msgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := dedupKey(sessionID, msgID)
	if entry, ok := s.records[key]; ok && entry.record.State == DedupProcessing {
		delete(s.records, key)
	}
	return nil
}

// MemorySessionRepository is safe for concurrent use.
type MemorySessionRepository struct {
	mu       sync.RWMutex
	sessions map[string]SessionState
}

// NewMemorySessionRepository returns a process-local repository that is safe
// for concurrent use.
func NewMemorySessionRepository() *MemorySessionRepository {
	return &MemorySessionRepository{sessions: make(map[string]SessionState)}
}

var ErrSessionExists = errors.New("kmtproto: session already exists")

func (s *MemorySessionRepository) Create(state SessionState) error {
	if state.SessionID == "" || state.ExpiresAt.IsZero() {
		return errors.New("kmtproto: invalid session state")
	}
	s.mu.Lock()
	if _, exists := s.sessions[state.SessionID]; exists {
		s.mu.Unlock()
		return ErrSessionExists
	}
	s.sessions[state.SessionID] = state
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionRepository) Lookup(sessionID string, now time.Time) (SessionState, bool, error) {
	s.mu.RLock()
	state, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok || !now.Before(state.ExpiresAt) {
		return SessionState{}, false, nil
	}
	return state, true, nil
}

// MemoryReplayStore is a concurrency-safe, process-local reference store.
type MemoryReplayStore struct {
	mu      sync.RWMutex
	events  map[string][]Envelope
	floor   map[string]uint64
	current map[string]uint64
	ids     map[string]map[string]struct{}
}

// NewMemoryReplayStore returns a process-local replay store that is safe for
// concurrent use. Pruning retained events never resets a session's sequence
// high-water mark.
func NewMemoryReplayStore() *MemoryReplayStore {
	return &MemoryReplayStore{
		events:  make(map[string][]Envelope),
		floor:   make(map[string]uint64),
		current: make(map[string]uint64),
		ids:     make(map[string]map[string]struct{}),
	}
}

func (s *MemoryReplayStore) CurrentSeq(sessionID string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current[sessionID], nil
}

func (s *MemoryReplayStore) Replay(sessionID string, afterSeq, throughSeq uint64, limits ReplayLimits) ([]Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limits.MaxEvents == 0 || limits.MaxBytes <= 0 || throughSeq < afterSeq {
		return nil, ErrReplayLimitExceeded
	}
	if throughSeq-afterSeq > limits.MaxEvents {
		return nil, ErrReplayLimitExceeded
	}
	if floor := s.floor[sessionID]; floor > 0 && afterSeq < floor-1 {
		return nil, ErrReplayUnavailable
	}
	var out []Envelope
	totalBytes := 0
	for _, event := range s.events[sessionID] {
		if event.Seq > afterSeq && event.Seq <= throughSeq {
			encoded, err := json.Marshal(event)
			if err != nil {
				return nil, fmt.Errorf("kmtproto: encode replay event: %w", err)
			}
			if len(encoded) > limits.MaxBytes-totalBytes {
				return nil, ErrReplayLimitExceeded
			}
			totalBytes += len(encoded)
			out = append(out, copyEnvelope(event))
		}
	}
	if throughSeq > afterSeq && uint64(len(out)) != throughSeq-afterSeq {
		return nil, ErrReplayUnavailable
	}
	return out, nil
}

func (s *MemoryReplayStore) Append(sessionID, eventID, eventType string, content []byte, timestamp int64) (Envelope, error) {
	if sessionID == "" || eventID == "" || !utf8.ValidString(sessionID) || !utf8.ValidString(eventID) || !utf8.ValidString(eventType) || !json.Valid(content) {
		return Envelope{}, errors.New("kmtproto: invalid event append")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[sessionID] == nil {
		s.ids[sessionID] = make(map[string]struct{})
	}
	if _, exists := s.ids[sessionID][eventID]; exists {
		return Envelope{}, errors.New("kmtproto: duplicate event id")
	}
	if s.current[sessionID] == math.MaxUint64 {
		return Envelope{}, ErrSequenceExhausted
	}
	seq := s.current[sessionID] + 1
	payload, err := json.Marshal(EventPayload{EventType: eventType, Content: append([]byte(nil), content...)})
	if err != nil {
		return Envelope{}, fmt.Errorf("kmtproto: encode event payload: %w", err)
	}
	event := Envelope{V: WireVersionV2, Type: FrameEvent, ID: eventID, SessionID: sessionID, Seq: seq, Timestamp: timestamp, Payload: payload}
	s.events[sessionID] = append(s.events[sessionID], event)
	s.current[sessionID] = seq
	s.ids[sessionID][eventID] = struct{}{}
	return copyEnvelope(event), nil
}

func (s *MemoryReplayStore) PruneBefore(sessionID string, firstAvailableSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.current[sessionID]; current != math.MaxUint64 && firstAvailableSeq > current+1 {
		firstAvailableSeq = current + 1
	}
	events := s.events[sessionID]
	i := 0
	for i < len(events) && events[i].Seq < firstAvailableSeq {
		i++
	}
	s.events[sessionID] = append([]Envelope(nil), events[i:]...)
	if firstAvailableSeq > s.floor[sessionID] {
		s.floor[sessionID] = firstAvailableSeq
	}
}
