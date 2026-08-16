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
var ErrSequenceExhausted = errors.New("kmtproto: event sequence exhausted")

// ServerSessionStore implementations must be safe for concurrent use.
// Claim is atomic for (sessionID, msgID). Once it returns claimed=true, no
// duplicate may claim the same identity until Complete or Abort (or an explicit
// store-specific crash-recovery procedure) resolves the PROCESSING record.
type ServerSessionStore interface {
	Claim(sessionID, msgID string) (claimed bool, existing *DedupRecord, err error)
	Complete(sessionID, msgID string, ack *Envelope) error
	Abort(sessionID, msgID string) error
}

type SessionRepository interface {
	Create(sessionID string, expiresAt time.Time) error
	Exists(sessionID string, now time.Time) (bool, error)
}

// ReplayStore implementations must be safe for concurrent use and must return
// original EVENT envelopes in strictly contiguous sequence order.
type ReplayStore interface {
	CurrentSeq(sessionID string) (uint64, error)
	Replay(sessionID string, afterSeq, throughSeq uint64) ([]Envelope, error)
}

type EventAppender interface {
	Append(sessionID, eventID, eventType string, content []byte, timestamp int64) (Envelope, error)
}

// ApplicationHandler may be called concurrently for different SEND identities.
// The idempotencyKey is the globally unique SEND ID. Applications that require
// end-to-end side-effect deduplication must durably honor that key.
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
	return &MemoryDedupStore{clock: clock, ttl: ttl, records: make(map[string]memoryDedupEntry)}
}

func dedupKey(sessionID, msgID string) string { return sessionID + "\x00" + msgID }

func (s *MemoryDedupStore) Claim(sessionID, msgID string) (bool, *DedupRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := dedupKey(sessionID, msgID)
	if entry, ok := s.records[key]; ok {
		if entry.record.State == DedupProcessing || s.clock.Now().Before(entry.expiresAt) {
			r := entry.record
			if r.Ack != nil {
				ack := copyEnvelope(*r.Ack)
				r.Ack = &ack
			}
			return false, &r, nil
		}
		delete(s.records, key)
	}
	s.records[key] = memoryDedupEntry{record: DedupRecord{State: DedupProcessing}, expiresAt: s.clock.Now().Add(s.ttl)}
	return true, nil, nil
}

func (s *MemoryDedupStore) Complete(sessionID, msgID string, ack *Envelope) error {
	if ack == nil {
		return errors.New("kmtproto: nil ACK")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := dedupKey(sessionID, msgID)
	entry, ok := s.records[key]
	if !ok || entry.record.State != DedupProcessing {
		return errors.New("kmtproto: message is not claimed")
	}
	copyAck := copyEnvelope(*ack)
	entry.record = DedupRecord{State: DedupCompleted, Ack: &copyAck}
	entry.expiresAt = s.clock.Now().Add(s.ttl)
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
	sessions map[string]time.Time
}

// NewMemorySessionRepository returns a process-local repository that is safe
// for concurrent use.
func NewMemorySessionRepository() *MemorySessionRepository {
	return &MemorySessionRepository{sessions: make(map[string]time.Time)}
}

func (s *MemorySessionRepository) Create(sessionID string, expiresAt time.Time) error {
	s.mu.Lock()
	s.sessions[sessionID] = expiresAt
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionRepository) Exists(sessionID string, now time.Time) (bool, error) {
	s.mu.RLock()
	expiresAt, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	return ok && now.Before(expiresAt), nil
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

func (s *MemoryReplayStore) Replay(sessionID string, afterSeq, throughSeq uint64) ([]Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if floor := s.floor[sessionID]; floor > 0 && afterSeq < floor-1 {
		return nil, ErrReplayUnavailable
	}
	var out []Envelope
	for _, event := range s.events[sessionID] {
		if event.Seq > afterSeq && event.Seq <= throughSeq {
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
	event := Envelope{V: WireVersionV1, Type: FrameEvent, ID: eventID, SessionID: sessionID, Seq: seq, Timestamp: timestamp, Payload: payload}
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
