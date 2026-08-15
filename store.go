package kmtproto

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrReplayUnavailable = errors.New("kmtproto: replay range unavailable")

type ServerSessionStore interface {
	Claim(sessionID, msgID string) (claimed bool, existing *DedupRecord, err error)
	Complete(sessionID, msgID string, ack *Envelope) error
	Abort(sessionID, msgID string) error
}

type SessionRepository interface {
	Create(sessionID string, expiresAt time.Time) error
	Exists(sessionID string, now time.Time) (bool, error)
}

type ReplayStore interface {
	CurrentSeq(sessionID string) (uint64, error)
	Replay(sessionID string, afterSeq, throughSeq uint64) ([]Envelope, error)
}

type EventAppender interface {
	Append(sessionID, eventID, eventType string, content []byte, timestamp int64) (Envelope, error)
}

type ApplicationHandler interface {
	HandleSend(ctx context.Context, idempotencyKey string, payload []byte) error
}

type memoryDedupEntry struct {
	record    DedupRecord
	expiresAt time.Time
}

type MemoryDedupStore struct {
	mu      sync.Mutex
	clock   Clock
	ttl     time.Duration
	records map[string]memoryDedupEntry
}

func NewMemoryDedupStore(clock Clock, ttl time.Duration) *MemoryDedupStore {
	return &MemoryDedupStore{clock: clock, ttl: ttl, records: make(map[string]memoryDedupEntry)}
}

func dedupKey(sessionID, msgID string) string { return sessionID + "\x00" + msgID }

func (s *MemoryDedupStore) Claim(sessionID, msgID string) (bool, *DedupRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := dedupKey(sessionID, msgID)
	if entry, ok := s.records[key]; ok && s.clock.Now().Before(entry.expiresAt) {
		r := entry.record
		if r.Ack != nil {
			ack := copyEnvelope(*r.Ack)
			r.Ack = &ack
		}
		return false, &r, nil
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

type MemorySessionRepository struct {
	mu       sync.RWMutex
	sessions map[string]time.Time
}

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

type MemoryReplayStore struct {
	mu     sync.RWMutex
	events map[string][]Envelope
	floor  map[string]uint64
	ids    map[string]map[string]struct{}
}

func NewMemoryReplayStore() *MemoryReplayStore {
	return &MemoryReplayStore{events: make(map[string][]Envelope), floor: make(map[string]uint64), ids: make(map[string]map[string]struct{})}
}

func (s *MemoryReplayStore) CurrentSeq(sessionID string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := s.events[sessionID]
	if len(events) == 0 {
		return 0, nil
	}
	return events[len(events)-1].Seq, nil
}

func (s *MemoryReplayStore) Replay(sessionID string, afterSeq, throughSeq uint64) ([]Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if floor := s.floor[sessionID]; afterSeq+1 < floor {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[sessionID] == nil {
		s.ids[sessionID] = make(map[string]struct{})
	}
	if _, exists := s.ids[sessionID][eventID]; exists {
		return Envelope{}, errors.New("kmtproto: duplicate event id")
	}
	events := s.events[sessionID]
	seq := uint64(1)
	if len(events) > 0 {
		seq = events[len(events)-1].Seq + 1
	} else if s.floor[sessionID] > 1 {
		seq = s.floor[sessionID]
	}
	event := Envelope{V: WireVersionV1, Type: FrameEvent, ID: eventID, SessionID: sessionID, Seq: seq, Timestamp: timestamp, Payload: mustPayload(EventPayload{EventType: eventType, Content: append([]byte(nil), content...)})}
	s.events[sessionID] = append(s.events[sessionID], event)
	s.ids[sessionID][eventID] = struct{}{}
	return copyEnvelope(event), nil
}

func (s *MemoryReplayStore) PruneBefore(sessionID string, firstAvailableSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.events[sessionID]
	i := 0
	for i < len(events) && events[i].Seq < firstAvailableSeq {
		i++
	}
	s.events[sessionID] = append([]Envelope(nil), events[i:]...)
	s.floor[sessionID] = firstAvailableSeq
}
