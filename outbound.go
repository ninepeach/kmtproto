package kmtproto

import (
	"context"
	"errors"
	"sync"
)

var ErrOutboundClosed = errors.New("kmtproto: outbound queue closed")

// OutboundQueue is a concurrency-safe, process-local reference FIFO.
type OutboundQueue struct {
	mu     sync.Mutex
	wake   chan struct{}
	frames []Envelope
	closed bool
}

// NewOutboundQueue creates a process-local, concurrency-safe FIFO. The queue is
// intentionally unbounded and is a reference integration helper; production
// backpressure policy belongs to the caller's runtime.
func NewOutboundQueue() *OutboundQueue {
	return &OutboundQueue{wake: make(chan struct{}, 1)}
}

func (q *OutboundQueue) Enqueue(frame Envelope) error {
	return q.EnqueueBatch([]Envelope{frame})
}

func (q *OutboundQueue) EnqueueBatch(frames []Envelope) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrOutboundClosed
	}
	for _, frame := range frames {
		q.frames = append(q.frames, copyEnvelope(frame))
	}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}

func (q *OutboundQueue) Next(ctx context.Context) (Envelope, error) {
	for {
		q.mu.Lock()
		if len(q.frames) > 0 {
			frame := q.frames[0]
			q.frames[0] = Envelope{}
			q.frames = q.frames[1:]
			q.mu.Unlock()
			return frame, nil
		}
		if q.closed {
			q.mu.Unlock()
			return Envelope{}, ErrOutboundClosed
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case <-q.wake:
		}
	}
}

func (q *OutboundQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

type ByteSender interface {
	Send([]byte) error
}

// SingleWriter serializes one queue to one byte sender.
type SingleWriter struct {
	Queue  *OutboundQueue
	Codec  Codec
	Sender ByteSender
}

// Run drains Queue and invokes Sender serially. Exactly one Run call may be
// active for a SingleWriter; Queue itself may have concurrent producers.
func (w *SingleWriter) Run(ctx context.Context) error {
	if w.Queue == nil || w.Codec == nil || w.Sender == nil {
		return errors.New("kmtproto: incomplete writer configuration")
	}
	for {
		frame, err := w.Queue.Next(ctx)
		if err != nil {
			return err
		}
		data, err := w.Codec.Encode(&frame)
		if err != nil {
			return err
		}
		if err := w.Sender.Send(data); err != nil {
			return err
		}
	}
}
