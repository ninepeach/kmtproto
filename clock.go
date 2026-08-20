package kmtproto

import (
	"sync"
	"time"
)

// Clock is a concurrency-safe value provider. Implementations must return
// promptly and must not synchronously call back into the protocol object that
// invoked Now; protocol transitions may sample it while holding state locks.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a concurrency-safe deterministic test clock.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock creates a deterministic clock that is safe for concurrent use.
func NewFakeClock(now time.Time) *FakeClock { return &FakeClock{now: now} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}
