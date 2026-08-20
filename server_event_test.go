package kmtproto

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventAppenderResultMustMatchRequest(t *testing.T) {
	clock := NewFakeClock(time.Unix(2_100, 0))
	config := DefaultServerConfig()
	config.Clock = clock
	config.NewSessionID = func() (string, error) { return "s_1", nil }
	replay := NewMemoryReplayStore()
	server, err := NewServerProtocol(config, ServerDependencies{
		Sessions:    NewMemorySessionRepository(),
		Dedup:       NewMemoryDedupStore(clock, config.DedupTTL),
		Replay:      replay,
		Appender:    mismatchedEventAppender{},
		Application: &recordingApp{},
	})
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, server)
	if err := server.PublishEvent("s_1", "e", "message", json.RawMessage(`{}`), NewOutboundQueue()); err == nil {
		t.Fatal("mismatched EventAppender result was accepted")
	}
}
