package kmtproto

import (
	"encoding/json"
	"testing"
	"time"
)

func TestClockIsSampledOutsideClientAndDedupMutexes(t *testing.T) {
	clientClock := &mutexProbeClock{now: time.Unix(4_000, 0)}
	clientConfig := DefaultClientConfig()
	clientConfig.Clock = clientClock
	client, err := NewClientProtocol(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientClock.probe = func() bool {
		if !client.mu.TryLock() {
			return false
		}
		client.mu.Unlock()
		return true
	}
	generation := client.BeginConnect()
	if err := client.TransportConnected(generation); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartSession(generation, "clock-client"); err != nil {
		t.Fatal(err)
	}
	welcome := Envelope{V: WireVersionV2, Type: FrameWelcome, SessionID: "s_clock", Payload: mustPayload(WelcomePayload{Mode: WelcomeModeNew})}
	if _, err := client.HandleIncoming(generation, welcome); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnqueueSend("m", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if clientClock.lockHeld || clientClock.callCount == 0 {
		t.Fatalf("ClientProtocol sampled Clock under its mutex: held=%v calls=%d", clientClock.lockHeld, clientClock.callCount)
	}

	dedupClock := &mutexProbeClock{now: time.Unix(5_000, 0)}
	store := NewMemoryDedupStore(dedupClock, time.Hour)
	dedupClock.probe = func() bool {
		if !store.mu.TryLock() {
			return false
		}
		store.mu.Unlock()
		return true
	}
	if claimed, _, err := store.Claim("s", "m", SendFingerprint{}); err != nil || !claimed {
		t.Fatalf("Claim failed: claimed=%v err=%v", claimed, err)
	}
	ack := Envelope{V: WireVersionV2, Type: FrameAck, SessionID: "s", Payload: mustPayload(AckPayload{RefID: "m"})}
	if err := store.Complete("s", "m", &ack); err != nil {
		t.Fatal(err)
	}
	if dedupClock.lockHeld || dedupClock.callCount < 2 {
		t.Fatalf("dedup store sampled Clock under its mutex: held=%v calls=%d", dedupClock.lockHeld, dedupClock.callCount)
	}
}
