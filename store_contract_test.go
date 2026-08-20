package kmtproto

import (
	"errors"
	"testing"
	"time"
)

func TestMemorySessionRepositoryRejectsCollision(t *testing.T) {
	repository := NewMemorySessionRepository()
	first := SessionState{SessionID: "s", ExpiresAt: time.Unix(3_000, 0)}
	if err := repository.Create(first); err != nil {
		t.Fatal(err)
	}
	second := SessionState{SessionID: "s", ExpiresAt: time.Unix(4_000, 0)}
	if err := repository.Create(second); !errors.Is(err, ErrSessionExists) {
		t.Fatalf("duplicate Create returned %v", err)
	}
	stored, exists, err := repository.Lookup("s", time.Unix(2_500, 0))
	if err != nil || !exists || !stored.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("collision replaced Session: %#v exists=%v err=%v", stored, exists, err)
	}
}

func TestProcessingDedupClaimDoesNotExpire(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	store := NewMemoryDedupStore(clock, time.Minute)
	claimed, _, err := store.Claim("s", "m", SendFingerprint{})
	if err != nil || !claimed {
		t.Fatalf("first claim: claimed=%v err=%v", claimed, err)
	}
	clock.Advance(2 * time.Minute)
	claimed, record, err := store.Claim("s", "m", SendFingerprint{})
	if err != nil || claimed || record == nil || record.State != DedupProcessing {
		t.Fatalf("active claim expired: claimed=%v record=%#v err=%v", claimed, record, err)
	}
}

func TestReplayHighWaterSurvivesFullPrune(t *testing.T) {
	store := NewMemoryReplayStore()
	first, err := store.Append("s", "e1", "test", []byte(`{}`), 0)
	if err != nil {
		t.Fatal(err)
	}
	store.PruneBefore("s", first.Seq+1)
	current, err := store.CurrentSeq("s")
	if err != nil || current != 1 {
		t.Fatalf("high-water decreased: current=%d err=%v", current, err)
	}
	second, err := store.Append("s", "e2", "test", []byte(`{}`), 0)
	if err != nil || second.Seq != 2 {
		t.Fatalf("sequence reused after prune: event=%#v err=%v", second, err)
	}
}

func TestMemoryReplayStoreRejectsInvalidContentWithoutPanic(t *testing.T) {
	store := NewMemoryReplayStore()
	if _, err := store.Append("s", "e", "test", []byte(`not-json`), 0); err == nil {
		t.Fatal("invalid replay content accepted")
	}
}
