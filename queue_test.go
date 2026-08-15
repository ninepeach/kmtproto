package kmtproto

import (
	"context"
	"sync"
	"testing"
)

func TestOutboundBatchCannotInterleave(t *testing.T) {
	queue := NewOutboundQueue()
	var start sync.WaitGroup
	start.Add(1)
	done := make(chan struct{})
	go func() {
		start.Wait()
		_ = queue.EnqueueBatch([]Envelope{{ID: "a1"}, {ID: "a2"}, {ID: "a3"}})
		close(done)
	}()
	start.Done()
	<-done
	if err := queue.Enqueue(Envelope{ID: "live"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a1", "a2", "a3", "live"} {
		got, err := queue.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want {
			t.Fatalf("got %q, want %q", got.ID, want)
		}
	}
}
