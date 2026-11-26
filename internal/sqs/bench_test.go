package sqs

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkReceiveDelete measures a consumer's steady state on a deep queue:
// receive ten, delete them, and send ten more so that the depth holds. With
// inFlight > 0 that many messages are received first and stay hidden, as they
// would with other consumers working on the same queue.
func BenchmarkReceiveDelete(b *testing.B) {
	for _, depth := range []int{10, 10_000, 100_000} {
		for _, inFlight := range []int{0, 1000} {
			if inFlight >= depth {
				continue
			}
			b.Run(fmt.Sprintf("depth=%d/inflight=%d", depth, inFlight), func(b *testing.B) {
				benchReceiveDelete(b, depth, inFlight)
			})
		}
	}
}

func benchReceiveDelete(b *testing.B, depth, inFlight int) {
	s, _ := newService(&testing.T{})
	if err := s.CreateQueue("bench", nil); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < depth; i++ {
		if _, err := s.Send("bench", SendInput{Body: "x"}); err != nil {
			b.Fatal(err)
		}
	}
	noWait, hidden := time.Duration(0), 12*time.Hour
	for got := 0; got < inFlight; {
		out, err := s.Receive(context.Background(), "bench", ReceiveInput{Max: 10, Wait: &noWait, Visibility: &hidden})
		if err != nil {
			b.Fatal(err)
		}
		got += len(out)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := s.Receive(ctx, "bench", ReceiveInput{Max: 10, Wait: &noWait})
		if err != nil || len(out) == 0 {
			b.Fatalf("Receive = %d messages, %v", len(out), err)
		}
		for _, m := range out {
			if err := s.Delete("bench", m.Receipt); err != nil {
				b.Fatal(err)
			}
			if _, err := s.Send("bench", SendInput{Body: "x"}); err != nil {
				b.Fatal(err)
			}
		}
	}
}
