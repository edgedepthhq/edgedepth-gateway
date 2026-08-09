package binance

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// TestLiveFeedEmits isolates Feed: does it turn Binance frames into pb
// messages? Bisects between Stream (known good) and Hub.
func TestLiveFeedEmits(t *testing.T) {
	if os.Getenv("EDGEDEPTH_LIVE") != "1" {
		t.Skip("set EDGEDEPTH_LIVE=1")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var mu sync.Mutex
	counts := map[pb.Stream]int{}

	f := NewFeed("btcusdt", log, func(stream pb.Stream, tf, evtMs int64, inner proto.Message) {
		mu.Lock()
		counts[stream]++
		n := counts[stream]
		mu.Unlock()
		if n <= 2 {
			t.Logf("emit %-24s %v", stream.String(), inner)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	t.Logf("emit counts: %v", counts)
	if counts[pb.Stream_STREAM_ORDERBOOK] < 2 {
		t.Errorf("expected ongoing orderbook diffs, got %d", counts[pb.Stream_STREAM_ORDERBOOK])
	}
	if counts[pb.Stream_STREAM_TRADES] == 0 {
		t.Errorf("expected trades, got 0")
	}
}
