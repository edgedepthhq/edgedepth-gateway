package binance

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// TestLiveStreamDelivers isolates the Stream type: connect to Binance and
// count frames. If this passes and the hub probe does not, the fault is above
// this layer.
func TestLiveStreamDelivers(t *testing.T) {
	if os.Getenv("EDGEDEPTH_LIVE") != "1" {
		t.Skip("set EDGEDEPTH_LIVE=1")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var n atomic.Int64
	names := []string{"btcusdt@aggTrade", "btcusdt@depth@100ms", "btcusdt@markPrice@1s", "btcusdt@forceOrder"}
	s := NewStream(names, log, func(env Envelope) {
		if n.Add(1) <= 3 {
			t.Logf("frame: stream=%s bytes=%d", env.Stream, len(env.Data))
		}
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s.Run(ctx)

	if got := n.Load(); got == 0 {
		t.Fatalf("Stream delivered no frames in 15s")
	} else {
		t.Logf("Stream delivered %d frames", got)
	}
}
