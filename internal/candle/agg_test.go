package candle

import (
	"testing"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// A series that has never seen a trade must stay silent: no closed candles
// from Tick, nothing dirty from Current. Before this held, the gateway
// streamed all-zero candles from cold start and the chart autoscaled to a
// 0..70000 axis with the real price compressed into a line at the top.
func TestNoEmissionBeforeFirstTrade(t *testing.T) {
	s := NewSeries(1, nil)

	base := int64(1_000_000_000_000)
	for i := int64(0); i < 10; i++ {
		closed, closedStat := s.Tick(base + i*1000)
		if closed != nil || closedStat != nil {
			t.Fatalf("tick %d: closed a bucket with no trades ever seen: %v", i, closed)
		}
		if c, _, dirty := s.Current(); dirty || c != nil {
			t.Fatalf("tick %d: Current leaked a priceless candle: %v dirty=%v", i, c, dirty)
		}
	}
}

// The first trade starts emission, and the buckets it closes later must
// carry its price, not zeros.
func TestFirstTradeStartsEmission(t *testing.T) {
	s := NewSeries(1, nil)
	base := int64(1_000_000_000_000)

	s.Tick(base) // opens a priceless bucket
	if closed, _ := s.AddTrade(Trade{Price: 65000, Qty: 1, IsBuy: true, Timestamp: base + 1500}); closed != nil {
		t.Fatalf("rolling out of a priceless bucket must not emit it: %v", closed)
	}

	c, _, dirty := s.Current()
	if !dirty || c == nil {
		t.Fatal("trade did not mark the series dirty")
	}
	if c.Open != 65000 || c.Close != 65000 {
		t.Fatalf("first candle did not open at the trade price: %+v", c)
	}

	// An empty bucket after a real one closes flat at the previous close.
	closed, _ := s.Tick(base + 3000)
	if closed == nil || closed.Close != 65000 || !closed.Final {
		t.Fatalf("expected a final candle at 65000, got %+v", closed)
	}
	if c, _, dirty := s.Current(); !dirty || c == nil || c.Open != 65000 {
		t.Fatalf("flat continuation bucket should be emitted: %+v dirty=%v", c, dirty)
	}
}

// A series seeded from REST history is priced from the start.
func TestSeededSeriesEmits(t *testing.T) {
	s := NewSeries(60, nil)
	base := int64(1_000_000_000_000) - (int64(1_000_000_000_000) % 60_000)
	s.Seed(&pb.Candle{Open: 64000, High: 64100, Low: 63900, Close: 64050,
		TimestampMs: base, Timeframe: 60})

	closed, _ := s.Tick(base + 60_000)
	if closed == nil || closed.Close != 64050 {
		t.Fatalf("seeded bucket should close at the seed price: %+v", closed)
	}
	if c, _, dirty := s.Current(); !dirty || c == nil || c.Open != 64050 {
		t.Fatalf("bucket after seed should open flat at seed close: %+v dirty=%v", c, dirty)
	}
}
