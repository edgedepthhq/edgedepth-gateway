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
		if c, _ := s.Current(); c != nil {
			t.Fatalf("tick %d: Current leaked a priceless candle: %v", i, c)
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

	c, _ := s.Current()
	if c == nil {
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
	if c, _ := s.Current(); c == nil || c.Open != 65000 {
		t.Fatalf("flat continuation bucket should be emitted: %+v", c)
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
	if c, _ := s.Current(); c == nil || c.Open != 64050 {
		t.Fatalf("bucket after seed should open flat at seed close: %+v", c)
	}
}

// Stats must not ride on the candle flush. Mark price, funding and open
// interest come from the 30s REST poll, not from the tape, so a symbol that
// has not traded still has a stats panel worth filling. When the two shared
// one dirty flag the terminal's stats strip rendered "-" for every field even
// with a healthy trade feed, because Current withheld both whenever the candle
// was unpriced.
func TestStatsFlowWithoutATrade(t *testing.T) {
	mark, oi := 64000.0, 1234.0
	s := NewSeries(1, func() (float64, float64, float64, int64) {
		return mark, 0.0001, oi, 0
	})
	base := int64(1_000_000_000_000)

	s.Tick(base) // opens a bucket that no trade will ever price

	c, st := s.Current()
	if c != nil {
		t.Fatalf("unpriced candle leaked alongside the stat: %+v", c)
	}
	if st == nil {
		t.Fatal("stat withheld on an untraded symbol, which is the coupling bug")
	}
	if st.MarkPrice != 64000 || st.OpenInterestUsd != 1234 {
		t.Fatalf("stat did not carry the polled mark and open interest: %+v", st)
	}

	// Nothing moved, so nothing is worth resending at 10 Hz.
	if _, st := s.Current(); st != nil {
		t.Fatalf("unchanged stat resent: %+v", st)
	}

	// A fresh REST reading is.
	mark, oi = 64100, 1300
	_, st = s.Current()
	if st == nil || st.MarkPrice != 64100 || st.OpenInterestUsd != 1300 {
		t.Fatalf("mark refresh did not mark the stat dirty: %+v", st)
	}
}

// The flip side: a stat with nothing in it must stay off the wire, or the
// strip shows a 0.00 mark instead of the "-" that means "nothing yet". This is
// the cold-start window before the first REST poll returns.
func TestEmptyStatIsWithheld(t *testing.T) {
	s := NewSeries(1, func() (float64, float64, float64, int64) {
		return 0, 0, 0, 0
	})
	s.Tick(1_000_000_000_000)
	if _, st := s.Current(); st != nil {
		t.Fatalf("all-zero stat reached the wire: %+v", st)
	}
}
