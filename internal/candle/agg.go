// Package candle builds candles and stats from the raw trade stream.
//
// This is where the gateway earns its keep. Binance's smallest kline is one
// minute, so 1s/5s/15s/30s candles cannot be fetched, only constructed from
// individual trades. The terminal renders them because its entitlements
// default to Pro when no host globals are present, which means a local
// gateway gets second-resolution candles that no free hosted product offers.
package candle

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Trade is the minimal trade shape the aggregator needs.
type Trade struct {
	Price     float64
	Qty       float64
	IsBuy     bool
	Timestamp int64 // ms
}

// Liquidation feeds the liquidation totals carried on Stat.
type Liquidation struct {
	Price     float64
	Qty       float64
	IsBuy     bool // true = a SHORT was liquidated (exchange market-buys to close)
	Timestamp int64
}

// MarkFn supplies the latest mark price, funding rate, open interest (in base
// asset) and next funding time. Stats are sampled from it at bucket close.
type MarkFn func() (mark, funding, oi float64, nextFunding int64)

// Series aggregates one symbol at one timeframe.
//
// It is deliberately not self-flushing: the owner drives Tick, so every
// timeframe for a symbol advances on one clock and emissions stay batched.
type Series struct {
	TfSec int64

	mu      sync.Mutex
	candle  *pb.Candle
	stat    *pb.Stat
	bucket  int64 // current bucket start, ms
	dirty   bool
	markFn  MarkFn
	started bool
}

// NewSeries creates an aggregator for one timeframe, in seconds.
func NewSeries(tfSec int64, markFn MarkFn) *Series {
	return &Series{TfSec: tfSec, markFn: markFn}
}

func (s *Series) bucketOf(tsMs int64) int64 {
	w := s.TfSec * 1000
	return tsMs - (tsMs % w)
}

// Seed installs historical candles as the starting point so a series that was
// backfilled from REST does not restart its first live candle from zero.
func (s *Series) Seed(last *pb.Candle) {
	if last == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.bucket = last.TimestampMs
	s.candle = last
	// stat must be initialised alongside candle: started=true unlocks
	// AddLiquidation, which writes into it unguarded.
	s.stat = &pb.Stat{TimestampMs: last.TimestampMs, Timeframe: s.TfSec}
	s.started = true
}

// AddTrade folds a trade into the current bucket. Any completed bucket is
// returned so the caller can emit it with final=true.
func (s *Series) AddTrade(t Trade) (closed *pb.Candle, closedStat *pb.Stat) {
	b := s.bucketOf(t.Timestamp)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started || b > s.bucket {
		closed, closedStat = s.rollLocked(b)
	}
	if b < s.bucket {
		// A late trade for a bucket we already closed. Dropping it keeps the
		// series monotonic, which matters more to the chart than the handful
		// of volume this loses.
		return closed, closedStat
	}

	c := s.candle
	if c.Open == 0 && c.High == 0 && c.Low == 0 {
		c.Open, c.High, c.Low = t.Price, t.Price, t.Price
	}
	if t.Price > c.High {
		c.High = t.Price
	}
	if t.Price < c.Low || c.Low == 0 {
		c.Low = t.Price
	}
	c.Close = t.Price
	c.Volume += t.Qty
	if t.IsBuy {
		c.Vbuy += t.Qty
		c.Tbuy++
		s.stat.TradeBuy++
	} else {
		c.Vsell += t.Qty
		c.Tsell++
		s.stat.TradeSell++
	}
	s.dirty = true
	return closed, closedStat
}

// AddLiquidation folds a liquidation into the current bucket's stat totals.
func (s *Series) AddLiquidation(l Liquidation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	usd := l.Qty * l.Price
	// is_buy true means a SHORT was force-closed, so it lands on the short
	// side of the ledger. The long/short naming refers to the position that
	// died, not to the direction of the order that killed it.
	if l.IsBuy {
		s.stat.LiqShortVolume += usd
		s.stat.LiqShortUsd += usd
	} else {
		s.stat.LiqLongVolume += usd
		s.stat.LiqLongUsd += usd
	}
	s.stat.LiqTotalUsd = s.stat.LiqLongUsd + s.stat.LiqShortUsd
	if s.stat.LiqTotalUsd > 0 {
		s.stat.LiqRatio = (s.stat.LiqLongUsd - s.stat.LiqShortUsd) / s.stat.LiqTotalUsd
	}
	s.dirty = true
}

// Tick advances the clock to now, closing the bucket if it has elapsed even
// when no trade arrived. Without this an illiquid symbol would hold a candle
// open indefinitely and the chart would show a frozen bar.
func (s *Series) Tick(nowMs int64) (closed *pb.Candle, closedStat *pb.Stat) {
	b := s.bucketOf(nowMs)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || b > s.bucket {
		return s.rollLocked(b)
	}
	return nil, nil
}

// rollLocked closes the current bucket and opens one at b.
func (s *Series) rollLocked(b int64) (closed *pb.Candle, closedStat *pb.Stat) {
	if s.started && s.candle != nil {
		c := cloneCandle(s.candle)
		c.Final = true
		closed = c
		st := cloneStat(s.stat)
		st.Final = true
		closedStat = st
	}

	prevClose := 0.0
	if s.candle != nil {
		prevClose = s.candle.Close
	}

	mark, funding, oi, nextFunding := 0.0, 0.0, 0.0, int64(0)
	if s.markFn != nil {
		mark, funding, oi, nextFunding = s.markFn()
	}
	oiUsd := oi * mark

	// A new candle opens flat at the previous close so gaps do not render as
	// a drop to zero on an illiquid symbol.
	s.candle = &pb.Candle{
		Open:        prevClose,
		High:        prevClose,
		Low:         prevClose,
		Close:       prevClose,
		TimestampMs: b,
		Timeframe:   s.TfSec,
	}
	s.stat = &pb.Stat{
		MarkPrice:       mark,
		Funding:         funding,
		TimestampMs:     b,
		Timeframe:       s.TfSec,
		OpenInterestUsd: oiUsd,
		NextFundingTime: nextFunding,
		OiOpen:          oiUsd,
		OiHigh:          oiUsd,
		OiLow:           oiUsd,
		OiClose:         oiUsd,
	}
	s.bucket = b
	s.started = true
	s.dirty = true
	return closed, closedStat
}

// Current returns a copy of the in-progress candle and stat, or nil if the
// series has not started. dirty reports whether anything changed since the
// last call, so the caller can skip an unchanged emission.
func (s *Series) Current() (c *pb.Candle, st *pb.Stat, dirty bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil, nil, false
	}
	// Refresh the live OI/mark on the open stat so the panel tracks between
	// bucket boundaries.
	if s.markFn != nil {
		mark, funding, oi, nextFunding := s.markFn()
		oiUsd := oi * mark
		s.stat.MarkPrice = mark
		s.stat.Funding = funding
		s.stat.NextFundingTime = nextFunding
		s.stat.OiClose = oiUsd
		s.stat.OpenInterestUsd = oiUsd
		if oiUsd > s.stat.OiHigh {
			s.stat.OiHigh = oiUsd
		}
		if oiUsd < s.stat.OiLow || s.stat.OiLow == 0 {
			s.stat.OiLow = oiUsd
		}
	}
	d := s.dirty
	s.dirty = false
	return cloneCandle(s.candle), cloneStat(s.stat), d
}

// Protobuf messages carry an internal state field that must not be copied by
// value (go vet flags it, and the copy shares generated-code internals), so
// these go through proto.Clone rather than a struct assignment.

func cloneCandle(c *pb.Candle) *pb.Candle {
	if c == nil {
		return nil
	}
	return proto.Clone(c).(*pb.Candle)
}

func cloneStat(s *pb.Stat) *pb.Stat {
	if s == nil {
		return nil
	}
	return proto.Clone(s).(*pb.Stat)
}

// SubMinute reports whether a timeframe has no Binance kline equivalent and
// must therefore be built from trades.
func SubMinute(tfSec int64) bool { return tfSec < 60 }
