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

	mu     sync.Mutex
	candle *pb.Candle
	stat   *pb.Stat
	bucket int64 // current bucket start, ms

	// Candle and stat go dirty independently. The candle moves only on trades;
	// mark price, funding and open interest arrive from a REST poll that owes
	// nothing to the tape. Sharing one flag meant an untraded symbol published
	// no stats at all, which is what left the terminal's stats strip empty.
	candleDirty bool
	statDirty   bool

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
	s.candleDirty = true
	s.statDirty = true // the trade counts live on the stat
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
	// Liquidations touch the stat only; the candle is unchanged and does not
	// need reflushing on their account.
	s.statDirty = true
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

// priced reports whether a candle carries a real price. Before the first
// trade, and with no previous close to open flat at, every OHLC field is
// zero, and such a candle must never reach the wire: the chart autoscales to
// include it and the real price compresses into a line at the top.
func priced(c *pb.Candle) bool {
	return c != nil && (c.Open != 0 || c.High != 0 || c.Low != 0 || c.Close != 0)
}

// statReadable reports whether a stat carries anything the panel can render.
// An all-zero stat is worse than none: the strip shows a 0.00 mark price
// instead of the "-" that honestly means "nothing yet". Cold start hits this
// for the few hundred ms before the first REST poll returns.
func statReadable(st *pb.Stat) bool {
	return st != nil && (st.MarkPrice != 0 || st.OpenInterestUsd != 0 ||
		st.Funding != 0 || st.LiqTotalUsd != 0 ||
		st.TradeBuy != 0 || st.TradeSell != 0)
}

// rollLocked closes the current bucket and opens one at b.
func (s *Series) rollLocked(b int64) (closed *pb.Candle, closedStat *pb.Stat) {
	if s.started && priced(s.candle) {
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
	// open_interest_usd carries CONTRACT QTY despite its name: the hosted
	// backend's stat sampler says so explicitly (actor/stat/stat.go
	// SetOpenInterest) and the terminal's stats panel multiplies the field by
	// mark price to get notional. Sending USD here made the panel display
	// contracts*mark*mark, off by a factor of the price.
	oiContracts := oi

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
		OpenInterestUsd: oiContracts,
		NextFundingTime: nextFunding,
		OiOpen:          oiContracts,
		OiHigh:          oiContracts,
		OiLow:           oiContracts,
		OiClose:         oiContracts,
	}
	s.bucket = b
	s.started = true
	// A bucket that opens with no previous close has no price to show yet, so
	// there is nothing worth flushing; the candle goes dirty on the first real
	// trade. Marking it dirty here would stream all-zero candles every Tick
	// until one arrives.
	s.candleDirty = prevClose != 0
	// The stat is new regardless. It carries this bucket's opening mark,
	// funding and open interest, none of which needed a trade to exist.
	s.statDirty = true
	return closed, closedStat
}

// refreshMarkLocked pulls the latest mark, funding and open interest onto the
// open stat so the panel tracks between bucket boundaries, and reports a real
// change by setting statDirty. The change check is what keeps this from
// reflushing an identical stat on every 100ms flush tick.
func (s *Series) refreshMarkLocked() {
	if s.markFn == nil || s.stat == nil {
		return
	}
	mark, funding, oi, nextFunding := s.markFn()
	if mark == s.stat.MarkPrice && funding == s.stat.Funding &&
		oi == s.stat.OiClose && nextFunding == s.stat.NextFundingTime {
		return
	}
	s.stat.MarkPrice = mark
	s.stat.Funding = funding
	s.stat.NextFundingTime = nextFunding
	// Contracts, not USD; see the note in rollLocked.
	s.stat.OiClose = oi
	s.stat.OpenInterestUsd = oi
	if oi > s.stat.OiHigh {
		s.stat.OiHigh = oi
	}
	if oi < s.stat.OiLow || s.stat.OiLow == 0 {
		s.stat.OiLow = oi
	}
	s.statDirty = true
}

// Current returns the in-progress candle and stat. Either is nil when it has
// nothing new worth sending, so a caller emits whatever it is handed and skips
// the rest.
//
// The two are deliberately independent. The candle is withheld until a trade
// has priced it (see priced), because an unpriced one is all zeros and the
// chart autoscales to include it. The stat is not withheld on that basis: mark
// price, funding and open interest come from the REST poll, so a symbol that
// has not traded in this bucket still has a stats panel worth filling. Tying
// the stat to the candle flush is what left the terminal's stats strip reading
// "-" with a healthy feed and live values sitting in MarkState.
func (s *Series) Current() (c *pb.Candle, st *pb.Stat) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil, nil
	}
	s.refreshMarkLocked()
	if s.candleDirty && priced(s.candle) {
		c = cloneCandle(s.candle)
		s.candleDirty = false
	}
	if s.statDirty && statReadable(s.stat) {
		st = cloneStat(s.stat)
		s.statDirty = false
	}
	return c, st
}

// Stat returns the current stat without consuming its dirty flag, for priming
// a subscriber that arrived after the last flush. Current cannot serve this:
// it hands out each value once, so calling it here would swallow the frame
// every other client is waiting on.
func (s *Series) Stat() *pb.Stat {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || !statReadable(s.stat) {
		return nil
	}
	return cloneStat(s.stat)
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
