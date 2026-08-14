package hub

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/internal/candle"
	"github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
	"github.com/edgedepthhq/edgedepth-gateway/internal/wire"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// candleTimeframes are the series built for every active symbol. The
// sub-minute entries are the ones venue REST history cannot supply.
var candleTimeframes = []int64{1, 5, 15, 30, 60, 300, 900, 1800, 3600, 14400, 86400}

// flushInterval bounds how often in-progress candles and stats go out. Every
// trade would be far too chatty on a 1s series during a burst; ten updates a
// second is already smoother than the eye can follow.
const flushInterval = 100 * time.Millisecond

// feedKey identifies one upstream instrument feed. The terminal keys every
// subscription on (exchange, symbol), so the hub does too.
type feedKey struct {
	Exchange string
	Symbol   string
}

// Hub owns the upstream feeds and fans their output out to connected clients.
// Venues are pluggable: everything venue-specific sits behind
// exchange.Exchange, registered in New.
type Hub struct {
	log *slog.Logger

	// exchanges is the venue registry, keyed by Exchange.ID(). Subscriptions
	// for an unregistered venue are ignored rather than errored: the terminal
	// always subscribes to venues this gateway may not serve (e.g. the "hl"
	// global ticker) and it renders fine without them.
	exchanges map[string]exchange.Exchange

	mu      sync.RWMutex
	feeds   map[feedKey]*symbolFeed
	clients map[*Client]struct{}

	// validSymbols is the per-venue whitelist. A missing entry means "not
	// loaded", in which case anything is allowed through and the venue
	// rejects bad symbols itself.
	validSymbols map[string]map[string]bool

	// The 24h ticker is one global stream per venue shared by every client,
	// so it is reference-counted separately from the per-symbol feeds.
	tickerRefs   map[string]int
	tickerCancel map[string]context.CancelFunc
}

type symbolFeed struct {
	feed   exchange.Feed
	series map[int64]*candle.Series
	cancel context.CancelFunc
	refs   int
}

// New creates a hub serving the given venues.
func New(log *slog.Logger, exchanges ...exchange.Exchange) *Hub {
	reg := make(map[string]exchange.Exchange, len(exchanges))
	for _, ex := range exchanges {
		reg[ex.ID()] = ex
	}
	return &Hub{
		log:          log,
		exchanges:    reg,
		feeds:        make(map[feedKey]*symbolFeed),
		clients:      make(map[*Client]struct{}),
		validSymbols: make(map[string]map[string]bool),
		tickerRefs:   make(map[string]int),
		tickerCancel: make(map[string]context.CancelFunc),
	}
}

// LoadSymbols populates each venue's tradable-symbol whitelist. A venue that
// fails to answer keeps running without validation.
func (h *Hub) LoadSymbols(ctx context.Context) error {
	var firstErr error
	for id, ex := range h.exchanges {
		set, err := ex.Symbols(ctx)
		if err != nil {
			h.log.Warn("could not load symbol list, continuing without validation",
				"exchange", id, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		h.mu.Lock()
		h.validSymbols[id] = set
		h.mu.Unlock()
		h.log.Info("loaded tradable symbols", "exchange", id, "count", len(set))
	}
	return firstErr
}

func (h *Hub) symbolOK(ex, sym string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	set := h.validSymbols[ex]
	if set == nil {
		return true
	}
	return set[sym]
}

// AddClient registers a client connection.
func (h *Hub) AddClient(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	n := len(h.clients)
	h.mu.Unlock()
	h.log.Info("client connected", "clients", n)
}

// RemoveClient deregisters a client and releases its feed references.
func (h *Hub) RemoveClient(c *Client) {
	h.mu.Lock()
	delete(h.clients, c)
	n := len(h.clients)
	h.mu.Unlock()

	for _, k := range c.Keys() {
		if k.Symbol == exchange.GlobalSymbol {
			h.releaseTicker(k.Exchange)
			continue
		}
		h.release(feedKey{k.Exchange, k.Symbol})
	}
	h.log.Info("client disconnected", "clients", n)
}

// Subscribe attaches a client to a stream, starting the upstream feed if this
// is the first reference to that (exchange, symbol).
func (h *Hub) Subscribe(c *Client, k wire.Key) {
	ex, ok := h.exchanges[k.Exchange]
	if !ok {
		h.log.Debug("ignoring subscription for unregistered venue",
			"exchange", k.Exchange, "symbol", k.Symbol)
		return
	}

	// "global" is a sentinel pair for the all-market ticker, not an
	// instrument. It gets its own single upstream stream per venue.
	if k.Symbol == exchange.GlobalSymbol {
		if k.Stream != pb.Stream_STREAM_TICKER24H {
			return
		}
		if !c.addKey(k) {
			return
		}
		h.acquireTicker(ex)
		return
	}

	if !h.symbolOK(k.Exchange, k.Symbol) {
		h.log.Warn("rejected subscription for unknown symbol",
			"exchange", k.Exchange, "symbol", k.Symbol, "stream", k.Stream)
		return
	}
	if !c.addKey(k) {
		return // already subscribed; do not double-count the feed reference
	}
	fk := feedKey{k.Exchange, k.Symbol}
	h.acquire(ex, fk)

	// Prime the new subscriber so it is not left waiting for the next event.
	// An orderbook in particular is meaningless without a base snapshot.
	if k.Stream == pb.Stream_STREAM_ORDERBOOK {
		h.mu.RLock()
		sf := h.feeds[fk]
		h.mu.RUnlock()
		if sf != nil {
			if snap := sf.feed.Snapshot(); snap != nil {
				h.sendTo(c, k, 0, time.Now().UnixMilli(), snap)
			}
		}
	}
}

// Unsubscribe detaches a client from a stream.
func (h *Hub) Unsubscribe(c *Client, k wire.Key) {
	if !c.removeKey(k) {
		return
	}
	if k.Symbol == exchange.GlobalSymbol {
		h.releaseTicker(k.Exchange)
		return
	}
	h.release(feedKey{k.Exchange, k.Symbol})
}

// acquireTicker starts the venue's shared all-market ticker on first use.
func (h *Hub) acquireTicker(ex exchange.Exchange) {
	id := ex.ID()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tickerRefs[id]++
	if h.tickerRefs[id] > 1 {
		return
	}
	tf := ex.GlobalTicker(func(stream pb.Stream, tfr, evtMs int64, inner proto.Message) {
		h.broadcast(id, exchange.GlobalSymbol, stream, tfr, evtMs, inner)
	})
	if tf == nil {
		// Venue has no all-market ticker; the refcount still tracks the
		// subscription and releaseTicker tolerates the missing cancel.
		h.log.Debug("venue has no global ticker", "exchange", id)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.tickerCancel[id] = cancel
	h.log.Info("starting global ticker24h feed", "exchange", id)
	go tf.Run(ctx)
}

func (h *Hub) releaseTicker(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tickerRefs[id]--
	if h.tickerRefs[id] > 0 || h.tickerCancel[id] == nil {
		return
	}
	h.log.Info("stopping global ticker24h feed", "exchange", id)
	h.tickerCancel[id]()
	delete(h.tickerCancel, id)
}

// acquire starts (or reference-counts) the upstream feed for an instrument.
func (h *Hub) acquire(ex exchange.Exchange, fk feedKey) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sf, ok := h.feeds[fk]; ok {
		sf.refs++
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	sf := &symbolFeed{
		series: make(map[int64]*candle.Series, len(candleTimeframes)),
		cancel: cancel,
		refs:   1,
	}

	sf.feed = ex.NewFeed(fk.Symbol, func(stream pb.Stream, tf, evtMs int64, inner proto.Message) {
		h.broadcast(fk.Exchange, fk.Symbol, stream, tf, evtMs, inner)

		// Trades and liquidations also drive the aggregators.
		switch m := inner.(type) {
		case *pb.Trade:
			for _, s := range sf.series {
				if closed, closedStat := s.AddTrade(candle.Trade{
					Price: m.Price, Qty: m.Qty, IsBuy: m.IsBuy, Timestamp: m.TimestampMs,
				}); closed != nil {
					h.emitCandle(fk, s.TfSec, closed, closedStat)
				}
			}
		case *pb.Liquidation:
			for _, s := range sf.series {
				s.AddLiquidation(candle.Liquidation{
					Price: m.Price, Qty: m.Qty, IsBuy: m.IsBuy, Timestamp: m.TimestampMs,
				})
			}
		}
	})

	for _, tf := range candleTimeframes {
		sf.series[tf] = candle.NewSeries(tf, sf.feed.MarkState)
	}

	h.feeds[fk] = sf
	h.log.Info("starting upstream feed", "exchange", fk.Exchange, "symbol", fk.Symbol)

	go sf.feed.Run(ctx)
	go h.flushLoop(ctx, fk, sf)
}

// release drops a reference and shuts the feed down when the last one goes.
func (h *Hub) release(fk feedKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sf, ok := h.feeds[fk]
	if !ok {
		return
	}
	sf.refs--
	if sf.refs > 0 {
		return
	}
	h.log.Info("stopping upstream feed", "exchange", fk.Exchange, "symbol", fk.Symbol)
	sf.cancel()
	delete(h.feeds, fk)
}

// flushLoop emits in-progress candles and stats on a fixed cadence, and
// closes buckets that elapsed without a trade.
func (h *Hub) flushLoop(ctx context.Context, fk feedKey, sf *symbolFeed) {
	tick := time.NewTicker(flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := time.Now().UnixMilli()
		for _, s := range sf.series {
			// Close any bucket that elapsed with no trades, so an illiquid
			// symbol does not hold one bar open forever.
			if closed, closedStat := s.Tick(now); closed != nil {
				h.emitCandle(fk, s.TfSec, closed, closedStat)
			}
			// Candle and stat are emitted on their own merits. Gating the
			// stat on a non-nil candle, as this loop used to, meant a symbol
			// with no trades published no stats either, so mark, funding and
			// open interest rendered "-" while sitting populated in MarkState.
			if c, st := s.Current(); c != nil || st != nil {
				h.emitCandle(fk, s.TfSec, c, st)
			}
		}
	}
}

func (h *Hub) emitCandle(fk feedKey, tf int64, c *pb.Candle, st *pb.Stat) {
	if c != nil {
		// SINGULAR on the live stream, and this is not a style choice. The
		// terminal's handle_candle_message parses STREAM_CANDLES as a bare
		// pb::Candle with no plural fallback, unlike stats and volumes which
		// try the batch first. Proto3 does not reject the wrong shape: every
		// Candles field number collides with a different wire type in Candle,
		// both sides skip everything as unknown fields, ParseFromArray returns
		// TRUE, and the terminal gets a candle of all zeros at timestamp 0.
		// That zero candle sorts to the back of the series, inverts the
		// chart's X range and blanks it until the timeframe is changed.
		// STREAM_HISTORICAL_CANDLES stays plural: handle_historical_candles
		// parses pb::Candles, and a batch is the whole point there.
		h.broadcast(fk.Exchange, fk.Symbol, pb.Stream_STREAM_CANDLES, tf, c.TimestampMs, c)
	}
	if st != nil {
		stats := &pb.Stats{Timeframe: tf, Values: []*pb.Stat{st}}
		// The terminal subscribes STREAM_STATS with three different timeframe
		// conventions, all observed on the wire: the chart's OI overlay uses
		// MILLISECONDS (tf_sec*1000, e.g. 300000 on the 5m chart), the stats
		// panel uses seconds, and one path subscribes with 0. Frames dispatch
		// client-side by exact (stream, timeframe) key match against the
		// subscribed value, so the envelope must echo the subscriber's own
		// convention. Broadcast under each alias; broadcast() encodes nothing
		// when no client holds a key, so unused aliases cost one map scan.
		h.broadcast(fk.Exchange, fk.Symbol, pb.Stream_STREAM_STATS, tf, st.TimestampMs, stats)
		h.broadcast(fk.Exchange, fk.Symbol, pb.Stream_STREAM_STATS, tf*1000, st.TimestampMs, stats)
		if tf == 1 {
			h.broadcast(fk.Exchange, fk.Symbol, pb.Stream_STREAM_STATS, 0, st.TimestampMs, stats)
		}
	}
}

// broadcast encodes once and sends to every client subscribed to the key.
func (h *Hub) broadcast(ex, symbol string, stream pb.Stream, tf, evtMs int64, inner proto.Message) {
	k := wire.Key{Exchange: ex, Symbol: symbol, Stream: stream}
	if wire.Timeframed(stream) {
		k.Timeframe = tf
	}

	h.mu.RLock()
	targets := make([]*Client, 0, 4)
	for c := range h.clients {
		if c.subscribed(k) {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()

	if len(targets) == 0 {
		return
	}

	// One encode for all recipients: the payload is identical per key.
	buf, err := wire.Encode(k.Pair(), stream, k.Timeframe, evtMs, inner)
	if err != nil {
		h.log.Error("encode failed", "stream", stream, "err", err)
		return
	}
	for _, c := range targets {
		c.Send(buf)
	}
}

// sendTo encodes for a single client, used for priming a fresh subscription.
func (h *Hub) sendTo(c *Client, k wire.Key, tf, evtMs int64, inner proto.Message) {
	buf, err := wire.Encode(k.Pair(), k.Stream, tf, evtMs, inner)
	if err != nil {
		h.log.Error("encode failed", "stream", k.Stream, "err", err)
		return
	}
	c.Send(buf)
}

// HistoricalCandles answers a get_historical_candles request from the venue's
// historical source, riding the same envelope on STREAM_HISTORICAL_CANDLES.
// Timeframes the venue cannot serve get an empty batch rather than silence,
// so the client stops waiting on them and renders from live trades instead.
func (h *Hub) HistoricalCandles(ctx context.Context, c *Client, exID, symbol string, tfSec int64, count int, endTimeMs int64) {
	ex, ok := h.exchanges[exID]
	if !ok {
		return
	}
	key := wire.Key{Exchange: exID, Symbol: symbol,
		Stream: pb.Stream_STREAM_HISTORICAL_CANDLES, Timeframe: tfSec}

	vals, err := ex.HistoricalCandles(ctx, symbol, tfSec, count, endTimeMs)
	if err != nil {
		if errors.Is(err, exchange.ErrUnsupported) {
			h.log.Debug("no historical source for timeframe",
				"exchange", exID, "symbol", symbol, "timeframe", tfSec)
			h.sendTo(c, key, tfSec, 0, &pb.Candles{Timeframe: tfSec})
			return
		}
		h.log.Warn("historical candles fetch failed",
			"exchange", exID, "symbol", symbol, "timeframe", tfSec, "err", err)
		return
	}

	out := &pb.Candles{Timeframe: tfSec, Values: vals}
	h.log.Debug("serving historical candles",
		"exchange", exID, "symbol", symbol, "timeframe", tfSec, "count", len(vals))
	h.sendTo(c, key, tfSec, 0, out)

	// Seed the live series so the first live candle continues from history
	// instead of opening at zero.
	if len(vals) > 0 {
		h.mu.RLock()
		sf := h.feeds[feedKey{exID, symbol}]
		h.mu.RUnlock()
		if sf != nil {
			if s := sf.series[tfSec]; s != nil {
				s.Seed(cloneLast(vals))
			}
		}
	}
}

func cloneLast(vals []*pb.Candle) *pb.Candle {
	last := vals[len(vals)-1]
	return proto.Clone(last).(*pb.Candle)
}
