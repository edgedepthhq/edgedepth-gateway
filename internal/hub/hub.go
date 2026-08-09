package hub

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/internal/binance"
	"github.com/edgedepthhq/edgedepth-gateway/internal/candle"
	"github.com/edgedepthhq/edgedepth-gateway/internal/wire"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Exchange is the only exchange this gateway serves. The terminal keys every
// subscription on (exchange, symbol) and its Binance futures venue is
// "binancef", so anything else is rejected rather than silently served.
const Exchange = "binancef"

// candleTimeframes are the series built for every active symbol. The
// sub-minute entries are the ones Binance cannot supply.
var candleTimeframes = []int64{1, 5, 15, 30, 60, 300, 900, 1800, 3600, 14400, 86400}

// flushInterval bounds how often in-progress candles and stats go out. Every
// trade would be far too chatty on a 1s series during a burst; ten updates a
// second is already smoother than the eye can follow.
const flushInterval = 100 * time.Millisecond

// Hub owns the upstream feeds and fans their output out to connected clients.
type Hub struct {
	log *slog.Logger

	mu      sync.RWMutex
	feeds   map[string]*symbolFeed
	clients map[*Client]struct{}

	// validSymbols is the exchangeInfo whitelist. nil means "not loaded yet",
	// in which case anything is allowed through and Binance itself rejects it.
	validSymbols map[string]bool

	// The 24h ticker is one global stream shared by every client, so it is
	// reference-counted separately from the per-symbol feeds.
	tickerRefs   int
	tickerCancel context.CancelFunc
}

type symbolFeed struct {
	feed   *binance.Feed
	series map[int64]*candle.Series
	cancel context.CancelFunc
	refs   int
}

// New creates a hub.
func New(log *slog.Logger) *Hub {
	return &Hub{
		log:     log,
		feeds:   make(map[string]*symbolFeed),
		clients: make(map[*Client]struct{}),
	}
}

// LoadSymbols populates the tradable-symbol whitelist from Binance.
func (h *Hub) LoadSymbols(ctx context.Context) error {
	set, err := binance.ExchangeSymbols(ctx)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.validSymbols = set
	h.mu.Unlock()
	h.log.Info("loaded tradable symbols", "count", len(set))
	return nil
}

func (h *Hub) symbolOK(sym string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.validSymbols == nil {
		return true
	}
	return h.validSymbols[sym]
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
		if k.Symbol == binance.GlobalSymbol {
			h.releaseTicker()
			continue
		}
		h.release(k.Symbol)
	}
	h.log.Info("client disconnected", "clients", n)
}

// Subscribe attaches a client to a stream, starting the upstream feed if this
// is the first reference to that symbol.
func (h *Hub) Subscribe(c *Client, k wire.Key) {
	// The terminal always subscribes to the "hl" (Hyperliquid) global ticker
	// alongside the Binance one. Rejecting it quietly is correct: this
	// gateway is Binance-only and the client renders fine without it.
	if k.Exchange != Exchange {
		h.log.Debug("ignoring non-binancef subscription",
			"exchange", k.Exchange, "symbol", k.Symbol)
		return
	}

	// "global" is a sentinel pair for the all-market ticker, not an
	// instrument. It gets its own single upstream stream.
	if k.Symbol == binance.GlobalSymbol {
		if k.Stream != pb.Stream_STREAM_TICKER24H {
			return
		}
		if !c.addKey(k) {
			return
		}
		h.acquireTicker()
		return
	}

	if !h.symbolOK(k.Symbol) {
		h.log.Warn("rejected subscription for unknown symbol",
			"symbol", k.Symbol, "stream", k.Stream)
		return
	}
	if !c.addKey(k) {
		return // already subscribed; do not double-count the feed reference
	}
	h.acquire(k.Symbol)

	// Prime the new subscriber so it is not left waiting for the next event.
	// An orderbook in particular is meaningless without a base snapshot.
	if k.Stream == pb.Stream_STREAM_ORDERBOOK {
		h.mu.RLock()
		sf := h.feeds[k.Symbol]
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
	if k.Symbol == binance.GlobalSymbol {
		h.releaseTicker()
		return
	}
	h.release(k.Symbol)
}

// acquireTicker starts the shared all-market ticker stream on first use.
func (h *Hub) acquireTicker() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tickerRefs++
	if h.tickerRefs > 1 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.tickerCancel = cancel
	h.log.Info("starting global ticker24h feed")

	tf := binance.NewTickerFeed(h.log, func(u *pb.Ticker24HUpdate) {
		h.broadcast(binance.GlobalSymbol, pb.Stream_STREAM_TICKER24H, 0, u.TimestampMs, u)
	})
	go tf.Run(ctx)
}

func (h *Hub) releaseTicker() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tickerRefs--
	if h.tickerRefs > 0 || h.tickerCancel == nil {
		return
	}
	h.log.Info("stopping global ticker24h feed")
	h.tickerCancel()
	h.tickerCancel = nil
}

// acquire starts (or reference-counts) the upstream feed for a symbol.
func (h *Hub) acquire(symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sf, ok := h.feeds[symbol]; ok {
		sf.refs++
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	sf := &symbolFeed{
		series: make(map[int64]*candle.Series, len(candleTimeframes)),
		cancel: cancel,
		refs:   1,
	}

	sf.feed = binance.NewFeed(symbol, h.log, func(stream pb.Stream, tf, evtMs int64, inner proto.Message) {
		h.broadcast(symbol, stream, tf, evtMs, inner)

		// Trades and liquidations also drive the aggregators.
		switch m := inner.(type) {
		case *pb.Trade:
			for _, s := range sf.series {
				if closed, closedStat := s.AddTrade(candle.Trade{
					Price: m.Price, Qty: m.Qty, IsBuy: m.IsBuy, Timestamp: m.TimestampMs,
				}); closed != nil {
					h.emitCandle(symbol, s.TfSec, closed, closedStat)
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

	h.feeds[symbol] = sf
	h.log.Info("starting upstream feed", "symbol", symbol)

	go sf.feed.Run(ctx)
	go h.flushLoop(ctx, symbol, sf)
}

// release drops a reference and shuts the feed down when the last one goes.
func (h *Hub) release(symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sf, ok := h.feeds[symbol]
	if !ok {
		return
	}
	sf.refs--
	if sf.refs > 0 {
		return
	}
	h.log.Info("stopping upstream feed", "symbol", symbol)
	sf.cancel()
	delete(h.feeds, symbol)
}

// flushLoop emits in-progress candles and stats on a fixed cadence, and
// closes buckets that elapsed without a trade.
func (h *Hub) flushLoop(ctx context.Context, symbol string, sf *symbolFeed) {
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
				h.emitCandle(symbol, s.TfSec, closed, closedStat)
			}
			c, st, dirty := s.Current()
			if !dirty || c == nil {
				continue
			}
			h.emitCandle(symbol, s.TfSec, c, st)
		}
	}
}

func (h *Hub) emitCandle(symbol string, tf int64, c *pb.Candle, st *pb.Stat) {
	if c != nil {
		h.broadcast(symbol, pb.Stream_STREAM_CANDLES, tf, c.TimestampMs,
			&pb.Candles{Timeframe: tf, Values: []*pb.Candle{c}})
	}
	if st != nil {
		h.broadcast(symbol, pb.Stream_STREAM_STATS, tf, st.TimestampMs,
			&pb.Stats{Timeframe: tf, Values: []*pb.Stat{st}})
	}
}

// broadcast encodes once and sends to every client subscribed to the key.
func (h *Hub) broadcast(symbol string, stream pb.Stream, tf, evtMs int64, inner proto.Message) {
	k := wire.Key{Exchange: Exchange, Symbol: symbol, Stream: stream}
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

// HistoricalCandles answers a get_historical_candles request.
//
// The plan for this gateway assumed historical backfill would stay empty
// without the hosted backend. It does not have to: Binance's public REST
// klines cover every timeframe from 1m up, and the response rides the same
// envelope on STREAM_HISTORICAL_CANDLES. Only the sub-minute series have no
// REST source, and those legitimately start empty and fill from live trades.
func (h *Hub) HistoricalCandles(ctx context.Context, c *Client, symbol string, tfSec int64, count int, endTimeMs int64) {
	if candle.SubMinute(tfSec) {
		h.log.Debug("no REST source for sub-minute history",
			"symbol", symbol, "timeframe", tfSec)
		// Answer with an empty batch rather than silence, so the client stops
		// waiting on it and renders from live trades instead.
		h.sendTo(c, wire.Key{Exchange: Exchange, Symbol: symbol,
			Stream: pb.Stream_STREAM_HISTORICAL_CANDLES, Timeframe: tfSec},
			tfSec, 0, &pb.Candles{Timeframe: tfSec})
		return
	}

	kl, err := binance.Klines(ctx, symbol, tfSec, count, endTimeMs)
	if err != nil {
		h.log.Warn("klines fetch failed", "symbol", symbol, "timeframe", tfSec, "err", err)
		return
	}

	out := &pb.Candles{Timeframe: tfSec, Values: make([]*pb.Candle, 0, len(kl))}
	for i := range kl {
		k := &kl[i]
		// Binance gives taker BUY volume; the sell side is the remainder.
		vsell := k.Volume - k.TakerBuyVolume
		if vsell < 0 {
			vsell = 0
		}
		out.Values = append(out.Values, &pb.Candle{
			Open:        k.Open,
			High:        k.High,
			Low:         k.Low,
			Close:       k.Close,
			Volume:      k.Volume,
			Vbuy:        k.TakerBuyVolume,
			Vsell:       vsell,
			TimestampMs: k.OpenTime,
			Timeframe:   tfSec,
			// The final kline is still forming unless its close time has passed.
			Final: k.CloseTime < time.Now().UnixMilli(),
		})
	}

	h.log.Debug("serving historical candles",
		"symbol", symbol, "timeframe", tfSec, "count", len(out.Values))

	h.sendTo(c, wire.Key{Exchange: Exchange, Symbol: symbol,
		Stream: pb.Stream_STREAM_HISTORICAL_CANDLES, Timeframe: tfSec},
		tfSec, 0, out)

	// Seed the live series so the first live candle continues from history
	// instead of opening at zero.
	if len(out.Values) > 0 {
		h.mu.RLock()
		sf := h.feeds[symbol]
		h.mu.RUnlock()
		if sf != nil {
			if s := sf.series[tfSec]; s != nil {
				s.Seed(cloneLast(out.Values))
			}
		}
	}
}

func cloneLast(vals []*pb.Candle) *pb.Candle {
	last := vals[len(vals)-1]
	return proto.Clone(last).(*pb.Candle)
}
