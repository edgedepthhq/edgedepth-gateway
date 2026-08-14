package binance

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// EventType exists to absorb the "e" key. See the note on depthDiff in
// feed.go: without it Go's case-insensitive fallback assigns "e" to "E" and
// every message fails to unmarshal.
type tickerEntry struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	LastPrice string `json:"c"`
	ChangePct string `json:"P"`
	QuoteVol  string `json:"q"`
}

// TickerFeed streams Binance's all-market 24h ticker array.
//
// Binance pushes !ticker@arr once per second carrying every symbol, which is
// roughly 150KB/s. The terminal wants exactly that for its watchlist and
// status bar, so it is forwarded whole rather than filtered per subscriber.
type TickerFeed struct {
	log  *slog.Logger
	emit func(*pb.Ticker24HUpdate)

	// frames counts stream pushes, so the REST fallback can tell "the socket
	// is working" from "the socket connected and delivered nothing".
	frames atomic.Int64
}

// tickerPollInterval is how often the REST fallback refreshes. The watchlist
// shows last price and 24h change, neither of which needs sub-minute
// freshness, and the endpoint is weight-40. A var, like RESTBase and
// TradeStream, so a test does not have to wait 30 seconds.
var tickerPollInterval = 30 * time.Second

// NewTickerFeed creates the global ticker feed.
func NewTickerFeed(log *slog.Logger, emit func(*pb.Ticker24HUpdate)) *TickerFeed {
	return &TickerFeed{log: log.With("feed", "ticker24h"), emit: emit}
}

// Run blocks until ctx is cancelled.
func (t *TickerFeed) Run(ctx context.Context) {
	go t.pollREST(ctx)
	s := NewStream([]string{"!ticker@arr"}, t.log, t.onMessage, nil)
	s.Run(ctx)
}

// pollREST serves the 24h ticker from REST while the stream is silent.
//
// !ticker@arr is not reachable from every network, the same class of problem
// as @aggTrade (see TradeStream), and it fails the same silent way: the socket
// connects, subscribes, and delivers nothing. The visible result is a
// watchlist where every row has a symbol and no numbers, which reads as a
// broken product rather than a blocked stream.
//
// The stream wins whenever it works, since one push per second beats a 30s
// poll, so this stops emitting the moment a single frame arrives. The startup
// poll fires immediately rather than after a tick, which fills the watchlist
// slightly before the socket finishes connecting on every network.
func (t *TickerFeed) pollREST(ctx context.Context) {
	tick := time.NewTicker(tickerPollInterval)
	defer tick.Stop()

	for n := 0; ; n++ {
		if t.frames.Load() == 0 {
			arr, err := Tickers24h(ctx)
			switch {
			case err != nil:
				if ctx.Err() == nil {
					t.log.Debug("24h ticker poll failed", "err", err)
				}
			case len(arr) > 0:
				// The startup poll beats the socket to the punch on a healthy
				// network too, so it proves nothing. A second silent round
				// does, and that is worth telling the operator about.
				if n == 1 {
					t.log.Warn("no !ticker@arr frames after 30s, serving the 24h "+
						"ticker from REST instead; watchlist prices will refresh "+
						"every 30s rather than every second", "symbols", len(arr))
				}
				t.emit(restUpdate(arr))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func restUpdate(arr []Ticker24h) *pb.Ticker24HUpdate {
	out := &pb.Ticker24HUpdate{
		Entries:     make([]*pb.Ticker24HEntry, 0, len(arr)),
		TimestampMs: time.Now().UnixMilli(),
	}
	for _, e := range arr {
		out.Entries = append(out.Entries, &pb.Ticker24HEntry{
			// The terminal expects the uppercase Binance form, which is what
			// both the stream and REST already give.
			Symbol:      e.Symbol,
			LastPrice:   e.LastPrice,
			ChangePct:   e.ChangePct,
			VolumeQuote: e.VolumeQuote,
			EventTimeMs: e.EventTimeMs,
		})
	}
	return out
}

func (t *TickerFeed) onMessage(env Envelope) {
	var arr []tickerEntry
	if json.Unmarshal(env.Data, &arr) != nil {
		return
	}
	if len(arr) == 0 {
		return
	}

	t.frames.Add(1)

	out := &pb.Ticker24HUpdate{
		Entries:     make([]*pb.Ticker24HEntry, 0, len(arr)),
		TimestampMs: time.Now().UnixMilli(),
	}
	for _, e := range arr {
		out.Entries = append(out.Entries, &pb.Ticker24HEntry{
			// The terminal expects the uppercase Binance form here.
			Symbol:      e.Symbol,
			LastPrice:   parseF(e.LastPrice),
			ChangePct:   parseF(e.ChangePct),
			VolumeQuote: parseF(e.QuoteVol),
			EventTimeMs: e.EventTime,
		})
	}
	t.emit(out)
}
