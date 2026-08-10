package binance

import (
	"context"
	"encoding/json"
	"log/slog"
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
}

// NewTickerFeed creates the global ticker feed.
func NewTickerFeed(log *slog.Logger, emit func(*pb.Ticker24HUpdate)) *TickerFeed {
	return &TickerFeed{log: log.With("feed", "ticker24h"), emit: emit}
}

// Run blocks until ctx is cancelled.
func (t *TickerFeed) Run(ctx context.Context) {
	s := NewStream([]string{"!ticker@arr"}, t.log, t.onMessage, nil)
	s.Run(ctx)
}

func (t *TickerFeed) onMessage(env Envelope) {
	var arr []tickerEntry
	if json.Unmarshal(env.Data, &arr) != nil {
		return
	}
	if len(arr) == 0 {
		return
	}

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
