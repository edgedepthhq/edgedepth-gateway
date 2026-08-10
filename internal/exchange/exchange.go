// Package exchange defines the venue adapter seam. The hub knows streams,
// candle aggregation and fan-out; everything venue-specific (transport, JSON
// shapes, orderbook sync procedure, REST endpoints) lives behind Exchange.
//
// Adding a venue is one new package implementing Exchange plus one line in
// cmd/edgedepth-gateway registering it. See CONTRIBUTING.md for the
// walkthrough and internal/binance for the reference implementation.
package exchange

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// GlobalSymbol is the sentinel the terminal subscribes with for the
// all-market 24h ticker: Pair{<exchange>, "global"}, not a real instrument.
// It is a terminal-wide convention, not a Binance one; the client subscribes
// {"hl", "global"} the same way. Treating it as a symbol and opening an
// upstream feed for it would error against any venue.
const GlobalSymbol = "global"

// ErrUnsupported marks a capability a venue does not have (for example
// historical candles on a venue with no klines REST). The hub answers such
// requests with an empty batch so the client stops waiting.
var ErrUnsupported = errors.New("not supported on this venue")

// Emit hands one decoded market message to the hub. timeframe is 0 for
// non-timeframed streams; eventTimeMs is the market event timestamp.
type Emit func(stream pb.Stream, timeframe int64, eventTimeMs int64, inner proto.Message)

// Runner is a long-lived upstream connection. Run blocks until ctx is
// cancelled and owns its own reconnect loop.
type Runner interface {
	Run(ctx context.Context)
}

// Feed is one instrument's live upstream connection. Beyond delivering
// frames through Emit it maintains the orderbook, because a client that
// connects ten minutes in needs a base snapshot before diffs mean anything.
type Feed interface {
	Runner

	// Snapshot returns the current book as a BookUpdate{snapshot:true}, or
	// nil until the book has synced. Used to prime a fresh subscription.
	Snapshot() *pb.BookUpdate

	// MarkState is the latest mark/funding/open-interest reading, polled by
	// the candle aggregator when it stamps per-bucket stats. Venues without
	// these concepts return zeros; the stats panel stays empty and everything
	// else works.
	MarkState() (mark, funding, oi float64, nextFundingMs int64)
}

// Exchange is one venue adapter.
type Exchange interface {
	// ID is the venue key the terminal subscribes with, e.g. "binancef".
	// It must match the client side exactly: the terminal keys every
	// subscription on (exchange, symbol), and a mismatched id means
	// subscriptions silently never match. Check the client's existing venue
	// ids before inventing one.
	ID() string

	// Symbols returns the tradable-symbol whitelist. A nil map means "allow
	// everything and let the venue reject bad symbols itself"; the hub calls
	// this once at startup and continues without validation on error.
	Symbols(ctx context.Context) (map[string]bool, error)

	// NewFeed creates the per-symbol feed. The hub calls Run on it.
	NewFeed(symbol string, emit Emit) Feed

	// GlobalTicker returns the all-market 24h ticker runner, or nil when the
	// venue has no equivalent; the hub then ignores global subscriptions for
	// this venue and the terminal renders fine without them.
	GlobalTicker(emit Emit) Runner

	// HistoricalCandles returns up to count bars of tfSec candles ending at
	// endTimeMs (0 = now), oldest first, ready for the wire. Return
	// ErrUnsupported for timeframes with no historical source; live
	// aggregation fills those from the first trade onward.
	HistoricalCandles(ctx context.Context, symbol string, tfSec int64, count int, endTimeMs int64) ([]*pb.Candle, error)
}
