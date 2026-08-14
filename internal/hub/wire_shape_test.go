package hub_test

// Regression guard for the wire-shape class of bug: a message the gateway
// encodes one way and the terminal decodes another. Proto3 does not reject a
// mismatch, so these never surface as an error, only as a blank panel minutes
// later. Every assertion below decodes the frame EXACTLY as
// c-based-trader-client/src/core/message_handler.cpp does, because matching
// the gateway's own idea of what it sent is what let the bug ship.
//
// No network: a stub venue stands in for Binance, so this runs in CI where the
// live probe in probe_test.go cannot.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
	"github.com/edgedepthhq/edgedepth-gateway/internal/hub"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

const (
	stubSymbol   = "btcusdt"
	stubHistTf   = int64(60)
	stubHistLast = 63500.0
)

// ── stub venue ──────────────────────────────────────────────────────────────

type stubFeed struct {
	emit exchange.Emit
}

func (f *stubFeed) Run(ctx context.Context)  { <-ctx.Done() }
func (f *stubFeed) Snapshot() *pb.BookUpdate { return nil }
func (f *stubFeed) MarkState() (float64, float64, float64, int64) {
	return 64000, 0.0001, 1234, 0
}

// trade pushes one trade through the same callback the real feed uses, which
// is what drives the candle aggregators.
func (f *stubFeed) trade(price, qty float64, tsMs int64) {
	f.emit(pb.Stream_STREAM_TRADES, 0, tsMs, &pb.Trade{
		Price: price, Qty: qty, IsBuy: true, TimestampMs: tsMs,
	})
}

type stubExchange struct {
	mu   sync.Mutex
	feed *stubFeed
}

func (e *stubExchange) ID() string                                       { return "binancef" }
func (e *stubExchange) Symbols(context.Context) (map[string]bool, error) { return nil, nil }
func (e *stubExchange) GlobalTicker(exchange.Emit) exchange.Runner       { return nil }

func (e *stubExchange) NewFeed(_ string, emit exchange.Emit) exchange.Feed {
	f := &stubFeed{emit: emit}
	e.mu.Lock()
	e.feed = f
	e.mu.Unlock()
	return f
}

func (e *stubExchange) HistoricalCandles(_ context.Context, _ string, tfSec int64, _ int, _ int64) ([]*pb.Candle, error) {
	if tfSec < 60 {
		return nil, exchange.ErrUnsupported
	}
	base := time.Now().UnixMilli() / (tfSec * 1000) * (tfSec * 1000)
	return []*pb.Candle{
		{Open: 63000, High: 63600, Low: 62900, Close: 63200, Volume: 12,
			TimestampMs: base - tfSec*1000, Timeframe: tfSec, Final: true},
		{Open: 63200, High: 63700, Low: 63100, Close: stubHistLast, Volume: 9,
			TimestampMs: base, Timeframe: tfSec, Final: true},
	}, nil
}

// waitForFeed blocks until a subscription has caused the venue to open a feed.
func (e *stubExchange) waitForFeed(t *testing.T) *stubFeed {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		f := e.feed
		e.mu.Unlock()
		if f != nil {
			return f
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("venue was never asked for a feed, so the subscription did not land")
	return nil
}

// ── harness ─────────────────────────────────────────────────────────────────

// newHub stands up a hub over a stub venue.
func newHub(t *testing.T) (*stubExchange, *httptest.Server) {
	t.Helper()
	ex := &stubExchange{}
	h := hub.New(slog.New(slog.NewTextHandler(io.Discard, nil)), ex)
	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	t.Cleanup(srv.Close)
	return ex, srv
}

// dial connects one terminal to a hub. Several may share one.
func dial(t *testing.T, srv *httptest.Server, readTimeout time.Duration) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	return conn
}

// newProbe stands up a hub over a stub venue and returns a connected client.
func newProbe(t *testing.T) (*stubExchange, *websocket.Conn) {
	t.Helper()
	ex, srv := newHub(t)
	return ex, dial(t, srv, 10*time.Second)
}

func subscribe(t *testing.T, conn *websocket.Conn, sym string, stream pb.Stream, tf int64) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage,
		[]byte(subMsg("subscribe", sym, stream, tf))); err != nil {
		t.Fatalf("subscribe %v tf=%d: %v", stream, tf, err)
	}
}

// readFrame returns the next payload on the given stream, failing the test if
// none arrives before the read deadline.
func readFrame(t *testing.T, conn *websocket.Conn, want pb.Stream) *pb.WSPayload {
	t.Helper()
	for {
		mt, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("no %v frame arrived: %v", want, err)
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		var env pb.WSPayload
		if err := proto.Unmarshal(raw, &env); err != nil {
			t.Fatalf("frame is not a WSPayload: %v", err)
		}
		if env.Stream == want {
			return &env
		}
	}
}

// ── the assertions ──────────────────────────────────────────────────────────

// A live candle frame must decode as a SINGULAR pb.Candle, because that is the
// only shape handle_candle_message parses. When the gateway sent the plural
// pb.Candles wrapper instead, this decode still succeeded and returned a
// candle of all zeros at timestamp 0, which sorted to the back of the client's
// series, inverted the chart's X range and blanked it permanently.
func TestLiveCandleIsSingularOnTheWire(t *testing.T) {
	ex, conn := newProbe(t)

	if err := conn.WriteMessage(websocket.TextMessage,
		[]byte(subMsg("subscribe", stubSymbol, pb.Stream_STREAM_CANDLES, 1))); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	const price = 64250.5
	ts := time.Now().UnixMilli()
	ex.waitForFeed(t).trade(price, 0.25, ts)

	env := readFrame(t, conn, pb.Stream_STREAM_CANDLES)

	var c pb.Candle
	if err := proto.Unmarshal(env.Data, &c); err != nil {
		t.Fatalf("live candle does not decode as pb.Candle: %v", err)
	}
	if c.Close != price {
		t.Fatalf("live candle decoded with close %v, want %v. A zero here means "+
			"the gateway is sending a shape the terminal cannot read: %+v",
			c.Close, price, &c)
	}
	if c.TimestampMs < 1e12 {
		t.Fatalf("candle timestamp %d is not a ms epoch: %+v", c.TimestampMs, &c)
	}
	if c.Timeframe != env.Timeframe {
		t.Fatalf("timeframe mismatch: envelope=%d inner=%d", env.Timeframe, c.Timeframe)
	}

	// The other half of the contract. If someone reintroduces the wrapper this
	// stays true while the singular decode above silently zeroes, so assert it
	// explicitly rather than trusting the close check alone.
	var batch pb.Candles
	if proto.Unmarshal(env.Data, &batch) == nil && len(batch.Values) > 0 {
		t.Fatalf("live candle frame is a pb.Candles batch; the terminal parses "+
			"STREAM_CANDLES as a singular pb.Candle and would read zeros: %+v", &batch)
	}
}

// Historical candles keep the plural wrapper: handle_historical_candles parses
// pb.Candles, and a batch is the whole point of that stream. A well-meant
// "make candles consistent" change that made this singular would break
// backfill instead.
func TestHistoricalCandlesArePluralOnTheWire(t *testing.T) {
	_, conn := newProbe(t)

	req, _ := json.Marshal(map[string]any{
		"method": "get_historical_candles",
		"data": map[string]any{
			"pair":      map[string]string{"exchange": "binancef", "symbol": stubSymbol},
			"timeframe": stubHistTf,
			"count":     200,
		},
	})
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		t.Fatalf("historical request: %v", err)
	}

	env := readFrame(t, conn, pb.Stream_STREAM_HISTORICAL_CANDLES)

	var batch pb.Candles
	if err := proto.Unmarshal(env.Data, &batch); err != nil {
		t.Fatalf("historical frame does not decode as pb.Candles: %v", err)
	}
	if len(batch.Values) != 2 {
		t.Fatalf("expected the venue's 2 candles, got %d: %+v", len(batch.Values), &batch)
	}
	if got := batch.Values[len(batch.Values)-1].Close; got != stubHistLast {
		t.Fatalf("last historical candle closed at %v, want %v", got, stubHistLast)
	}
	if batch.Timeframe != env.Timeframe {
		t.Fatalf("timeframe mismatch: envelope=%d inner=%d", env.Timeframe, batch.Timeframe)
	}
}
