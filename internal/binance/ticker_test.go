package binance

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Two entries plus the decoy that matters: priceChange and
// priceChangePercent both exist and differ, and the terminal renders the
// field it is given as a percentage. Picking the wrong one shows plausible
// wrong numbers, which is worse than showing none.
const ticker24hFixture = `[
  {"symbol":"BTCUSDT","priceChange":"-1200.5","priceChangePercent":"-1.865",
   "lastPrice":"63200.10","quoteVolume":"9876543210.5","closeTime":1786000000000},
  {"symbol":"ETHUSDT","priceChange":"41.2","priceChangePercent":"2.034",
   "lastPrice":"2065.44","quoteVolume":"1234567890.1","closeTime":1786000000001}
]`

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// serveFixture points RESTBase at a local server for the duration of a test.
func serveFixture(t *testing.T, body string) *atomic.Int64 {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, body)
	}))
	prev := RESTBase
	RESTBase = srv.URL
	t.Cleanup(func() {
		RESTBase = prev
		srv.Close()
	})
	return &hits
}

func TestTickers24hFieldMapping(t *testing.T) {
	serveFixture(t, ticker24hFixture)

	got, err := Tickers24h(context.Background())
	if err != nil {
		t.Fatalf("Tickers24h: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	btc := got[0]
	if btc.Symbol != "BTCUSDT" {
		t.Errorf("symbol = %q, want BTCUSDT (the terminal keys on the uppercase form)", btc.Symbol)
	}
	if btc.LastPrice != 63200.10 {
		t.Errorf("last price = %v, want 63200.10", btc.LastPrice)
	}
	if btc.ChangePct != -1.865 {
		t.Errorf("change = %v, want -1.865 percent. %v would mean priceChange "+
			"got mapped where priceChangePercent belongs", btc.ChangePct, -1200.5)
	}
	if btc.VolumeQuote != 9876543210.5 {
		t.Errorf("quote volume = %v, want 9876543210.5", btc.VolumeQuote)
	}
	if btc.EventTimeMs != 1786000000000 {
		t.Errorf("event time = %d, want the closeTime 1786000000000", btc.EventTimeMs)
	}
}

// The fallback must fill the watchlist when !ticker@arr delivers nothing, and
// must get out of the way the moment the stream does deliver. Without the
// first half, every watchlist row on a network that blocks the stream shows a
// symbol and no numbers.
func TestTicker24hRESTFallbackFillsThenYields(t *testing.T) {
	serveFixture(t, ticker24hFixture)

	prev := tickerPollInterval
	tickerPollInterval = 20 * time.Millisecond
	t.Cleanup(func() { tickerPollInterval = prev })

	var emits atomic.Int64
	updates := make(chan *pb.Ticker24HUpdate, 1)
	f := NewTickerFeed(quietLog(), func(u *pb.Ticker24HUpdate) {
		emits.Add(1)
		// Non-blocking: a stalled consumer must not pace the poller.
		select {
		case updates <- u:
		default:
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.pollREST(ctx)

	select {
	case u := <-updates:
		if len(u.Entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(u.Entries))
		}
		if u.Entries[0].Symbol != "BTCUSDT" || u.Entries[0].LastPrice != 63200.10 {
			t.Fatalf("fallback entry did not carry the REST values: %+v", u.Entries[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("REST fallback never emitted, so the watchlist would stay blank")
	}

	// One stream frame is enough to retire the fallback: a per-second push
	// beats a 30s poll and the two would otherwise fight over the same rows.
	f.frames.Add(1)
	time.Sleep(3 * tickerPollInterval) // let any in-flight poll land
	settled := emits.Load()
	time.Sleep(10 * tickerPollInterval)
	if extra := emits.Load() - settled; extra != 0 {
		t.Fatalf("fallback kept polling after the stream came alive: %d extra emissions", extra)
	}
}

// A stream push must count, or the fallback never yields.
func TestStreamFrameRetiresTheFallback(t *testing.T) {
	f := NewTickerFeed(quietLog(), func(*pb.Ticker24HUpdate) {})
	f.onMessage(Envelope{Data: []byte(
		`[{"e":"24hrTicker","E":1786000000000,"s":"BTCUSDT","c":"63200.10","P":"-1.865","q":"1"}]`)})
	if f.frames.Load() != 1 {
		t.Fatal("a parsed stream push did not register, so the REST fallback would poll forever")
	}
}
