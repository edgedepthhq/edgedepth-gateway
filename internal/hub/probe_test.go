package hub_test

// This is a live integration probe, not a unit test. It stands up the real
// gateway, points it at real Binance, subscribes exactly the way the terminal
// does, and asserts that decoded frames carry sane values.
//
// It is skipped unless EDGEDEPTH_LIVE=1 because it needs outbound network.
//
//	EDGEDEPTH_LIVE=1 go test ./internal/hub -run TestLive -v

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/internal/binance"
	"github.com/edgedepthhq/edgedepth-gateway/internal/hub"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

func subMsg(method, symbol string, stream pb.Stream, tf int64) string {
	return fmt.Sprintf(
		`{"method":%q,"data":{"pair":{"exchange":"binancef","symbol":%q},"stream":%d,"timeframe":%d}}`,
		method, symbol, int32(stream), tf)
}

func TestLiveBinanceRoundTrip(t *testing.T) {
	if os.Getenv("EDGEDEPTH_LIVE") != "1" {
		t.Skip("set EDGEDEPTH_LIVE=1 to run the live Binance probe")
	}

	// Default to aggTrade (the shipping default); override to prove the
	// fallback path on networks where aggTrade is unreachable.
	if ts := os.Getenv("BINANCE_TRADE_STREAM"); ts != "" {
		binance.TradeStream = ts
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	h := hub.New(log, binance.New(log))

	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer srv.Close()

	u := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer conn.Close()

	const sym = "btcusdt"
	subs := []string{
		subMsg("subscribe", sym, pb.Stream_STREAM_TRADES, 0),
		subMsg("subscribe", sym, pb.Stream_STREAM_ORDERBOOK, 0),
		subMsg("subscribe", sym, pb.Stream_STREAM_LIQUIDATIONS, 0),
		subMsg("subscribe", sym, pb.Stream_STREAM_CANDLES, 1),
		subMsg("subscribe", sym, pb.Stream_STREAM_STATS, 1),
		subMsg("subscribe", "global", pb.Stream_STREAM_TICKER24H, 0),
		// The terminal always asks for this too; it must not break anything.
		subMsg("subscribe", "global", pb.Stream_STREAM_TICKER24H, 0),
	}
	for _, s := range subs {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(s)); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}

	// And a historical backfill request, the thing the plan assumed we could
	// not serve.
	hist, _ := json.Marshal(map[string]any{
		"method": "get_historical_candles",
		"data": map[string]any{
			"pair":      map[string]string{"exchange": "binancef", "symbol": sym},
			"timeframe": 60,
			"count":     200,
		},
	})
	if err := conn.WriteMessage(websocket.TextMessage, hist); err != nil {
		t.Fatalf("historical request: %v", err)
	}

	var lastStat atomic.Value
	var (
		mu      sync.Mutex
		seen    = map[pb.Stream]int{}
		samples = map[pb.Stream]string{}
	)

	deadline := time.Now().Add(45 * time.Second)
	_ = conn.SetReadDeadline(deadline)

	for time.Now().Before(deadline) {
		mt, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if mt != websocket.BinaryMessage {
			continue
		}

		var env pb.WSPayload
		if err := proto.Unmarshal(raw, &env); err != nil {
			t.Fatalf("frame is not a WSPayload: %v", err)
		}
		if env.Pair == nil {
			t.Fatalf("frame missing pair, stream=%v", env.Stream)
		}

		mu.Lock()
		seen[env.Stream]++
		first := samples[env.Stream] == ""
		mu.Unlock()

		if env.Stream == pb.Stream_STREAM_STATS {
			var b pb.Stats
			if proto.Unmarshal(env.Data, &b) == nil && len(b.Values) > 0 {
				// Keep the first mark-priced stat once one arrives: the mark
				// comes from the REST premiumIndex poll on networks without
				// the markPrice stream, and later flushes never regress it.
				if cur, ok := lastStat.Load().(*pb.Stat); !ok || cur.MarkPrice <= 0 {
					lastStat.Store(b.Values[0])
				}
			}
		}

		if !first {
			continue
		}

		desc, err := describe(&env)
		if err != nil {
			t.Fatalf("decode inner for stream %v: %v", env.Stream, err)
		}
		mu.Lock()
		samples[env.Stream] = desc
		mu.Unlock()
		t.Logf("first %-28s %s", env.Stream.String(), desc)

		// Stop once we have the streams that do not depend on a liquidation
		// happening to fire during the window, AND a mark price. The mark
		// arrives via the REST premiumIndex poll a few hundred ms in, later
		// than the first stats flush, so leaving early without it makes the
		// assertion below race the poll.
		mu.Lock()
		enough := samples[pb.Stream_STREAM_TRADES] != "" &&
			samples[pb.Stream_STREAM_ORDERBOOK] != "" &&
			samples[pb.Stream_STREAM_CANDLES] != "" &&
			samples[pb.Stream_STREAM_STATS] != "" &&
			samples[pb.Stream_STREAM_HISTORICAL_CANDLES] != "" &&
			true
		mu.Unlock()
		if enough {
			if st, ok := lastStat.Load().(*pb.Stat); ok && st.MarkPrice > 0 {
				break
			}
		}
	}

	// Streams this probe can prove end to end.
	required := []pb.Stream{
		pb.Stream_STREAM_TRADES,
		pb.Stream_STREAM_ORDERBOOK,
		pb.Stream_STREAM_CANDLES,
		pb.Stream_STREAM_STATS,
		pb.Stream_STREAM_HISTORICAL_CANDLES,
	}
	for _, st := range required {
		if seen[st] == 0 {
			t.Errorf("no frames received for %v", st)
		}
	}

	// Streams that depend on upstream feeds which are not reachable from
	// every network. They are reported, never asserted: a liquidation may
	// simply not have happened during the window, and some networks do not
	// serve the all-market ticker at all. Absence here is not proof of a bug,
	// but it is also not proof the path works, so say which it was.
	for _, st := range []pb.Stream{pb.Stream_STREAM_TICKER24H, pb.Stream_STREAM_LIQUIDATIONS} {
		if seen[st] == 0 {
			t.Logf("UNVERIFIED: no %v frames arrived; this path is untested by this run", st)
		}
	}

	// The stats panel is blank without a mark price. The WebSocket markPrice
	// stream is not reachable everywhere, so this doubles as a check that the
	// REST premiumIndex fallback filled it in.
	if v := lastStat.Load(); v == nil {
		t.Errorf("no stats sample captured")
	} else if st := v.(*pb.Stat); st.MarkPrice <= 0 {
		t.Errorf("stats never got a mark price (REST premiumIndex fallback did not fill in): %+v", st)
	} else {
		t.Logf("stats mark=%.2f funding=%.6f oi_usd=%.0f", st.MarkPrice, st.Funding, st.OpenInterestUsd)
	}

	t.Logf("frame counts: %v", seen)
}

// describe decodes the inner message and sanity-checks it, returning a short
// human summary. A field that decodes but is zero where it must not be is a
// silent wire mismatch, which is exactly the failure mode worth catching.
func describe(env *pb.WSPayload) (string, error) {
	switch env.Stream {
	case pb.Stream_STREAM_TRADES:
		var m pb.Trade
		if err := proto.Unmarshal(env.Data, &m); err != nil {
			return "", err
		}
		if m.Price <= 0 || m.Qty <= 0 {
			return "", fmt.Errorf("implausible trade: %+v", &m)
		}
		if m.TimestampMs < 1e12 {
			return "", fmt.Errorf("timestamp %d is not a ms epoch (wrong JSON field mapped?)", m.TimestampMs)
		}
		return fmt.Sprintf("price=%.2f qty=%g is_buy=%v ts=%d",
			m.Price, m.Qty, m.IsBuy, m.TimestampMs), nil

	case pb.Stream_STREAM_ORDERBOOK:
		var m pb.BookUpdate
		if err := proto.Unmarshal(env.Data, &m); err != nil {
			return "", err
		}
		return fmt.Sprintf("snapshot=%v bids=%d asks=%d last_u=%d",
			m.Snapshot, len(m.Bids), len(m.Asks), m.LastUpdateId), nil

	case pb.Stream_STREAM_CANDLES, pb.Stream_STREAM_HISTORICAL_CANDLES:
		var m pb.Candles
		if err := proto.Unmarshal(env.Data, &m); err != nil {
			return "", err
		}
		if len(m.Values) == 0 {
			return "empty batch", nil
		}
		c := m.Values[0]
		if m.Timeframe != env.Timeframe {
			return "", fmt.Errorf("timeframe mismatch: envelope=%d inner=%d",
				env.Timeframe, m.Timeframe)
		}
		return fmt.Sprintf("tf=%ds n=%d first o=%.2f h=%.2f l=%.2f c=%.2f vol=%g final=%v",
			m.Timeframe, len(m.Values), c.Open, c.High, c.Low, c.Close, c.Volume, c.Final), nil

	case pb.Stream_STREAM_STATS:
		var m pb.Stats
		if err := proto.Unmarshal(env.Data, &m); err != nil {
			return "", err
		}
		if len(m.Values) == 0 {
			return "empty batch", nil
		}
		s := m.Values[0]
		return fmt.Sprintf("tf=%ds mark=%.2f funding=%.6f oi_usd=%.0f",
			m.Timeframe, s.MarkPrice, s.Funding, s.OpenInterestUsd), nil

	case pb.Stream_STREAM_LIQUIDATIONS:
		var m pb.Liquidation
		if err := proto.Unmarshal(env.Data, &m); err != nil {
			return "", err
		}
		if m.Price <= 0 {
			return "", fmt.Errorf("implausible liquidation: %+v", &m)
		}
		if m.TimestampMs < 1e12 {
			return "", fmt.Errorf("liquidation timestamp %d is not a ms epoch", m.TimestampMs)
		}
		return fmt.Sprintf("price=%.2f qty=%g is_buy=%v ts=%d",
			m.Price, m.Qty, m.IsBuy, m.TimestampMs), nil

	case pb.Stream_STREAM_TICKER24H:
		var m pb.Ticker24HUpdate
		if err := proto.Unmarshal(env.Data, &m); err != nil {
			return "", err
		}
		if len(m.Entries) == 0 {
			return "", fmt.Errorf("empty ticker batch")
		}
		e := m.Entries[0]
		return fmt.Sprintf("entries=%d first=%s last=%.4f chg=%.2f%%",
			len(m.Entries), e.Symbol, e.LastPrice, e.ChangePct), nil
	}
	return "unhandled stream", nil
}
