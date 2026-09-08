package binance

import (
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"testing"
)

func TestTradeSideDuplicatesAndGapReset(t *testing.T) {
	var trades []*pb.Trade
	resets := 0
	f := NewFeed("btcusdt", slog.New(slog.NewTextHandler(io.Discard, nil)), func(_ pb.Stream, _, _ int64, m proto.Message) { trades = append(trades, m.(*pb.Trade)) })
	f.SetTradeReset(func(int64) { resets++ })
	a := []byte(`{"e":"aggTrade","E":1788739200123,"T":1788739200100,"a":10,"p":"100","q":"2","m":true}`)
	f.onAggTrade(a)
	f.onAggTrade(a)
	f.onAggTrade([]byte(`{"e":"aggTrade","E":1788739200124,"T":1788739200101,"a":12,"p":"101","q":"3","m":false}`))
	f.onAggTrade([]byte(`{"e":"aggTrade","T":1788739200102,"a":13,"p":"101","q":"3"}`))
	f.onAggTrade([]byte(`{"e":"aggTrade","T":1788739200102,"a":13,"p":"NaN","q":"3","m":false}`))
	if len(trades) != 2 || trades[0].IsBuy || !trades[1].IsBuy || trades[0].TimestampMs != 1788739200100 || resets != 1 {
		t.Fatalf("trades=%v resets=%d", trades, resets)
	}
}
