package binance

import (
	"context"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMarketAndDepthRoutes(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if NewStream([]string{"btcusdt@aggTrade"}, log, nil, nil).route != "/market" {
		t.Fatal("trade route")
	}
	if NewStream([]string{"btcusdt@depth@100ms"}, log, nil, nil).route != "/public" {
		t.Fatal("depth route")
	}
	if NewStream([]string{"!ticker@arr"}, log, nil, nil).route != "/market" {
		t.Fatal("ticker route")
	}
}

func TestBufferedDepthMustBeContinuous(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"lastUpdateId":100,"bids":[["99","2"]],"asks":[["101","3"]]}`))
	}))
	defer server.Close()
	previous := RESTBase
	RESTBase = server.URL
	defer func() { RESTBase = previous }()
	var frames []*pb.BookUpdate
	f := NewFeed("btcusdt", slog.New(slog.NewTextHandler(io.Discard, nil)), func(_ pb.Stream, _, _ int64, m proto.Message) { frames = append(frames, m.(*pb.BookUpdate)) })
	f.pending = []depthDiff{{FirstID: 99, FinalID: 102}, {FirstID: 103, FinalID: 104, PrevID: 101}}
	f.resync(context.Background(), 0)
	if f.synced || len(frames) > 0 {
		t.Fatal("buffered gap published as a valid book")
	}
	f.pending = []depthDiff{{FirstID: 99, FinalID: 102}, {FirstID: 103, FinalID: 104, PrevID: 102}}
	f.resync(context.Background(), 0)
	if !f.synced || len(frames) != 1 || !frames[0].Snapshot || frames[0].LastUpdateId != 104 {
		t.Fatal("continuous snapshot not emitted")
	}
	f.onDepth([]byte(`{"e":"depthUpdate","E":123,"U":103,"u":104,"pu":102,"b":[],"a":[]}`))
	if len(frames) != 1 {
		t.Fatal("duplicate diff emitted")
	}
	f.onDepth([]byte(`{"e":"depthUpdate","E":124,"U":105,"u":106,"pu":104,"b":[["99","0"]],"a":[]}`))
	if len(frames) != 2 || frames[1].Snapshot || len(f.bids) != 0 {
		t.Fatal("continuous diff/deletion failed")
	}
	// An older connection's asynchronous snapshot must not overwrite a new book.
	f.depthEpoch = 1
	f.resync(context.Background(), 0)
	if len(frames) != 2 {
		t.Fatal("stale connection snapshot escaped")
	}
}
