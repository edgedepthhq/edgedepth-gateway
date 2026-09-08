package hub

import (
	"github.com/gorilla/websocket"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Large history responses must disconnect a stalled reader even before the
// frame-count limit. A live book cannot safely recover from silently lost diffs.
func TestClientByteBudgetDisconnects(t *testing.T) {
	clients := make(chan *Client, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		clients <- newClient(conn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}))
	defer server.Close()
	socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	c := <-clients
	defer c.close()
	block := make([]byte, 1<<20)
	for i := 0; i < 16; i++ {
		c.Send(block)
	}
	select {
	case <-c.done:
		t.Fatal("closed within budget")
	default:
	}
	c.Send(block)
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("did not disconnect")
	}
	if len(c.send) != 16 || c.queuedBytes.Load() != sendBytes {
		t.Fatal("unbounded queued bytes")
	}
	c.Send(block)
	if len(c.send) != 16 {
		t.Fatal("accepted more data after close")
	}
}
