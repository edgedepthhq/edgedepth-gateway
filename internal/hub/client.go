package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/edgedepthhq/edgedepth-gateway/internal/wire"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// sendBuffer is how many encoded frames may queue for one client before it is
// considered too slow. Dropping frames is not an option for the orderbook:
// a missed diff silently corrupts the client's book. Disconnecting instead
// forces a reconnect and a fresh snapshot, which is recoverable.
const sendBuffer = 1024

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// The terminal is served from a different origin than this gateway by
	// design: you point a hosted client at your own localhost. Origin checks
	// would defeat the entire purpose.
	CheckOrigin: func(*http.Request) bool { return true },
}

// Client is one connected terminal.
type Client struct {
	conn *websocket.Conn
	send chan []byte
	log  *slog.Logger

	mu   sync.RWMutex
	keys map[wire.Key]struct{}

	closeOnce sync.Once
	done      chan struct{}
}

func newClient(conn *websocket.Conn, log *slog.Logger) *Client {
	return &Client{
		conn: conn,
		send: make(chan []byte, sendBuffer),
		log:  log,
		keys: make(map[wire.Key]struct{}),
		done: make(chan struct{}),
	}
}

// Send queues an encoded frame. It never blocks: if the buffer is full the
// client is closed rather than allowed to stall the broadcast path.
func (c *Client) Send(b []byte) {
	select {
	case c.send <- b:
	default:
		c.log.Warn("client too slow, closing", "buffered", len(c.send))
		c.close()
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *Client) addKey(k wire.Key) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.keys[k]; ok {
		return false
	}
	c.keys[k] = struct{}{}
	return true
}

func (c *Client) removeKey(k wire.Key) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.keys[k]; !ok {
		return false
	}
	delete(c.keys, k)
	return true
}

func (c *Client) subscribed(k wire.Key) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.keys[k]
	return ok
}

// Keys snapshots the current subscriptions.
func (c *Client) Keys() []wire.Key {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]wire.Key, 0, len(c.keys))
	for k := range c.keys {
		out = append(out, k)
	}
	return out
}

// ── control plane ───────────────────────────────────────────────────────────

// request is the JSON control message the terminal sends.
//
// The key is "method", not "type". This is worth stating plainly because the
// backend's own docs and several call sites talk about message "type", but
// what the client actually puts on the wire is create_subscribe_message in
// types.cpp, and that writes "method".
type request struct {
	Method string `json:"method"`
	Data   struct {
		Pair struct {
			Exchange string `json:"exchange"`
			Symbol   string `json:"symbol"`
		} `json:"pair"`
		Stream    int32 `json:"stream"`
		Timeframe int64 `json:"timeframe"`
		Count     int   `json:"count"`
		EndTime   int64 `json:"end_time"`
	} `json:"data"`
}

func (r *request) key() wire.Key {
	k := wire.Key{
		Exchange: r.Data.Pair.Exchange,
		Symbol:   r.Data.Pair.Symbol,
		Stream:   pb.Stream(r.Data.Stream),
	}
	if wire.Timeframed(k.Stream) {
		k.Timeframe = r.Data.Timeframe
	}
	return k
}

// ServeWS upgrades an HTTP request and runs the client until it disconnects.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warn("websocket upgrade failed", "err", err)
		return
	}
	c := newClient(conn, h.log)
	h.AddClient(c)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go c.writeLoop()
	h.readLoop(ctx, c)

	c.close()
	h.RemoveClient(c)
}

func (c *Client) writeLoop() {
	// A periodic ping detects a peer that vanished without a close frame,
	// which is the common case for a closed browser tab.
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-c.done:
			return
		case b := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(20 * time.Second))
			if err := c.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
				c.close()
				return
			}
		case <-ping.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.close()
				return
			}
		}
	}
}

func (h *Hub) readLoop(ctx context.Context, c *Client) {
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	})
	_ = c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	for {
		mt, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		if mt != websocket.TextMessage {
			continue // the control plane is JSON text only
		}

		var req request
		if err := json.Unmarshal(raw, &req); err != nil {
			h.log.Debug("unparsable control message", "err", err)
			continue
		}
		h.handle(ctx, c, &req)
	}
}

func (h *Hub) handle(ctx context.Context, c *Client, req *request) {
	switch req.Method {
	case "subscribe":
		h.Subscribe(c, req.key())

	case "unsubscribe":
		h.Unsubscribe(c, req.key())

	case "get_historical_candles":
		ex := req.Data.Pair.Exchange
		sym := req.Data.Pair.Symbol
		tf := req.Data.Timeframe
		count := req.Data.Count
		end := req.Data.EndTime
		// REST call: do not block the read loop, or a slow venue response
		// stalls every later control message from this client.
		go h.HistoricalCandles(ctx, c, ex, sym, tf, count, end)

	case "get_footprint_history", "get_volume_profile",
		"get_historical_heatmap", "get_replay_preview_candles",
		"get_historical_vpin":
		// Served only by the hosted backend. Staying silent is correct: the
		// terminal treats these as best-effort and renders without them.
		h.log.Debug("unsupported request ignored", "method", req.Method)

	default:
		h.log.Debug("unknown method", "method", req.Method)
	}
}
