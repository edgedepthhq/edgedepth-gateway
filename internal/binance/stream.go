package binance

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// WSBase is the Binance USD-M futures stream host. Override for testnet.
var WSBase = "wss://fstream.binance.com"

// maxStreamsPerConn is Binance's documented cap for a single combined-stream
// connection. Exceeding it gets the connection closed, so callers that need
// more must open several Streams.
const maxStreamsPerConn = 200

// Envelope is the combined-stream wrapper Binance puts around every message.
type Envelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// Stream is a self-healing connection to one Binance combined-stream URL.
// It reconnects with jittered exponential backoff and, because Binance drops
// every connection at the 24 hour mark, treats a clean close as routine.
type Stream struct {
	names   []string
	onMsg   func(Envelope)
	onReset func() // called after every (re)connect so callers can resync state

	log *slog.Logger
}

// NewStream builds a stream for the given raw Binance stream names, e.g.
// "btcusdt@aggTrade". onReset fires after each successful connect, including
// reconnects: any state derived from a continuous sequence (an orderbook, for
// one) must be rebuilt there.
func NewStream(names []string, log *slog.Logger, onMsg func(Envelope), onReset func()) *Stream {
	return &Stream{names: names, onMsg: onMsg, onReset: onReset, log: log}
}

// Run blocks until ctx is cancelled, keeping the connection alive throughout.
func (s *Stream) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := s.dial(ctx)
		if ctx.Err() != nil {
			return
		}
		// A connection that survived a while then dropped is normal (Binance
		// cuts every socket at 24h). Only escalate backoff on fast failures,
		// which is what a real outage or a bad stream name looks like.
		if time.Since(start) > time.Minute {
			backoff = time.Second
		}
		if err != nil {
			s.log.Warn("binance stream dropped, reconnecting",
				"err", err, "backoff", backoff.String())
		}
		jitter := time.Duration(rand.Int63n(int64(backoff/2 + 1)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff + jitter):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Stream) dial(ctx context.Context) error {
	// Binance stream names are built from a known-safe alphabet: lowercase
	// symbols, digits, "@", "!", "." and the "/" separator. Percent-encoding
	// them and unescaping afterwards is how "!ticker@arr" silently became
	// "%21ticker@arr" and returned a connection that never delivered a frame.
	// Join them verbatim instead.
	u := WSBase + "/stream?streams=" + strings.Join(s.names, "/")

	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, u, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	s.log.Info("binance stream connected", "streams", len(s.names))
	if s.onReset != nil {
		s.onReset()
	}

	// Binance pings every few minutes; gorilla answers pongs automatically.
	// The read deadline is the actual liveness check: no traffic at all for
	// 10 minutes means the socket is dead even if TCP has not noticed.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
		return conn.WriteControl(websocket.PongMessage, []byte(appData),
			time.Now().Add(10*time.Second))
	})

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
		var env Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			s.log.Debug("binance stream: unparsable frame", "err", err)
			continue
		}
		s.onMsg(env)
	}
}

// Chunk splits stream names into connection-sized groups.
func Chunk(names []string) [][]string {
	var out [][]string
	for len(names) > maxStreamsPerConn {
		out = append(out, names[:maxStreamsPerConn])
		names = names[maxStreamsPerConn:]
	}
	if len(names) > 0 {
		out = append(out, names)
	}
	return out
}

func upper(s string) string { return strings.ToUpper(s) }
func lower(s string) string { return strings.ToLower(s) }
