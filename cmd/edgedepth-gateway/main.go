// Command edgedepth-gateway bridges Binance's public futures streams into the
// EdgeDepth terminal wire format on localhost.
//
// Point the terminal at it:
//
//	https://app.edgedepth.com/terminal/btcusdt?ws=ws://localhost:8080/ws
//
// No API key, no account. Binance's public market data needs neither.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/internal/binance"
	"github.com/edgedepthhq/edgedepth-gateway/internal/hub"
)

func main() {
	var (
		addr    = flag.String("addr", envOr("EDGEDEPTH_ADDR", ":8080"), "listen address")
		path    = flag.String("path", envOr("EDGEDEPTH_PATH", "/ws"), "websocket path")
		logLvl  = flag.String("log", envOr("EDGEDEPTH_LOG", "info"), "log level: debug, info, warn, error")
		restURL = flag.String("binance-rest", envOr("BINANCE_REST", ""), "override Binance REST base URL")
		wsURL   = flag.String("binance-ws", envOr("BINANCE_WS", ""), "override Binance stream base URL")
		tradeSt = flag.String("trade-stream", envOr("BINANCE_TRADE_STREAM", "aggTrade"),
			"Binance trade stream: aggTrade (aggregated per taker order) or trade (raw per fill). "+
				"Switch to trade if the tape stays empty while the orderbook updates.")
	)
	flag.Parse()

	if *tradeSt != "aggTrade" && *tradeSt != "trade" {
		fmt.Fprintf(os.Stderr, "invalid -trade-stream %q: want aggTrade or trade\n", *tradeSt)
		os.Exit(2)
	}
	binance.TradeStream = *tradeSt

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLevel(*logLvl),
	}))

	if *restURL != "" {
		binance.RESTBase = strings.TrimSuffix(*restURL, "/")
	}
	if *wsURL != "" {
		binance.WSBase = strings.TrimSuffix(*wsURL, "/")
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The venue registry. Adding an exchange is one adapter package plus one
	// entry here; see CONTRIBUTING.md.
	h := hub.New(log, binance.New(log))

	// The symbol whitelist is a nicety, not a requirement: without it the
	// gateway still runs and the venue rejects bad symbols itself.
	loadCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	_ = h.LoadSymbols(loadCtx)
	cancel()

	mux := http.NewServeMux()
	mux.HandleFunc(*path, h.ServeWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("edgedepth-gateway listening",
			"addr", *addr, "path", *path,
			"connect", "ws://localhost"+*addr+*path)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
