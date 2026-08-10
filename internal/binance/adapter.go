package binance

import (
	"context"
	"log/slog"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/internal/candle"
	"github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Binance is the exchange.Exchange adapter for Binance USD-M futures. It is
// the reference venue implementation: transport in stream.go, JSON shapes and
// the orderbook sync procedure in feed.go, REST in rest.go.
type Binance struct {
	log *slog.Logger
}

// New creates the adapter.
func New(log *slog.Logger) *Binance {
	return &Binance{log: log}
}

// ID is "binancef" because that is the venue string the terminal uses for
// Binance futures. It predates this gateway and cannot change here.
func (b *Binance) ID() string { return "binancef" }

// Symbols returns the exchangeInfo whitelist.
func (b *Binance) Symbols(ctx context.Context) (map[string]bool, error) {
	return ExchangeSymbols(ctx)
}

// NewFeed creates the per-symbol feed.
func (b *Binance) NewFeed(symbol string, emit exchange.Emit) exchange.Feed {
	return NewFeed(symbol, b.log, emit)
}

// GlobalTicker streams !ticker@arr, the all-market 24h ticker.
func (b *Binance) GlobalTicker(emit exchange.Emit) exchange.Runner {
	return NewTickerFeed(b.log, func(u *pb.Ticker24HUpdate) {
		emit(pb.Stream_STREAM_TICKER24H, 0, u.TimestampMs, u)
	})
}

// HistoricalCandles serves REST klines for 1m and up. Sub-minute timeframes
// have no REST source on Binance and legitimately start empty, filling from
// live trades.
func (b *Binance) HistoricalCandles(ctx context.Context, symbol string, tfSec int64, count int, endTimeMs int64) ([]*pb.Candle, error) {
	if candle.SubMinute(tfSec) {
		return nil, exchange.ErrUnsupported
	}
	kl, err := Klines(ctx, symbol, tfSec, count, endTimeMs)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.Candle, 0, len(kl))
	for i := range kl {
		k := &kl[i]
		// Binance gives taker BUY volume; the sell side is the remainder.
		vsell := k.Volume - k.TakerBuyVolume
		if vsell < 0 {
			vsell = 0
		}
		out = append(out, &pb.Candle{
			Open:        k.Open,
			High:        k.High,
			Low:         k.Low,
			Close:       k.Close,
			Volume:      k.Volume,
			Vbuy:        k.TakerBuyVolume,
			Vsell:       vsell,
			TimestampMs: k.OpenTime,
			Timeframe:   tfSec,
			// The final kline is still forming unless its close time has passed.
			Final: k.CloseTime < time.Now().UnixMilli(),
		})
	}
	return out, nil
}
