package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// RESTBase is the Binance USD-M futures REST host. Override for testnet.
var RESTBase = "https://fapi.binance.com"

var httpClient = &http.Client{Timeout: 15 * time.Second}

func getJSON(ctx context.Context, path string, q url.Values, out any) error {
	u := RESTBase + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// 418 and 429 mean we are rate limited. Surface the code so callers
		// can back off rather than hammer.
		return fmt.Errorf("binance %s: http %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// DepthSnapshot is the REST orderbook snapshot used to seed the diff stream.
type DepthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// Depth fetches an orderbook snapshot. limit must be one of Binance's
// accepted values (5, 10, 20, 50, 100, 500, 1000); 1000 is the deepest and
// what the terminal's DOM ladder wants.
func Depth(ctx context.Context, symbol string, limit int) (*DepthSnapshot, error) {
	var out DepthSnapshot
	err := getJSON(ctx, "/fapi/v1/depth", url.Values{
		"symbol": {upper(symbol)},
		"limit":  {strconv.Itoa(limit)},
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Kline is one historical candle. Binance returns these as heterogeneous
// JSON arrays, hence the custom unmarshal.
type Kline struct {
	OpenTime         int64
	Open             float64
	High             float64
	Low              float64
	Close            float64
	Volume           float64
	CloseTime        int64
	QuoteVolume      float64
	TradeCount       int64
	TakerBuyVolume   float64
	TakerBuyQuoteVol float64
}

func (k *Kline) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if len(raw) < 11 {
		return fmt.Errorf("kline: expected >=11 fields, got %d", len(raw))
	}
	num := func(i int) float64 {
		var s string
		if json.Unmarshal(raw[i], &s) == nil {
			f, _ := strconv.ParseFloat(s, 64)
			return f
		}
		return 0
	}
	i64 := func(i int) int64 {
		var v int64
		_ = json.Unmarshal(raw[i], &v)
		return v
	}
	k.OpenTime = i64(0)
	k.Open, k.High, k.Low, k.Close, k.Volume = num(1), num(2), num(3), num(4), num(5)
	k.CloseTime = i64(6)
	k.QuoteVolume = num(7)
	k.TradeCount = i64(8)
	k.TakerBuyVolume, k.TakerBuyQuoteVol = num(9), num(10)
	return nil
}

// intervalFor maps a timeframe in seconds to a Binance kline interval string.
// Binance has no sub-minute klines, so 1s/5s/15s/30s return false and must be
// built locally from the trade stream.
func intervalFor(tfSec int64) (string, bool) {
	switch tfSec {
	case 60:
		return "1m", true
	case 180:
		return "3m", true
	case 300:
		return "5m", true
	case 900:
		return "15m", true
	case 1800:
		return "30m", true
	case 3600:
		return "1h", true
	case 7200:
		return "2h", true
	case 14400:
		return "4h", true
	case 21600:
		return "6h", true
	case 28800:
		return "8h", true
	case 43200:
		return "12h", true
	case 86400:
		return "1d", true
	case 259200:
		return "3d", true
	case 604800:
		return "1w", true
	default:
		return "", false
	}
}

// Klines fetches historical candles. endTimeMs of 0 means "up to now".
// Binance caps limit at 1500.
func Klines(ctx context.Context, symbol string, tfSec int64, count int, endTimeMs int64) ([]Kline, error) {
	interval, ok := intervalFor(tfSec)
	if !ok {
		return nil, fmt.Errorf("no binance kline interval for timeframe %ds", tfSec)
	}
	if count > 1500 {
		count = 1500
	}
	if count <= 0 {
		count = 500
	}
	q := url.Values{
		"symbol":   {upper(symbol)},
		"interval": {interval},
		"limit":    {strconv.Itoa(count)},
	}
	if endTimeMs > 0 {
		q.Set("endTime", strconv.FormatInt(endTimeMs, 10))
	}
	var out []Kline
	if err := getJSON(ctx, "/fapi/v1/klines", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// PremiumIndex is the REST equivalent of the @markPrice stream. It exists as
// a fallback because the WebSocket mark-price stream is not reachable from
// every network, and without mark price and funding the stats panel is blank.
type PremiumIndex struct {
	MarkPrice       float64
	LastFundingRate float64
	NextFundingTime int64
}

// Premium fetches mark price and funding for a symbol.
func Premium(ctx context.Context, symbol string) (*PremiumIndex, error) {
	var out struct {
		MarkPrice       string `json:"markPrice"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}
	if err := getJSON(ctx, "/fapi/v1/premiumIndex", url.Values{
		"symbol": {upper(symbol)},
	}, &out); err != nil {
		return nil, err
	}
	mark, _ := strconv.ParseFloat(out.MarkPrice, 64)
	rate, _ := strconv.ParseFloat(out.LastFundingRate, 64)
	return &PremiumIndex{
		MarkPrice:       mark,
		LastFundingRate: rate,
		NextFundingTime: out.NextFundingTime,
	}, nil
}

// OpenInterest is the current notional open interest for a symbol.
func OpenInterest(ctx context.Context, symbol string) (float64, error) {
	var out struct {
		OpenInterest string `json:"openInterest"`
		Time         int64  `json:"time"`
	}
	if err := getJSON(ctx, "/fapi/v1/openInterest", url.Values{
		"symbol": {upper(symbol)},
	}, &out); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(out.OpenInterest, 64)
}

// ExchangeSymbols returns the tradable USD-M perpetual symbols, lowercased.
// Used to validate what a client asks for before opening an upstream socket.
func ExchangeSymbols(ctx context.Context) (map[string]bool, error) {
	var out struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
			Status string `json:"status"`
		} `json:"symbols"`
	}
	if err := getJSON(ctx, "/fapi/v1/exchangeInfo", nil, &out); err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(out.Symbols))
	for _, s := range out.Symbols {
		if s.Status == "TRADING" {
			set[lower(s.Symbol)] = true
		}
	}
	return set, nil
}
