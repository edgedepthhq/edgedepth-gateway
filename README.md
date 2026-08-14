# EdgeDepth Gateway

A small Go binary that bridges Binance's public USD-M futures streams into the
[EdgeDepth Terminal](https://github.com/edgedepthhq/edgedepth-terminal) wire
format, on localhost.

The terminal is a client. It speaks protobuf over WebSocket and connects to
whatever feed you point it at. This is a feed you can run yourself, with one
command, using no API key and no account.

```
Binance public streams  ->  edgedepth-gateway  ->  ws://localhost:8080/ws  ->  terminal
```

## Quick start

```bash
docker compose up
```

Then open the terminal against it:

```
https://app.edgedepth.com/terminal/btcusdt?ws=ws://localhost:8080/ws
```

Or without Docker:

```bash
go build ./cmd/edgedepth-gateway && ./edgedepth-gateway
```

If your browser refuses the insecure WebSocket from a hosted HTTPS page, run
the terminal locally instead and point it at the same URL. Browsers treat
`localhost` as a trustworthy origin, so this usually just works, but the local
terminal is the guaranteed path.

## What you get

Everything here is computed from Binance's free public data. No key, no tier.

| Feature | Works | Notes |
| --- | --- | --- |
| Candlestick chart | yes | history from Binance REST klines |
| 1s / 5s / 15s / 30s candles | yes | built trade by trade from the raw stream |
| DOM ladder and orderbook | yes | REST snapshot plus diff stream, sequence checked |
| Trade tape | yes | |
| Stats: mark price, funding, open interest | yes | |
| Liquidations and the liquidation Field | yes | Field is computed client side from candles |
| VPVR, TPO, paper trading, watchlist | yes | all client side |
| Historical backfill (1m and above) | yes | Binance REST klines |
| VPIN, positioning, modelled liq heatmap | no | hosted backend only |
| Patterns, scanner scores, contagion | no | hosted backend only |
| Footprint history, volume profile history | no | live only, no REST source |

Sub-minute candles are accumulated from individual trades as they arrive. The
building candle therefore moves trade by trade instead of waiting for a closed
bar. The terminal renders them because its entitlements default to Pro when no
host globals are present.

## Configuration

Every flag has an environment variable equivalent.

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `-addr` | `EDGEDEPTH_ADDR` | `:8080` | listen address |
| `-path` | `EDGEDEPTH_PATH` | `/ws` | WebSocket path |
| `-log` | `EDGEDEPTH_LOG` | `info` | `debug`, `info`, `warn`, `error` |
| `-trade-stream` | `BINANCE_TRADE_STREAM` | `aggTrade` | `aggTrade` or `trade` |
| `-binance-rest` | `BINANCE_REST` | Binance | override REST base URL |
| `-binance-ws` | `BINANCE_WS` | Binance | override stream base URL |

**If the tape stays empty while the orderbook updates**, start with
`-trade-stream=trade`. Some networks do not serve Binance's `@aggTrade`
stream. The gateway logs a warning naming this exact fix when it sees a live
orderbook and no trades after 30 seconds.

## How it works

The contract is small enough to describe in full.

**Downstream (gateway to terminal)** is one `WSPayload` protobuf per binary
WebSocket frame. No length prefix, no stream-id header. `WSPayload.stream`
carries the stream id and `WSPayload.data` carries the marshalled inner
message. Compression is optional and detected by content: the terminal sniffs
the zstd magic and passes anything else through untouched, so this gateway
sends plain protobuf.

**Upstream (terminal to gateway)** is JSON text frames keyed on `method`:

```json
{"method":"subscribe","data":{"pair":{"exchange":"binancef","symbol":"btcusdt"},"stream":1,"timeframe":0}}
```

Streams served: 1 trades, 2 candles, 3 orderbook, 4 stats, 5 liquidations,
8 historical candles, 29 ticker24h. `get_historical_candles` is answered from
Binance REST. Requests the hosted backend owns are ignored, and the terminal
renders without them.

`proto/edgedepth.proto` is the whole contract. Field numbers must match the
terminal's `protos/messages.proto` exactly, because a mismatch fails silently
rather than loudly.

**Venues are pluggable.** Everything Binance-specific sits behind the
`Exchange` interface in `internal/exchange`; the hub only knows streams,
candle aggregation and fan-out. Adding Bybit, OKX, Hyperliquid or anything
else with public market data is one adapter package plus one registration
line. [CONTRIBUTING.md](CONTRIBUTING.md) has the walkthrough.

## Development

```bash
go build ./...
go test ./...

# Live probe against real Binance: subscribes the way the terminal does and
# asserts the decoded frames carry sane values.
EDGEDEPTH_LIVE=1 go test ./internal/hub -run TestLive -v
```

The live probe is the useful one. It catches the failure mode this wire format
is prone to, which is a field that decodes cleanly into the wrong place. Two
real examples, both caught by it and both fixed here: Binance sends `e` and
`E` in the same object, and Go's case-insensitive JSON fallback puts the event
type string into the event time int; the `@trade` payload likewise sends `T`
and `t`, which lands the trade id in the timestamp and produces a plausible
looking number that is off by three orders of magnitude.

## License

MIT. The terminal itself is AGPL-3.0; this gateway is deliberately separate
and permissive so it can be embedded anywhere.
