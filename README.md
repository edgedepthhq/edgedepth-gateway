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
| Stats: mark price, funding, open interest | yes | REST polled, so they fill on a symbol that has not traded |
| Watchlist prices and 24h change | yes | `!ticker@arr`, falling back to REST when that stream is blocked |
| Liquidations and the liquidation Field | yes | Field is computed client side from candles |
| Paper trading | yes | entirely client side |
| Historical backfill (1m and above) | yes | Binance REST klines |
| VPIN, positioning, modelled liq heatmap | no | hosted backend only |
| Patterns, scanner scores, contagion | no | hosted backend only |
| VPVR / footprint history | bounded | local candidate: closed observed minutes after warmup, up to 60 minutes / 50,000 price-minute cells per active symbol |
| TPO | candle approximation | terminal builds 30m blocks from available candle ranges |

Sub-minute candles are accumulated from individual trades as they arrive. The
building candle therefore moves trade by trade instead of waiting for a closed
bar. The terminal renders them because its entitlements default to Pro when no
host globals are present.

## Observed volume history (local candidate)

These changes are not yet published in release images. Build this checkout to
try them. A fresh subscription has no stored trade history: aggregation starts
at the next minute boundary, and the first trade in a later minute closes it.
Expect roughly one to two minutes of warmup on an active market, longer on a
quiet one. No trades is not proof of a complete empty minute.

A reconnect, aggregate-ID gap, or regressing event time discards the current
partial minute. Older closed buckets remain until evicted. History retains at
most 60 minutes and 50,000 price-minute cells per active symbol. At most 16
symbols can be active; the last unsubscribe releases that symbol and its
history. A process restart loses all volume history. There is no REST trade
backfill, persistent storage, or guarantee of complete venue-wide volume.

When a minute alone exceeds the cell budget, it is discarded. Missing minutes
remain gaps. Requests select whole minutes inside `[start_time, end_time)` and
not after now. Profile bounds report the first/last included minute; they do
not certify continuity between them. POC ties choose the lower price; value
area expands adjacent rows around POC to at least 70% of available volume.

Quantities are base-asset units for the USD-M symbols this adapter supports.
Buyer-is-maker means aggressor sell; missing maker flags are rejected. CM
messages (`st=2`) are excluded from the trade path. Counts are received aggregate
messages (or raw trade messages), not an estimate of distinct taker orders.
Duplicate IDs do not add volume twice.

`get_footprint_history` emits stream-17 `TickVolumeUpdate` messages with raw
nested `TickVolumeLevels` protobuf, then an empty stream-17 sentinel even when
cold. `get_volume_profile` emits stream-26 `VolumeProfileResponse`, empty when
unavailable. The terminal accepts this existing format without a new protocol.
Only subscribed feeds that implement `exchange.TradeContinuity` produce stored
volume history; new adapters must report resets before accepting a new sequence.

The client queue is capped at 1,024 frames **and 16 MiB** including an in-flight
write. A slow client disconnects instead of silently losing order-book deltas.
Control frames are capped at 64 KiB. These are local-workbench bounds, not a
public multi-tenant service capacity guarantee.

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

**Current Binance routing:** trades, mark price, liquidation and ticker streams
use `/market/stream`; depth uses a separate `/public/stream` connection. The
base override is the host root (for example `wss://fstream.binance.com`), without
a route suffix. Legacy combined endpoints can leave the book moving while the
tape is empty. Check the image revision and logs before trying an override.
The default `aggTrade` route was tested live for this candidate. `trade` remains
an experimental compatibility option, not a promise of availability.

The all-market `!ticker@arr` stream is blocked on some of the same networks,
which would leave every watchlist row showing a symbol and no numbers. That
one needs no flag: the gateway notices the silence and serves the same data
from REST every 30 seconds instead, and says so in the log.

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
8 historical candles, 17 closed tick-volume history, 26 volume profile, and
29 ticker24h. `get_historical_candles` is answered from
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
go test -race ./...
go vet ./...

# Live probe against real Binance: subscribes the way the terminal does and
# asserts the decoded frames carry sane values.
EDGEDEPTH_LIVE=1 go test ./internal/hub -run TestLive -v
```

Both suites exist to catch one failure mode: a field that decodes cleanly into
the wrong place. Nothing errors, the panel just goes blank or shows a plausible
wrong number, usually minutes later.

`internal/hub/wire_shape_test.go` needs no network. A stub venue drives a trade
through the real subscribe, aggregate, flush and encode path, and every frame is
then decoded the way `c-based-trader-client` decodes it rather than the way this
gateway encoded it. That distinction is the whole point: live candles went out
wrapped in the plural `Candles` while the terminal parsed a singular `Candle`,
and because proto3 skips mismatched fields as unknown, the parse succeeded and
handed the chart a candle of all zeros.

The live probe covers what a stub cannot, which is Binance's own JSON. Two real
examples it caught: Binance sends `e` and `E` in the same object and Go's
case-insensitive fallback puts the event type string into the event time int;
the `@trade` payload sends `T` and `t`, which lands the trade id in the
timestamp and produces a number that looks plausible and is off by three orders
of magnitude.

## License

MIT. The terminal itself is AGPL-3.0; this gateway is deliberately separate
and permissive so it can be embedded anywhere.
