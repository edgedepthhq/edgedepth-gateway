# Contributing

Bug reports and pull requests are welcome. Keep changes small and verified:
`go build ./... && go vet ./... && go test ./...` must pass, and if your
change touches the live data path, run the probe against real Binance:

```bash
EDGEDEPTH_LIVE=1 go test ./internal/hub -run TestLive -v
```

## Adding an exchange

The most useful contribution this gateway can receive is another venue.
Everything venue-specific sits behind one interface, so a new exchange is a
contained package, not a refactor.

### The seam

`internal/exchange` defines the contract:

- `Exchange` is the adapter: an `ID()`, a tradable-symbol whitelist, a
  per-symbol `Feed` factory, an optional all-market ticker, and optional
  historical candles.
- `Feed` is one instrument's upstream connection: `Run(ctx)` owns the
  transport and reconnects, `Snapshot()` returns the current orderbook so a
  late-joining client gets a base before diffs, and `MarkState()` feeds the
  per-bucket stats (return zeros if the venue has no funding/mark concept).
- Frames leave the adapter through `Emit` as EdgeDepth protobuf messages
  (`pb.Trade`, `pb.BookUpdate`, `pb.Liquidation`, ...). The hub owns candle
  aggregation, fan-out and the wire envelope; you never touch those.

`internal/binance` is the reference implementation: transport and reconnect
in `stream.go`, JSON decoding and the orderbook sync procedure in `feed.go`,
REST in `rest.go`, and the thin `exchange.Exchange` adapter in `adapter.go`.

### Steps

1. Create `internal/<venue>` with an adapter implementing
   `exchange.Exchange`.
2. Maintain the orderbook inside your `Feed` and emit a
   `pb.BookUpdate{Snapshot: true}` after every (re)sync, then diffs with
   `Snapshot: false`. Set sizes to 0 to delete levels.
3. Emit trades with `IsBuy` = the AGGRESSOR side. For liquidations, copy the
   venue's order side verbatim into `IsBuy` (on Binance, a BUY force order
   closes a short; inverting this flips the client's liquidation heatmap).
4. Return `exchange.ErrUnsupported` from `HistoricalCandles` for timeframes
   the venue cannot backfill; live aggregation fills them from the first
   trade. Return nil from `GlobalTicker` if there is no all-market ticker.
5. Register the venue in `cmd/edgedepth-gateway/main.go`:
   `hub.New(log, binance.New(log), yourvenue.New(log))`.

### The venue id trap

The terminal keys every subscription on `(exchange, symbol)`. Your `ID()`
must match the venue string the client uses EXACTLY, or subscriptions
silently never match and every panel stays empty with no error anywhere.
`"binancef"` is Binance futures and `"hl"` is Hyperliquid on the client
side today; check the terminal source before inventing a new id, and open an
issue if the client does not know your venue yet, because the client needs
the id in its symbol routing for URLs like `/terminal/<symbol>?exchange=<id>`
to work.

### Two decoding traps that cost real debugging time

- Go's JSON decoder falls back to case-insensitive field matching. Binance
  payloads carry both `e`/`E` and, on trades, `T`/`t`; a struct that declares
  only one of a pair gets the OTHER key assigned to it and either fails to
  parse or lands trade ids in timestamps. If your venue's JSON has
  case-colliding keys, declare BOTH fields even if one looks unused. See the
  comments in `internal/binance/feed.go`.
- Never swallow an unparsable payload silently. Log it. A silent return is
  how a venue-side wire change turns into an empty panel with no explanation.

## Continuity and volume history

A new feed may implement `exchange.TradeContinuity`. Install its reset callback
before `Run`, invoke it on connection replacement and any detected trade gap or
regression, and only then deliver new trades. Without that interface the hub
serves empty per-price history. Preserve source aggressor side, normalize actual
base units, and deduplicate source IDs. Do not infer aggressor side from price
movement or confuse aggregate-message counts with taker-order counts.

Use separate Binance market and public stream connections. Sequence-check both
the live depth chain and buffered deltas replayed after a REST snapshot; a stale
connection epoch must not publish its snapshot over a newer connection.
