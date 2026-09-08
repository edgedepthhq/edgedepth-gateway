# Gateway data-workflow release candidate

Unreleased local changes, 8 September 2026. No version tag or image publication
is implied.

- Route Binance market and depth streams through their current separate endpoints.
- Retain bounded closed-minute per-price observations and answer existing footprint/profile requests.
- Reset partial trade history across detected gaps; repair depth resync retry and stale-snapshot handling.
- Bound queued bytes and active symbols, and require continuity reporting for adapter history.

Before announcing: publish reviewed terminal and gateway revisions, pin both
image digests, and rerun the cold-start, file, live and pack-replay checks against
those published images. The repos version independently. Keep the previous
digests for rollback. Do not claim Windows, macOS, Safari, all venues, a fixed
FPS figure, historical completeness, or broker execution from this Linux test.
