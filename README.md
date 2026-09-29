# BazaarTracker

A lightweight Python collector for Hypixel SkyBlock Bazaar snapshots. Implemented:
validated collection, restart-safe deduplication, SQLite quick-status and order-book
history, compressed raw responses, retention, storage checks, and retry logging.
A separate read-only Streamlit dashboard explores the stored history. Discord alerts
and arbitrage detection are not implemented. There is no trade feed, trading
automation, or prediction model.

## Weekly update — September 28, 2026

This week's work adds a storage-aware collector and a read-only dashboard:

- **Reliable snapshot history:** API source timestamps are separate from UTC
  collection times. Transactional inserts and unique keys prevent duplicates across
  restarts. Quick-status fields and both API order-book summaries are preserved;
  malformed snapshots are rejected, and legacy data remains untouched.
- **Storage and recovery:** New raw responses use compact gzip files with atomic
  writes. Configurable budget/free-space checks reserve write headroom, while
  retention removes only expired collector-owned raw files. Structured history is
  never automatically deleted. One-shot and duration modes, bounded backoff, and
  logs support supervised trials.
- **Dashboard:** Four read-only terminal pages: Market Overview, Item Explorer,
  Scanner, and Collector. Interactive Plotly price, spread, standing-volume,
  cumulative depth and market-opportunity charts use existing stored fields.
  Independent price scales, liquidity filters and explicit history limits keep
  comparisons readable. No collection, schema or data-integrity behavior changed.
- **Verification:** The analytics/dashboard regression suite includes the existing
  collector tests, fixture-only read-only query checks and Streamlit page tests.
  See the dashboard section for the verification command and metric definitions.
  Docker image verification remains outside this dashboard change.

The ten-minute collection trial completed normally with 10 new snapshots, each
containing 2,197 products and roughly 60,700 order-book levels. Logged logical data
size grew from 247,272,641 to 324,150,858 bytes: about **73.3 MiB in ten minutes**,
or **10.3 GiB/day** if that rate continued before retention. This short sample is
an estimate, not a capacity guarantee. The default 1 GiB budget cannot sustain a
full day at that rate, and raw retention does not bound structured-history growth.
See [disk-growth measurement](#measure-real-growth) before planning longer runs.

Use the [collector setup](#setup-and-run) for collection and the
[separate dashboard environment](#read-only-streamlit-dashboard) for exploration.
Discord alerts, arbitrage detection, and automated trading remain unimplemented.

## Setup and run

Python 3.11+ on macOS/Linux (the process lock uses `fcntl`), or Docker. From the
repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python src/ingestion/run_collector.py --once
.venv/bin/python src/ingestion/run_collector.py --duration 3600
```

Without either mode the collector runs until SIGINT/SIGTERM or a terminal error.
`--once` makes exactly one request; a duplicate is a successful check. Exit code 0
means normal completion; 1 means a request/rejection in one-shot mode, permanent
HTTP failure, storage stop, lock conflict, or persistence error. Ctrl+C/SIGTERM
finishes the current operation and interrupts waits. A duration caps scheduling
and waits; an in-flight HTTP request or disk operation can overrun the deadline.
Requests uses connect/read timeouts, not a strict total response deadline.

All configuration is through CLI flags (`--help`):

| Flag | Default | Meaning |
| --- | --- | --- |
| `--data-dir` | repository `data/` | Budget/accounting root |
| `--raw-dir` | `<data-dir>/raw` | Must be a child of data-dir |
| `--poll-interval` | `60` | Seconds between completed attempts |
| `--budget-gib` | `1` | Project data budget, GiB = 2^30 bytes |
| `--min-free-gib` | `5` | Free disk space to preserve |
| `--raw-retention-hours` | `48` | Raw-file age limit |
| `--headroom-mib` | `16` | Base write reserve, MiB = 2^20 bytes |
| `--timeout` | `10` | HTTP connect/read timeout in seconds |
| `--once` | off | One request, no retries |
| `--duration` | unlimited | Run duration in seconds; exclusive with `--once` |

Numeric settings must be finite and positive. A process lock prevents cooperating
collectors from sharing a data directory. Run on a local filesystem with working
SQLite locks and atomic rename; do not share it across machines.

## API and data semantics

Verified against [official Hypixel API documentation](https://api.hypixel.net/#tag/SkyBlock/paths/~1v2~1skyblock~1bazaar/get)
on September 28, 2026. Bazaar is a public endpoint with no documented API-key
requirement. This collector sends no authentication header; `HYPIXEL_API_KEY` is
not needed. For endpoints that require keys, Hypixel documents `API-Key`, not
Bearer authentication.

`lastUpdated` is stored unchanged as integer `source_updated_ms` (Unix milliseconds).
`collected_at_utc` is a separate timezone-aware UTC ISO timestamp taken after the
response is fetched. `snapshots` has one row per source timestamp. `quick_status`
has a composite primary key `(source_updated_ms, product_id)` and preserves all
eight existing price/volume/order/moving-week fields. `order_book_levels` adds
`api_side`, zero-based `level_index`, `price_per_unit`, `amount`, and `orders`.
Reconstruct each book with a source/product/side filter and `ORDER BY level_index`.
All three tables are inserted in one transaction, with foreign keys enabled.

The two books keep the API names `buy_summary` and `sell_summary` and their array
order. No bid/ask or instant-trade interpretation is assigned. The docs describe
limited top-order summaries, not a full book. Quick-status prices are weighted
aggregates of the top 2% by volume, not necessarily executable prices. This is
periodic snapshot data, not a complete trade feed; changes between polls are lost.

Validation rejects the **entire snapshot** before raw/database writes if success,
source timestamp, product identifiers, required quick-status fields, or any book
entry is malformed. Counts/amounts must be nonnegative SQLite-range integers;
prices must be finite nonnegative numbers. Source timestamps must be positive
integers. Missing timestamps are never replaced with collection time. Empty books
are stored as zero level rows, without invented prices or levels; an empty products
object is also accepted. Duplicate source timestamps are ignored even after restart.
API corrections reusing a timestamp will therefore not replace existing data.

## Storage and recovery

New database: `data/bazaar.sqlite3`, separate from `data/raw/`. New raw files:
`bazaar-v1-<source_updated_ms>.json.gz`, containing compact JSON. The entire data
root is accounted by logical file size, including legacy data, unrelated files,
the database, SQLite journals/WAL/SHM if present, and temporary raw files. Logs
report usage, reserve, request failures, rejected snapshots, counts, duplicates,
and stop reasons. Console logs redirected outside data-dir are outside the budget.

Before initialization and each collection, expired raw files are removed and
capacity checked. Before snapshot writes, an additional estimate reserves twice
the compressed size and the larger of four times uncompressed JSON size or 1 KiB
per structured row, plus the configurable base headroom. Disk free space is checked
on both database and raw filesystems. Usage is checked again after commit. Storage
exhaustion or a write failure stops collection; structured history is never pruned.
The database will eventually fill the budget even with raw retention enabled.

These are conservative pre-write estimates, **not a perfectly enforced hard
quota**: SQLite page/index/journal costs, filesystem allocation, other processes,
and a single unexpectedly large response can differ from estimates. JSON is held
in memory; HTTP response size is not bounded. No automatic VACUUM is performed.

Retention removes only regular, non-symlink files directly in the configured raw
directory matching the reserved `bazaar-v1-<positive integer>.json.gz` namespace
(or its `.tmp` staging suffix), whose filesystem modification time is older than
the retention window. Do not place unrelated files in this reserved namespace.
Recent files, nested legacy directories, unrelated names, databases, and symlinks
are left alone. Retention runs only while the collector runs, including startup;
file age follows collection/write time, not the API timestamp.

Raw writes use a temporary file, fsync, atomic rename, and directory fsync before
the SQLite commit. SQLite uses FULL synchronous mode and rollback journaling.
A crash before rename can leave a `.tmp`; a crash/commit failure after rename can
leave a complete orphan raw file. Neither counts as ingested: only committed
`snapshots` rows do. Retrying that source replaces its staging/raw file and commits
all rows. If that source never returns, its orphan expires normally. There is no
automatic replay of orphan files. Raw retention can intentionally leave committed
history without corresponding raw files. External file deletion/corruption and
hardware durability failures are outside the recovery guarantee.

## Legacy data

The previous `data/raw/hypixel_bazaar.db` and `product_quick_status` rows are
preserved unchanged alongside the new database. Their `timestamp` is collection
time, with no trustworthy source timestamp, so they are **not** imported or assigned
fabricated source IDs. Query legacy history separately; new deduplication begins
with the new schema. Old dated raw JSON files are retained indefinitely and count
toward the budget. Archive them manually if desired; automatic cleanup never deletes
them. Back up the entire data directory before manually reorganizing legacy data.
Files outside the configured data root are outside accounting.

## Failures and retries

Network errors, timeouts, HTTP 429 and 5xx retry with exponential delays capped at
300 seconds, never shorter than the configured poll interval. Valid `Retry-After`
seconds or HTTP dates can extend that cap: the server delay is honored rather than
retrying early. Duration/shutdown can end the wait without another request. Other
HTTP errors stop. Invalid payloads log rejection and resume on the next poll
(one-shot exits unsuccessfully). Database and filesystem errors stop rather than
claiming a successful snapshot. A successful response resets the retry counter.

## Docker

```sh
docker build -t bazaartracker .
docker run --rm --mount type=bind,source="$PWD/data",target=/app/data bazaartracker --once
docker run --rm --mount type=bind,source="$PWD/data",target=/app/data bazaartracker --duration 3600
```

Create `data/` first if needed. Keep the bind mount (or a named volume at `/app/data`)
for persistence across containers; without it container removal loses history.
Default limits still apply, including 5 GiB free inside the mounted filesystem.

## Measure real growth

Before and after a trial, record both logical bytes (matching collector accounting)
and allocated disk usage:

```sh
.venv/bin/python -c 'from pathlib import Path; p=Path("data"); print("logical bytes:", sum(f.stat().st_size for f in p.rglob("*") if f.is_file())); print("database bytes:", (p/"bazaar.sqlite3").stat().st_size if (p/"bazaar.sqlite3").exists() else 0); print("raw bytes:", sum(f.stat().st_size for f in (p/"raw").rglob("*") if f.is_file()))'
du -sk data
sqlite3 data/bazaar.sqlite3 'SELECT count(*), min(collected_at_utc), max(collected_at_utc) FROM snapshots;'
```

For a one-hour trial before retention removes files, `(after_bytes - before_bytes)
* 24` estimates daily growth at that trial's unique-snapshot rate. Measure database
and raw deltas separately: DB daily growth continues; raw should level off around
two days of raw production under the 48-hour default. Exclude initial schema cost,
legacy files and orphan cleanup from representative estimates; duplicates reduce
growth. After retention starts, total net growth understates raw production.
Repeat over representative hours and compare free space and configured headroom.
Approximate days remaining as `(budget - current_usage - write_reserve) / daily_DB_growth`
once raw storage stabilizes. This is an estimate, not a guarantee.


## Read-only Streamlit dashboard

Install into a **separate environment**; these commands do not change the collector's
`.venv`, dependencies, process, configuration, or data:

```sh
python3 -m venv .venv-dashboard
.venv-dashboard/bin/python -m pip install -r requirements-dashboard.txt
.venv-dashboard/bin/python -m streamlit run src/dashboard/app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
```

Open the local URL printed by Streamlit (normally http://127.0.0.1:8501). Stop the
dashboard with Ctrl+C; this does not stop ingestion. The dashboard does not fetch
Hypixel data, start a collector, migrate schemas, create indexes, or write SQLite.
Keep it local; authentication and public deployment are outside this task.

Defaults are `data/bazaar.sqlite3` and its sibling `raw/`. To view another data root:

```sh
BAZAAR_DASHBOARD_DB=/absolute/path/bazaar.sqlite3 BAZAAR_DASHBOARD_RAW=/absolute/path/raw .venv-dashboard/bin/python -m streamlit run src/dashboard/app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
```

Use **Refresh** to reload. Normal widget interactions reuse bounded in-memory
caches; there is no timer or automatic history/directory polling. Each refresh has
an explicit UTC timestamp. Ages update on UI interaction, but records remain cached
until refresh (or cache eviction). Newly queried items/windows can see newer database
state; separate short reads are not an atomic view of the entire running database.

The health view shows the newest source snapshot and its collection time, most
recent collection time, unique snapshot count, source/collection ranges, product
count, and separate logical sizes for the database, SQLite sidecars and raw files.
Raw size includes `.json`, `.json.gz`, and `.json.gz.tmp` files recursively, including
legacy raw JSON; it excludes legacy databases and unrelated file types. It is not
the collector's total-budget measurement. Symlinks are not followed. Files changing
during a scan can make the size approximate; inaccessible or capped scans are
flagged as partial. No files are opened for writing or removed.

The dashboard has four workspaces:

- **Market Overview:** current market cards, absolute top movers, largest spreads,
  standing-volume activity bars, and an interactive Plotly opportunity map. Rankings
  share a minimum quantity filter on both sides, initially 1,000 units per side.
- **Item Explorer:** searchable products; price, spread, standing-unit and order
  metrics; 1h/6h/24h/7d/All history controls; independent price scales with shared
  time axes, shared-scale or indexed alternatives; spread and volume histories;
  cumulative order-book depth; expandable raw books.
- **Scanner:** sortable market table with product search, total and smaller-side
  quantity minimums, and optional spread, buy-price and price-change ranges.
- **Collector:** storage sizes, counts, source/collection timestamps and ranges,
  coverage gaps and health details. Stale warnings also appear on market pages.

### Metric definitions and limitations

[Hypixel's official Bazaar API documentation](https://api.hypixel.net/#tag/SkyBlock/paths/~1v2~1skyblock~1bazaar/get)
describes prices as volume-weighted aggregates over the top 2% and volumes as
standing quantities in orders. We preserve the API side names rather than label
books with stock-market bid/ask terminology.

- Spread = `buyPrice - sellPrice`, in coins per unit.
- Spread % = `100 * (buyPrice - sellPrice) / buyPrice`. This is a price-gap
  measure, not net profit or return on capital. Both prices must be positive;
  missing/zero prices yield an unavailable spread. Negative spreads are retained.
- Total standing volume = `buyVolume + sellVolume`; smaller-side liquidity =
  `min(buyVolume, sellVolume)`; orders = `buyOrders + sellOrders`. Unknown inputs
  remain unknown. Summing quantities across different products is an activity
  proxy, not market capitalization or coin value.
- Price change = `100 * (current buyPrice / baseline buyPrice - 1)`. Periods end
  at the displayed market snapshot, not the wall clock. Fixed periods need an
  observed baseline no more than 180 seconds before their target. Recorded range
  uses the first stored snapshot. Actual endpoint times are displayed; missing
  products have blank changes. Endpoint comparisons do not assert intervening
  continuous coverage.
- Depth accumulates stored amounts independently on `buy_summary` (ascending
  price) and `sell_summary` (descending price), nearest price outward. Dotted
  aggregate-price markers are context, not executable quotes. The API summaries
  are a limited slice of the book, not full market depth.
- Opportunity-map x is total standing units on a log scale; y is spread %;
  bubble area is smaller-side standing quantity. A minimum marker size keeps
  zero-sided books visible when the liquidity filter is disabled. Hover gives
  exact quantities and prices. Invalid spreads and nonpositive total volume are
  excluded and the plotted count is shown.

History windows end at manual refresh time. Only actual observations are drawn;
no resampling, invented history or forward filling occurs. Lines break across
source/collection gaps over 180 seconds and missing item observations. Indexed
price mode uses the first displayed observation as 100 separately for each series;
if that initial value is invalid, that normalized series is unavailable. Current
cards and books use the item's latest observation at or before refresh, independent
of the chart window; absent/latest and stale items are flagged. Empty books stay
empty. Snapshots are not trades, and fees, execution slippage and fills are not
modeled. `movingWeek` is not used as a historical trade feed.

Analytics live in `src/dashboard/analytics.py` (pure calculations), Plotly figure
builders in `charts.py`, read-only SQL in `data.py`, and Streamlit page composition
in `pages.py`. `app.py` handles navigation and shared refresh state. Overlay and
signal calculations can extend these modules without coupling to Streamlit.
Volatility, technical indicators, forecasts, trading signals, and ML are deferred;
collect a longer, well-covered history before adding sample-qualified rolling
metrics. The existing short trial cannot support meaningful 1h/7d comparisons.

SQLite is opened with URI `mode=ro`, a 150 ms lock timeout, a read-only SQL authorizer,
and a roughly 750 ms SQLite VM execution budget per short connection. No connection
is cached or left open during rendering. Missing/empty/incompatible databases and
temporary locks display messages with a Refresh option. Existing source-first
primary keys are used without adding indexes; all-history distinct item discovery
and snapshot counts can exceed the budget on large databases and then fail visibly.
Only the first 5,000 discovered IDs are offered (sorted for display); charts and
coverage are capped at the newest 3,000 observations per window, and books at 1,000
levels total. Market queries read one exact timestamp (up to 5,000 products), never
backfill absent products from older snapshots; subset aggregates are labeled. Truncation is disclosed; no downsampling is performed. File scans
stop after 50,000 entries or roughly 750 ms and report a partial lower bound.

Run offline verification in the dashboard environment:

```sh
.venv-dashboard/bin/python -m unittest discover -s tests -v
```

Dashboard tests create separate temporary fixture databases and cover read-only
write denial, unchanged fixture bytes, missing/empty data, locks, item selection,
time windows, stale data, gap segmentation, result limits, and Streamlit AppTest
interactions across all four pages. Pure analytics tests cover missing values,
spread denominators, endpoint changes, depth sorting and accumulation, filtering,
and gap-safe Plotly traces. AppTest is behavioral testing; visual layout and chart
rendering require separate browser verification.
