# Storage audit and Storage V2 proposal

Historical audit baseline. Storage V2 was subsequently implemented; see
[the implementation and validation report](storage-v2.md) for current behavior.
The measurements and design discussion below describe the original audit.

Status: audit and non-destructive profiling only. No ingestion, schema, retention,
frequency, or historical-data changes have been made. No collector was started.
Measurements use the existing local data; a new continuous one-hour trial has not
been performed. The proposed schema below is a design for review, not a migration.

Verification: all 38 tests passed, including five new profiler tests. A second
measurement against the baseline showed zero new rows/bytes, and SHA-256 contents
plus file inventory were identical across all 61 files in the data root before
and after that profiler run. The disposable encoding benchmark verified an exact
roundtrip of all 1,586,259 book rows. This validates this sample and the profiler's
tested behavior, not an unimplemented production archive or migration.

## 1. Current architecture and complete write path

`Hypixel GET /v2/skyblock/bazaar → response.json() → validate + normalize → source-timestamp duplicate check → compact JSON + gzip → storage capacity check → BEGIN IMMEDIATE → SQLite inserts → atomic raw publish → SQLite commit → post-commit capacity check`.

Validation and normalization happen together **before** raw publication, not in a
later raw-file processing job. The raw file is a reserialized full JSON object,
not the byte-for-byte HTTP body. There is no asynchronous queue or archive worker.

| File/component | Responsibility |
|---|---|
| `src/ingestion/run_collector.py` | CLI configuration, advisory per-root `flock`, signals, scheduling, retries, startup/per-attempt raw cleanup, capacity checks, database/session lifecycle |
| `src/ingestion/bazaar_collector.py` | HTTP fetching, complete payload validation, tuple normalization, schema creation, deduplication, transactional persistence, gzip publication, retention and disk accounting |
| `tests/first_pull.py` | Manual one-shot entry point into the same runner; not a separate writer |
| `Dockerfile`, `requirements.txt` | Container entry point and Requests dependency; same persistence implementation |
| `tests/test_collector.py` | Fixture-based durability, rejection, deduplication, retention, budget, retry and scheduler tests |
| `src/dashboard/data.py` | Read-only SQLite queries and bounded filesystem size scan |
| `src/dashboard/analytics.py`, `charts.py` | Derived metrics and figures in memory; no historical feature writes |
| `src/dashboard/pages.py`, `app.py` | Page composition, cached queries and manual refresh; no ingestion |
| `tests/test_dashboard.py`, `tests/test_analytics.py` | Read-layer, page and calculation checks |
| `.gitignore`, `README.md` | Data exclusion and operational instructions; no automated backup/archive |
| `tools/profile_storage.py` (new) | Read-only inventory, SQL counts, optional dbstat, before/after comparison |
| `tools/benchmark_storage.py` (new) | Disposable offline encoding/size experiment; not a production archive writer |

Fetch uses Requests with a timeout; 429/5xx and network failures retry, permanent
HTTP failures stop, invalid payloads reject the whole snapshot. Validation requires
success=true, a positive integer source timestamp, consistent product identities,
all eight quick fields, and valid arrays/entries on both sides. Counts fit signed
64-bit integers; prices are finite nonnegative numbers. Empty books/products are
accepted. Unknown JSON fields survive only in raw JSON. Invalid responses are not
saved for later debugging by this pipeline.

The default delay is 60 seconds **after** each attempt completes; this is slightly
less frequent than exactly one snapshot/minute. Backoff, duplicate source data,
downtime, validation failures and processing time reduce actual writes. Source
time and post-fetch UTC collection time are distinct. A source timestamp is not
necessarily the time the client could have traded on that information.

### Schemas, indexes and write cardinality

The live database schema matches `open_database()`:

```sql
CREATE TABLE snapshots (
  source_updated_ms INTEGER PRIMARY KEY,
  collected_at_utc TEXT NOT NULL,
  product_count INTEGER NOT NULL
);
CREATE TABLE quick_status (
  source_updated_ms INTEGER NOT NULL REFERENCES snapshots(source_updated_ms),
  product_id TEXT NOT NULL,
  sell_price REAL NOT NULL, sell_volume INTEGER NOT NULL,
  sell_orders INTEGER NOT NULL, buy_price REAL NOT NULL,
  buy_volume INTEGER NOT NULL, buy_orders INTEGER NOT NULL,
  sell_moving_week INTEGER NOT NULL, buy_moving_week INTEGER NOT NULL,
  PRIMARY KEY (source_updated_ms, product_id)
);
CREATE TABLE order_book_levels (
  source_updated_ms INTEGER NOT NULL, product_id TEXT NOT NULL,
  api_side TEXT NOT NULL CHECK(api_side IN ('buy_summary','sell_summary')),
  level_index INTEGER NOT NULL, price_per_unit REAL NOT NULL,
  amount INTEGER NOT NULL, orders INTEGER NOT NULL,
  PRIMARY KEY (source_updated_ms, product_id, api_side, level_index),
  FOREIGN KEY (source_updated_ms, product_id)
    REFERENCES quick_status(source_updated_ms, product_id)
);
```

`snapshots.source_updated_ms` aliases the SQLite rowid. The other two are ordinary
rowid tables with separate unique B-trees:
`sqlite_autoindex_quick_status_1` and
`sqlite_autoindex_order_book_levels_1`. There are no explicit secondary indexes,
product-first indexes, partitions, triggers or compression in this database.

For each valid **new source timestamp**, persistence writes one metadata row, one
quick row per product, one level row per entry in both summary arrays, and one gzip
file. It does not skip unchanged products or identical books across timestamps.
An HTTP response with an already stored timestamp writes none of those. Validation
still runs before the duplicate check. The timestamp key also means a corrected
API response reusing a timestamp is ignored. Primary keys and one transaction
protect against duplicate/partial SQL histories; a cooperating collector lock
protects the data root. Foreign keys are enabled on collector connections.

At the measured 2,197 products and average 61,009.96 levels, each unique snapshot
adds approximately **63,208 SQL rows** (1 + 2,197 + 61,010). Level counts range from
60,712 to 61,241. The maximum observed side has 30 entries. With two sides, the
upper bound at 30 entries each and this product count would be 131,820 level rows;
actual books are frequently shorter/empty. There are no repeated same-price groups
in this sample. Do not assume that will remain true; preserve API array ordering.

### Raw format, durability, retention and read layer

`json.dumps(..., separators=(',', ':'), allow_nan=False)` followed by
`gzip.compress(..., mtime=0)` uses the Python runtime's default gzip level (no
explicit level is configured). Raw files use `bazaar-v1-<source_ms>.json.gz`.
A `.tmp` file is written with no symlink following, fsynced, renamed atomically,
and the containing directory fsynced. This occurs before SQLite commit. A failure
can leave an orphan raw file; no successful snapshot marker is committed on a
failed SQL transaction. There is no automatic replay of those orphans.

SQLite uses DELETE rollback journaling, synchronous=FULL, and one transaction per
snapshot. No WAL is configured. The measured database has auto_vacuum=0 and zero
freelist pages. No VACUUM or structured pruning is implemented.

Raw cleanup runs at collector startup and before attempts, using **filesystem
mtime**, with a default 48-hour window. It deletes only direct regular non-symlink
children matching the owned gzip or gzip.tmp namespace. It leaves nested legacy
raw files, the legacy DB, unrelated files and all structured history untouched.
When the collector is stopped, no background retention runs. A bounded raw buffer
is therefore a production-rate estimate, not a hard quota on the directory.

`snapshots`, `quick_status`, and `order_book_levels` grow indefinitely while unique
snapshots arrive. Legacy files are also retained indefinitely but are not actively
written by the current collector. The default 1 GiB budget and 5 GiB free-space
reserve stop ingestion rather than delete structured data. The write reserve is
16 MiB plus twice compressed JSON size plus the larger of four times compact JSON
size or 1 KiB per quick/level row. At current cardinality that last term alone is
about 64.7 MB. Logical size accounting covers all files in the data root, including
legacy data and transient journals, not filesystem snapshots/backups elsewhere.

The dashboard opens mode=ro, restricts SQL through an authorizer, waits at most
150 ms for locks, and applies a roughly 750 ms query budget. History/timeline are
limited to 3,000 observations, item discovery/market views to 5,000 products, and
current book display to 1,000 rows. The 3,000 limit is about 50 hours at one minute;
this is a display cap, **not storage retention**. Product history queries have only
timestamp-leading indexes and will not scale into a months-long research engine
unchanged. The raw size display ignores legacy `.db` files; the new profiler counts
all regular files and separates owned snapshots from legacy overhead.

The separate legacy database `data/raw/hypixel_bazaar.db` has one unindexed table:

```sql
CREATE TABLE product_quick_status (
  timestamp DATETIME, product_id TEXT,
  sell_price REAL, sell_volume INTEGER, sell_orders INTEGER,
  buy_price REAL, buy_volume INTEGER, buy_orders INTEGER,
  sell_moving_week INTEGER, buy_moving_week INTEGER
);
```

All legacy columns are nullable and there is no primary key. It has 68,107 rows
and occupies 6,201,344 bytes. Legacy timestamps are collection times; do not invent
source timestamps to merge this into the newer history.

## 2. Measurements and bottlenecks

Evidence: [baseline JSON](storage/baseline.json),
[encoding benchmark JSON](storage/benchmark.json). Measurement timestamp is in the
JSON (UTC). These are 26 stored snapshots in two short sessions on September 28,
2026, spanning 05:50–15:39 UTC with a long intervening gap. The machine's profiler
run is dated September 29 UTC. No one-hour sustained growth result is claimed.

All MB/GB below are decimal (10^6/10^9 bytes). GiB = 2^30; 25 weeks = 175 days;
month = 30 days. This avoids silently mixing the README's GiB/day with GB/day.

| Inventory | Measured value |
|---|---:|
| Database logical bytes | 189,149,184 (189.15 MB; 180.39 MiB) |
| Pages × page size | 46,179 × 4,096 |
| Freelist pages / active sidecar bytes | 0 / 0 |
| Committed snapshots | 26 |
| Quick-status rows / distinct products | 57,122 / 2,197 |
| Book-level rows | 1,586,259 |
| Raw directory, all regular files | 251,474,850 bytes; 59 files |
| Current collector gzip files | 11,879,188 bytes; 26 files |
| Legacy uncompressed/other raw files | 233,394,318 bytes; 32 files |
| Legacy SQLite inside raw directory | 6,201,344 bytes; 1 file |

The bulk of **today's raw directory size is legacy material**, not current gzip
production. Total main-DB-plus-raw logical stock is 440,624,034 bytes. Existing
legacy stock is not a recurring daily growth rate.

| SQLite object | Allocated B-tree bytes | Share of database |
|---|---:|---:|
| order_book_levels table | 92,602,368 | 48.96% |
| order_book_levels primary-key index | 90,689,536 | 47.95% |
| quick_status table | 3,661,824 | 1.94% |
| quick_status primary-key index | 2,187,264 | 1.16% |
| snapshots + sqlite_schema | 8,192 | <0.01% |

Books and their index occupy **96.90%** of the database. The book index is nearly
as large as the table. Average table-plus-index cost is about 115.5 bytes per level,
despite each level containing only three substantive numeric observations
(price, amount, orders). Book index pages contain about 15.6 MB unused space;
book table pages about 0.68 MB. Not all overhead is eliminable or redundant.

[SQLite dbstat](https://www.sqlite.org/dbstat.html) attributes B-tree page allocation,
payload and unused bytes to tables/indexes. It does not represent total filesystem
allocation or WAL/journal costs. The local Python runtime lacks the extension;
`/usr/bin/sqlite3 -readonly` has it. The profiler uses this only when requested.

### Expensive, cheap and redundant information

Expensive: per-level row overhead, repeated long product IDs, repeated side strings,
repeated source timestamps and a second copy of the composite key in the unique
index; all-products history multiplies this by roughly 61,000 rows per minute.

Cheap: one timestamp/collection-time/version record per snapshot; a product
registry; observed quick numeric primitives; compressed small feature vectors.

Redundant representation: repeated product identities in raw map/product/quick
objects; raw numeric observations duplicated in SQL during the debug window;
composite key material in both rowid table and unique index; persisted spread,
mid, imbalance and VWAP when their exact constituent primitives already exist.
`snapshots.product_count` can be recomputed from quick rows, but is inexpensive and
useful for reconciliation. Source and collection timestamps are **not** duplicates.
Quick-status aggregates, full-side volumes and moving-week values cannot generally
be reconstructed from the limited book summaries; preserve them independently.

[SQLite WITHOUT ROWID](https://www.sqlite.org/withoutrowid.html) can avoid the
rowid-plus-separate-primary-key layout for suitable composite-key tables. Numeric
product IDs/side enums remove repeated text. These are lossless representation
changes, but even optimized per-level SQLite need not be the permanent archive.

## 3. Growth calculations

Planning assumption: **1,440 unique snapshots/day**, with sample product/book
cardinality. This is not guaranteed API throughput and slightly exceeds the
runner's normal cadence after processing time. Do not divide by the long gap
between the stored sessions. These stock-per-snapshot estimates include schema
and allocation overhead; use a real before/after trial to refine marginal growth.

| Current component | MB/snapshot | GB/day | GB/week | GB/30d | GB/25wk |
|---|---:|---:|---:|---:|---:|
| SQLite, permanent/unpruned | 7.275 | 10.476 | 73.332 | 314.279 | 1,833.292 |
| Of that: quick table + index | 0.225 | 0.324 | 2.268 | 9.718 | 56.691 |
| Owned raw gzip production | 0.457 | 0.658 | 4.606 | 19.738 | 115.137 |

The last row is **bytes produced, not retained disk after retention**. At the
sample rate, owned raw converges toward 0.658 GB at 24 hours or 1.316 GB at 48 hours.
Add retained legacy stock (~0.240 GB), staging, journals, allocator overhead and
operational margin. Do not multiply the two-day buffer by 25 weeks. The projected
initial combined production is 11.134 GB/day (~10.37 GiB/day), consistent with the
older short trial; after raw retention stabilizes, SQLite growth still continues.

Expected row production per day: 1,440 metadata rows, 3,163,680 quick rows and
about 87,854,345 level rows. At 25 weeks this is about 553.6 million quick rows and
15.37 billion book rows. Counts as well as bytes make a single unbounded SQLite
file an unattractive research store.

### Repeatable before/after workflow

The profiler uses only the standard library. It never calls collector storage
initialization, cleanup, VACUUM, journal-mode changes, or schema creation. Database
access is mode=ro with query_only and a bounded read transaction. It saves reports
only to a new file outside the database/raw directories, or prints JSON to stdout.
A missing database fails rather than creates one. Symlink files are skipped and
reported. Filesystem/SQL failures produce a failed measurement, not a misleading
partial total. Exact counts can be expensive on a large database; the default SQL
budget is 30 seconds. Reads can briefly delay a DELETE-journal writer, so prefer
measurements between sessions and increase the timeout only deliberately.

```sh
mkdir -p /tmp/bazaar-profile
.venv/bin/python tools/profile_storage.py \
  --sqlite-cli /usr/bin/sqlite3 \
  --output /tmp/bazaar-profile/before.json

# Run the existing collector for ~1 hour with an explicitly chosen adequate budget.
# Its default 1 GiB budget may stop this trial early; no settings were changed here.
# Existing command: .venv/bin/python src/ingestion/run_collector.py --duration 3600

.venv/bin/python tools/profile_storage.py \
  --sqlite-cli /usr/bin/sqlite3 \
  --previous /tmp/bazaar-profile/before.json \
  --output /tmp/bazaar-profile/after.json
```

Use new report filenames for each trial; overwrites are refused. Omit `--sqlite-cli`
on systems without a capable CLI; counts still work, object attribution is null.
`--database` and `--raw-dir` select another collection root. Retain the actual
collector command/logs alongside measurements to establish uptime and success rate.
Allow roughly 0.67 GB new production for one hour at this sample rate, plus existing
stock, write reserve and free-space reserve; the default budget is not sufficient
for that complete trial on the currently measured root.

`comparison` includes new row counts, DB net bytes, raw added/removed/changed files,
new surviving gzip mean, MB-convertible bytes/snapshot, object deltas when available,
and day/week/30-day/175-day projections at the assumed unique-snapshot interval.
It refuses comparisons across changed file identity/schema/root or decreased row
counts. No new snapshots yields null per-snapshot DB projections. Raw deletions
are explicitly separated; files both created and deleted between measurements
cannot be recovered from a directory inventory. Changed files are disclosed, not
counted as new production. The source DB is not hashed by the profiler because
reading huge files solely to hash them would defeat a lightweight measurement.

Formulas: `DB bytes/snapshot = ΔDB bytes / Δcommitted snapshots`;
`rows/snapshot = Δrows / Δsnapshots`; `GB/day = bytes/snapshot × 1440 / 1e9`;
week/month/25wk multiply by 7/30/175. For measured wall-time throughput substitute
`Δsnapshots × 86400 / elapsed_seconds`; this includes any downtime.
Raw production uses average newly observed gzip size, never negative raw net growth.
Freelist reuse can mask DB live growth; inspect dbstat alongside file deltas.
Filesystem and SQLite measurements are not atomic with respect to a running writer.

## 4. Storage V2 representation and benchmark

Prefer **lossless compression before irreversible feature reduction**. Separate a
small serving store from immutable research history. Parquet organizes data in
column chunks and row groups, which makes repeated-field encoding practical;
see the [official file-format description](https://parquet.apache.org/docs/file-format/).
Use date partitions with reasonably sized batches, not one file per product per
minute. Preserve int64 counts/timestamps and float64 prices; do not silently round
prices or replace observations with five/fifteen-minute bars.

`tools/benchmark_storage.py` reads existing SQL, writes temporary artifacts, checks
every book row against the source after Parquet decoding, then removes temporary
artifacts. It uses PyArrow 25.0.1 already present in `.venv-dashboard`, Zstandard,
a numeric product dictionary and side codes. No dependency/collector configuration
was changed. Reproduce with:

```sh
.venv-dashboard/bin/python tools/benchmark_storage.py
```

This is an offline, potentially memory-intensive experiment; use on the small
sample, not a months-long active DB. It is not a crash-safe production exporter.
Quick/features use a combined 26-snapshot batch; books use one row group per
snapshot. There are only ~26 minutes of observations, not 26 consecutive minutes
or a representative day. Compression, price entropy, wider schemas and partition
size can change these numbers materially.

| Temporary representation | Measured bytes | Interpretation |
|---|---:|---|
| Numeric-key WITHOUT ROWID quick SQLite | 2,871,296 | ~51% smaller than current quick table+index; excludes registry/metadata/additional indexes |
| Quick-only Parquet | 672,018 | All eight original quick fields, timestamp, numeric product key |
| Quick + book-feature Parquet | 1,968,254 | Quick fields plus 16 book primitives |
| Lossless book-level Parquet | 12,071,457 | All 1,586,259 original level rows round-trip checked |
| Product registry Parquet | 23,354 | Small dictionary, not a per-snapshot recurring cost |

The feature sizing prototype includes, **on each API side**, best summary price,
level count, summary total quantity, cumulative quantity over first 1/5/10 levels,
and cumulative price×quantity over first 5/10 levels. These 16 values allow derived
spread/mid, depth imbalance and top-k VWAP without storing every derived value.
This is a sizing candidate, not finalized production feature code. Null/empty flags,
feature version, provenance/quality flags and manifests must be added and benchmarked.

| Permanent representation (sample extrapolation) | GB/day | GB/week | GB/30d | GB/25wk |
|---|---:|---:|---:|---:|
| Current SQLite | 10.476 | 73.332 | 314.279 | 1,833.292 |
| Compact quick-only SQLite | 0.159 | 1.113 | 4.771 | 27.830 |
| Quick-only Parquet | 0.037 | 0.261 | 1.117 | 6.513 |
| Quick + book features, all products | 0.109 | 0.763 | 3.270 | 19.077 |
| Quick + lossless books, all products | 0.706 | 4.941 | 21.174 | 123.514 |
| Features + lossless books, all products | 0.778 | 5.443 | 23.327 | 136.077 |
| Features + 10% of full-book encoded bytes | 0.176 | 1.231 | 5.276 | 30.777 |
| Features + 25% of full-book encoded bytes | 0.276 | 1.933 | 8.285 | 48.327 |
| Features + 40% of full-book encoded bytes | 0.376 | 2.635 | 11.293 | 65.877 |

Subset rows are **sensitivity scenarios, not an adopted universe or measured
subset benchmark**. Liquid products may have longer, more active books, so 25% of
products can consume much more than 25% of bytes. Model by encoded bytes, then
benchmark the actual objective universe. The registry is small and excluded from
these recurring figures. Quality columns, manifests, query catalogs, hot storage,
write staging and backups are also excluded; budget them explicitly.

Thus 1–3 GB/week is plausible for compact all-product features plus a constrained
lossless universe. At the measured ratios, the 3 GB limit leaves at most about
47.8% of all-book bytes before overhead; reserving 20% of the limit for uncertainty
reduces that to about 35%. A 1 GB ceiling has little room beyond the feature base.
All-products lossless books plus quick history currently project **above 3 GB/week**;
we should not promise otherwise. A feature+25%-bytes scenario saves about 97.4%
versus current permanent growth; retaining all compressed books with quick fields
already saves about 93.3% without losing book observations. These are sample-based
savings, not capacity guarantees.

## 5. Research information to retain

The [Hypixel API documentation](https://api.hypixel.net/#tag/SkyBlock/paths/~1v2~1skyblock~1bazaar/get)
describes summaries limited to the top 30 orders per side, standing quantities/order
counts, top-2%-by-volume aggregate prices, and moving-week volume incorporating
recent transactions plus live state. Therefore the existing system **does not
capture the full exchange order book or a trade/order-event feed**. Preserve the
phrase “complete API summaries,” not “complete market book.”

Keep API-side names as canonical. The documentation's example has ascending
`buy_summary` prices above descending `sell_summary` prices, consistent with
asks on buy_summary and bids on sell_summary from the immediate taker's perspective.
Treat this as a mapping to verify against the game/API before naming production
best_bid/best_ask or signing buy-pressure signals. The proposed feature extractor
sorts buy_summary ascending and sell_summary descending, retaining original level
indexes in the lossless archive. Crossed/zero/empty cases should be flagged rather
than fixed or silently discarded.

Recommended permanent primitives at one-minute observations for **all products**:

- All eight current quick fields, source timestamp and collection/availability time.
  Moving-week fields are rolling state, not a timestamped trade tape; their
  differences are not pure interval trade volume.
- Best **observed summary** price on each side, count of returned levels and empty/
  valid/capped-book flags. Distinguish observed emptiness (zero depth) from missing
  or invalid data (null). Fewer than k levels means depth over the available levels,
  accompanied by count/coverage flags; it does not mean k levels existed.
- Cumulative amounts at 1/5/10 entries and total **returned-summary** amount.
  Keep quick buy/sellVolume separately as the aggregate whole-side quantity.
- Cumulative price×amount at 5/10 entries, with the corresponding amounts. Compute
  VWAP as notional/depth when depth>0. This is a depth-weighted quote price, not
  a traded-volume VWAP. Preserve enough numeric precision and define tie ordering.
- Parser/feature/schema versions, units, side semantics, validation/coverage flags.

Derive mid, absolute spread and relative spread from best observed prices at query
or feature-build time: `(ask+bid)/2`, `ask-bid`, `(ask-bid)/mid` when valid. Keep these
names distinct from the existing dashboard's aggregate `buyPrice-sellPrice` and
spread divided by `buyPrice`; their denominators/interpretations differ. Derive
imbalance `(D_buy-D_sell)/(D_buy+D_sell)` only for a positive denominator, otherwise
null with an empty flag. Label sign as API-side imbalance until mapping is verified.

Top-one/summary-depth shares provide inexpensive concentration measures. Richer
concentration (e.g. Herfindahl share), price gaps, depth within fixed basis-point
bands, last observed price, and quote cost at specified sizes are useful candidates
for the research universe, but not all need be permanent columns from day one.
Rank depth can span very different price distances across products. Add price-band
or size-grid features only after choosing meaningful units and evaluating their
incremental value. Lossless curves allow these definitions to change later.

Do not permanently store every forward return/volatility horizon as ingestion
columns. Build versioned research datasets later from retained prices and coverage:
1/5/15/30/60-minute forward returns, rolling volatility, lagged depth changes and
signals. Match future observations within explicit tolerances; never manufacture
returns across missing periods. Backtests must use collection/availability time,
lagged universe membership and cost assumptions, not future source observations.
Walk-forward evaluation needs chronological splits, embargo/purging for overlapping
labels, versioned transforms and out-of-sample universe decisions.

### Capability matrix and the deletion tradeoff

| Question | Compact representation | Limitation / necessary extra data |
|---|---|---|
| Does imbalance predict later price movement? | Yes, at retained depth definitions and sampling rate | Cannot invent unrecorded imbalance thresholds after deletion; preserve quality and side definition |
| Do changes in depth predict forward returns? | Yes, from successive valid primitives | Gaps and order cancellations versus executions cannot be disambiguated |
| Does behavior vary with liquidity? | Yes, quick quantities/counts, depths and point-in-time membership | Quantities across heterogeneous products need price/notional normalization |
| Does spread predict short-term volatility? | Yes, observed spread plus future sampled price path | One-minute data misses intraminute extrema/events; aggregate-price and best-price volatility differ |
| How does execution size affect slippage? | Only coarse estimates for retained depth buckets/VWAPs | Arbitrary-size curve reconstruction requires individual levels or a richer exact curve |

Top-5/top-10 VWAP and totals are not sufficient statistics for arbitrary execution
size: many different distributions have identical totals/VWAP but different first
unit and intermediate costs. **Deleting all historical levels would irreversibly
prevent accurate reconstruction of the observed size-dependent execution curve.**

For retained products preserve lossless `(price, amount, orders, original index)`
arrays per side, or an exactly invertible price/cumulative-quantity/notional curve.
Compression is preferable to lossy fixed-size knots. Lossy size/notional-grid curves
can be evaluated as a lower-budget alternative only with explicit maximum cost
error and coverage limits measured against actual books. They cannot later support
arbitrary thresholds/features exactly. Preserve orders too for concentration work.

Even lossless API summaries allow only **snapshot-based hypothetical book walking**
within observed depth. Sizes beyond returned depth must be marked unsupported or
partially filled, not extrapolated as certain fills. There is no queue priority,
order ID history, cancellations versus executions, intraminute path, market impact,
latency evolution or guaranteed fill data. Passive-fill backtests and realistic
live execution require additional evidence/model assumptions. Fees and game rules
must be separately versioned. This limitation exists today; compression does not
cause it, but deleting levels makes it worse.

## 6. Research universe and proposed schema

Do not select a list now. Retain basic features for every observed product,
including illiquid/delisted products, to avoid survivorship bias. A future selection
policy should use only a trailing, historically available window and record:

- Coverage ratio, stale/gap frequency and fraction of valid two-sided books.
- Median/lower-quantile near-top depth in units **and estimated coin notional**,
  standing quantities, order counts, spread distribution and dispersion.
- Frequency/magnitude of quote/depth/order-count changes as activity proxies.
  Sampled changes cannot establish trade frequency, turnover or signed order flow.
- Moving-week values as a separately qualified activity proxy, not minute volume.
- Actual encoded bytes per product and query/research requirements.

Use a predeclared ranking/eligibility policy, minimum history, hysteresis and a
scheduled rebalance. Store candidates, exclusions and decision timestamps; once
selected, historical books remain even if membership later ends. Choose thresholds
from training windows and freeze them for walk-forward test periods. Hold a broad
provisional compressed archive during evaluation if budget permits, then review
coverage before narrowing. No arbitrary fixed percentage is adopted in this audit.

Proposed logical schema, applicable across hot SQLite and cold Parquet:

| Entity | Key / fields | Location and purpose |
|---|---|---|
| `products` | `product_key` integer PK, unique API `product_id`, first/last seen source | Small persistent catalog; never reuse keys |
| `snapshot_manifest` | source_ms PK, collected_at_ms, product_count, parser/schema versions, quality, payload hash, archive batch ID/state | Persistent provenance; retain source/availability distinction |
| `product_observations` | `(source_ms, product_key)`; original eight quick fields, feature version, two best prices, counts/flags, per-side depths 1/5/10/summary, notionals 5/10 | Permanent date-partitioned Parquet; recent serving subset in SQLite |
| `book_archive` | `(source_ms, product_key, api_side_code, level_index)`; float64 price, int64 amount/orders | Lossless columnar rows or nested arrays, with explicit side/ordering definition |
| `universe_membership` | `(policy_version, effective_from, product_key)`, effective_to, decision/availability time, trailing metrics/reason | Permanent point-in-time membership, no hindsight |
| `archive_batches` | batch ID, paths, time range, row counts, checksum, schema/feature version, status/commit time | Idempotent publication/reconciliation and partition discovery |

For hot composite-key tables benchmark numeric keys and WITHOUT ROWID. Add a
product/time lookup index only after explaining query needs and measuring its cost.
Cold files should carry collection time via the manifest, not necessarily repeat
it for every level. Use a catalog plus immutable checksummed partitions and atomic
publish state; a Parquet directory alone does not enforce uniqueness/foreign keys.
Keep raw payload and feature hashes/provenance separate: a checksum cannot recover
an expired field that was never normalized.

## 7. Proposed retention and bounded storage

These are recommendations, **not applied changes**:

| Data | Proposed policy |
|---|---|
| Full reserialized raw payload | 48-hour debug/recovery window initially (24-hour option only after reliability evidence) |
| Original quick observations + chosen basic primitives, all products | Permanent at observed ~one-minute resolution |
| Complete API summary books, all products | 48-hour lossless compressed rolling buffer, then eligibility-based archival decision |
| Lossless books for approved research universe | Permanent, same observed cadence, versioned membership |
| Recent serving SQLite rows | Bounded window sized to dashboard needs, prune only after verified archival |
| Metadata, versions, quality, membership and archive manifests | Permanent |
| Current historical SQLite and legacy files | Preserve during migration/reconciliation; no automatic deletion now |

A 48-hour all-product compressed book buffer projects to ~1.337 GB; raw to
~1.316 GB. A recent quick+feature columnar-equivalent buffer adds ~0.218 GB, but
SQLite serving overhead must be benchmarked separately. Thus compressed buffers
start around **2.65 GB for raw+books**, plus serving DB, legacy stock, publication
staging, free-space margin and backups. Avoid accidentally keeping both unbounded
SQL and archived copies forever. The rolling book buffer overlaps the permanent
universe; do not double-count the same physical files if shared.

If instead retaining the **current SQL level representation** for 48 hours, its
book table+index alone needs about **20.3 GB**, plus raw and quick/feature storage.
“Short retention” does not by itself make that representation cheap. DELETE returns
SQLite pages for reuse; it does not necessarily shrink the file, and VACUUM may
require substantial temporary disk. Design bounded hot storage/archive partitions
before introducing deletion, not after the disk fills. Keep a backpressure rule:
if archival/reconciliation fails, stop ingestion or alert rather than prune data
that was promised permanent retention.

## 8. Changes needed, migration strategy and implementation order

Exact existing components affected by a future V2 implementation:

1. `bazaar_collector.py`: separate schema initialization from versioned migrations;
   registry resolution, shared validation/feature extraction, typed persistence,
   archive staging/commit records, size accounting and eventually guarded retention.
   `validate`, `open_database`, `persist`, `Storage.check/usage/cleanup` are affected.
2. `run_collector.py`: archive/export scheduling or independent worker supervision,
   explicit retention configuration and health/backpressure. Preserve poll cadence,
   signal handling, dedup semantics and source/collection timestamps.
3. New proposed `src/storage/schema.py`, `features.py`, `archive.py`, `retention.py`
   and migration/export CLI: isolate contracts, derivation, idempotent file
   publication, manifests and reconciliation. These files are not implemented yet.
4. `src/dashboard/data.py`: route recent versus archived history; change current
   book lookup after level-table retirement; retain read-only guarantees and
   bounded query behavior. `analytics.py`/`charts.py` must consume versioned metric
   definitions; `pages.py`/`app.py` must disclose archival coverage/availability.
5. `requirements.txt` or a separate archive/research requirements file and
   `Dockerfile`: explicit Arrow/codec/query dependencies in the correct environment.
6. `tests/test_collector.py`, dashboard/analytics tests plus new migration/archive
   tests: exact roundtrips, empty/short books, zero denominators, side mapping,
   feature versions, uniqueness, crash points, late/out-of-order sources, partial
   exports, disk-full, recovery, retention gating and observed-depth slippage cases.
7. `README.md`, profiler and research notebooks: archive operations, growth alerts,
   query examples, availability-aware labels and versioned walk-forward datasets.

Recommended order:

1. **Now:** keep this audit/profiler; obtain a properly budgeted one-hour before/after
   measurement, followed by several representative sessions. No forced collection
   or silent budget increase was performed in this task.
2. Confirm side semantics/units and data contracts; benchmark date-sized lossless
   archives and actual query workloads. Choose a storage budget inclusive of hot
   buffers, backups and simultaneous old/new copies. Validate compression beyond
   this short sample before continuous unattended collection.
3. Build a lossless, additive exporter on a backup/staging copy first. Export all
   existing source snapshots and exact book rows. Derive versioned features from
   those preserved rows, without changing originals. Existing rows retain actual
   timestamps; no synthetic timestamps, forward filling or legacy-source invention.
4. Reconcile source/target per-snapshot product/level counts, keys, numeric values,
   side/index ordering, empty-book representation and hashes. Test restore plus
   crash/retry publication. Treat existing legacy DB separately; preserve a copy.
5. Run shadow feature/archive publication while V1 remains authoritative. Use
   deterministic batch keys, atomic temp-to-final promotion and committed manifests;
   tolerate export replays without duplicated research rows. Include orphan/partial
   batch recovery and disk-headroom testing.
6. Update read paths to query recent SQLite plus verified archives, deduplicating
   their overlap by source/product/side/index keys. Verify dashboard parity and
   research query reproducibility before switching writes/read ownership.
7. Evaluate the objective universe on trailing history; measure its actual book
   byte share and research coverage. Review lossless-all-products (~4.94 GB/week)
   versus budgeted subset tradeoff before committing to any irreversible reduction.
8. **Only after a separate reviewed implementation decision:** enable raw/hot-book
   expiry and SQL pruning behind archive-completeness checks. Back up and verify
   recovery first. Delete children before parents if SQL FKs apply; prune in bounded
   batches or rotate partitions. Never purge unarchived required observations.
9. After an overlap period and successful restore drill, retire old storage with
   explicit rollback/backup timing. Reclaim disk through a planned copy/compaction
   operation with enough free space; do not run a surprise VACUUM on the collector.
10. Build hypothesis tests, cost-aware backtests and walk-forward research datasets
    over immutable retained history. Keep ingestion primitive and labels/versioned
    research transforms reproducible.

The decision to review is therefore **which lossless book history to retain**, not
whether a handful of indicators can replace all microstructure information. The
measured representation overhead offers major savings before sampling frequency
or research fidelity needs to change.
