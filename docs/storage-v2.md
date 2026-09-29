# Storage V2: implementation and operations

Storage V2 retains observed one-minute market data and removes indefinite growth
of the per-level SQL representation for **new** snapshots. The V1 audit found that
book rows and their composite-key index occupied 96.9% of the database. The normal
poll interval is still 60 seconds after completed attempts. No resampling,
forward filling, or five/fifteen-minute downsampling is performed.

## Permanent representation

This implementation follows the audit's information-preservation, provenance and
retention contracts, with one explicit representation change: permanent blocks are
**checksummed JSON arrays compressed with gzip level 6 inside SQLite BLOBs**, not
Parquet files. The encoding uses only the existing Python standard library. One
transaction publishes snapshot metadata, hot rows, compact history, selected full
books, membership and the product registry together. A rollback cannot leave a
committed SQL snapshot whose permanent archive was only partly published.

This is a deliberate first implementation: it avoids coordinating a SQL commit with
external archive files and supports straightforward additive recovery. The tradeoff
is that SQLite still hosts permanent history, and per-snapshot compressed blocks do
not support column predicate pushdown. Offline readers stream/decode one snapshot
at a time. A future verified Parquet exporter can preserve these same logical
contracts; it is not implemented or required to operate V2. **Do not apply the
audit's Parquet compression ratios to this gzip implementation.**

| Storage class | Default lifetime | Details |
|---|---|---|
| `snapshots`, `products`, migration metadata | Permanent | Source timestamp and actual collection timestamp remain distinct |
| `compact_history` | Permanent, all products | Original eight quick fields plus book primitives at every accepted source timestamp |
| `historical_books` | Permanent, configured products | Exact original API-summary ordering, prices, amounts and order counts, including empty summaries |
| `v2_snapshots` | Permanent | Membership policy as observed at ingestion, ingestion time, protection/pruning markers |
| New `order_book_levels` rows | 24 hours | Temporary detailed SQL serving cache |
| New `quick_status` rows | 48 hours | Temporary product-history serving cache |
| Pre-migration V1 SQL rows | Protected indefinitely | Migration never deletes or rewrites them |
| Owned raw gzip files | 48 hours | Same filename/mtime-based retention as V1 |
| Migration backups and legacy raw/DB files | Manual retention | No automatic deletion; included in data-root budget |

“Complete historical books” means complete **returned API summaries**, not the
unobserved full exchange order book. Preserve this distinction in research results.

## Schema and encoding contract

Original V1 tables and keys remain intact. Five tables are added:

- `storage_migrations`: schema version 2, `backfilling`/`ready` state, backup path and
  SHA-256, start/completion UTC times. Collection refuses unknown/incomplete schemas.
- `products`: integer `product_key` primary key and unique API `product_id`.
  Keys are never reassigned by the application.
- `compact_history`: source timestamp primary/foreign key; codec identifier;
  feature version; product count; compressed-byte SHA-256; permanent payload.
- `historical_books`: source timestamp primary/foreign key; codec; archived-product
  count; level count; SHA-256; permanent payload. A zero-member pack explicitly
  records no retained books instead of conflating missing files with empty books.
- `v2_snapshots`: source timestamp primary/foreign key referring to both permanent
  packs; ingestion wall-clock milliseconds; `legacy_protected`; canonical universe
  JSON; original level count; SQL-book and quick-status pruning flags.

`v2_retention(legacy_protected, stored_at_ms)` bounds the candidate lookup. Snapshot
primary keys alias rowids, so there is no second composite-key index per permanent
level. There are no permanent per-level SQL rows for newly collected observations.
Foreign keys are enabled on writer connections. Source time still deduplicates
snapshots after temporary rows have expired.

Codec `json-gzip-v1` envelopes carry version, kind (`compact` or `books`), source
milliseconds and rows. The decoder checks checksum, envelope identity and size
(maximum decoded block 256 MiB). JSON numeric serialization preserves Python's
parsed integer/float values; no price quantization is applied. Arrays remove field
names repeated at every product/level. Product names are stored once in the registry.

Compact rows are:

```text
[product_key, [eight quick values], [buy-side primitives], [sell-side primitives]]
quick: sell_price, sell_volume, sell_orders, buy_price, buy_volume,
       buy_orders, sell_moving_week, buy_moving_week
side: best_price, level_count, summary_depth, depth_1, depth_5, depth_10,
      notional_5, notional_10, flags
```

Book rows are:

```text
[product_key, [[price, amount, orders], ... buy_summary in original order],
              [[price, amount, orders], ... sell_summary in original order]]
```

Array position reconstructs the exact original zero-based `level_index`. Unknown
extra API JSON fields are retained only in the temporary full raw payload; the
lossless book contract covers the audited price/amount/orders/index fields.

Feature version 1 preserves the API-side names and uses stable price sorting:
buy_summary ascending and sell_summary descending. It does not relabel the sides
as exchange bids/asks. Returned-summary depth is not whole-market depth. Quick
buy/sellVolume values independently preserve API aggregate standing quantities.

`notional_k` is cumulative price×amount over up to k returned entries. It is stored
as decimal text computed with 400-digit precision from the validated numbers'
decimal representations. This avoids float multiplication overflow and loss of
large integer quantity precision. `features.vwap` derives a float quote-depth VWAP
for positive depth; empty/zero depth returns None. All original prices, counts and
amounts remain in selected lossless books, regardless of feature derivation.

Flags are bitwise: 1=empty summary; 2=at least 30 returned entries (potential API
limit, not proof of full-market coverage); 4=zero price observed. `level_count`
indicates when fewer than k entries were returned. A valid short/empty array is
accepted; a missing side or malformed entry rejects the entire snapshot. A zero
best price is preserved/flagged rather than invented or dropped. The first sorted
entry is the best *observed summary price*, even if its amount is zero.

## Derived research quantities and limits

Reconstruct from retained primitives:

- Depth and depth changes at 1/5/10 entries and all returned entries, with gap checks.
- API-side imbalance `(buy_depth-sell_depth)/(buy_depth+sell_depth)` for each
  retained horizon; denominator zero or missing depth returns None.
- Quote-depth VWAP at 5/10 entries: `notional/depth`, not traded-volume VWAP.
- Mid and spread from validated best prices, once a side interpretation is chosen;
  keep these distinct from quick aggregate-price mid/spread. Relative-spread
  denominator must be part of the research feature definition.
- Top-one share of returned-summary depth, basic concentration, top-level count,
  plus liquidity from original quantities and order counts.
- Lagged returns, 1/5/15/30/60-minute forward returns, sampled volatility, regimes,
  and signal/backtest datasets from the original quick series and availability time.

Cannot reconstruct from compact features: arbitrary price thresholds, all
intermediate level gaps, arbitrary-size execution curves, order-by-order queue
state, intraminute activity or actual fills. The rolling moving-week values do not
constitute a minute-by-minute trade tape. No forward labels are computed during
ingestion, and no actual volume/trade flow is inferred from snapshot differences.

For size-dependent historical slippage, **require a permanent full-book result**.
`research.book_at` returns `available`, `permanent`, `reason`, and exact `levels`.
An available empty book has `available=True` and an empty level list. An excluded
product has `available=False`; it must not be simulated as an empty book or filled
using compact VWAPs. Sizes beyond observed summary depth remain unsupported/partial.
Even complete summaries cannot establish queue priority, latency, market impact,
intervening cancellations or guaranteed fills. No execution simulator is implied.

Read-only research example:

```python
from src.storage import research, features

with research.connect('data/bazaar.sqlite3') as conn:
    for row in research.history(conn, 'PRODUCT_ID', start_ms=0):
        sides = row['book_primitives']
        imbalance = features.imbalance(sides['buy_summary']['depth_5'],
                                       sides['sell_summary']['depth_5'])
        book = research.book_at(conn, row['source_updated_ms'], row['product_id'])
        if not book['available']:
            continue  # Do not attempt arbitrary-size historical book walking.
```

Readers preserve source and collection time separately. Long reads belong offline;
use short analysis windows or a consistent backup for intensive research to avoid
contending with the DELETE-journal writer. Dashboard archived-history decoding is
bounded and can reject long windows; a display limit does not delete stored history.
The dashboard labels expired/unselected books unavailable instead of empty.

## Research-universe configuration

Omitting `--research-universe` uses `provisional-all-v1`: capture every product's
lossless book while selection criteria are undecided. This maximizes preservation
and costs more than a subset. It does not claim to meet a 1–3 GB/week target.

To explicitly choose a set for **future** observations, supply JSON:

```json
{
  "version": 1,
  "policy_id": "reviewed-list-2026-09-29",
  "mode": "include",
  "products": ["YOUR_ACTUAL_PRODUCT_ID"]
}
```

```sh
.venv/bin/python src/ingestion/run_collector.py \
  --research-universe /path/to/universe.json --once
```

IDs are case-sensitive exact matches; unknown IDs cause no fabricated observations.
An explicitly empty include list archives no future product books. Use `mode=all`
with an empty products list for full capture. Unknown keys/modes, duplicate IDs and
malformed configurations fail before collection or cleanup. Configuration is loaded
once at process startup; restart to change it. Each committed snapshot stores the
complete effective policy, so later changes cannot rewrite old membership.

Future objective selection should use trailing coverage, near-top quantity and
coin-notional depth, spread distributions, order counts, and clearly labeled
snapshot-change/rolling-volume activity proxies. Use history available at each
decision, hysteresis and scheduled rebalances, and record policy versions. Do not
choose the universe from future returns or delete old books when a product leaves.
No automatic ranking or arbitrary top percentage is implemented.

## Retention and safety

CLI defaults:

```text
--poll-interval 60
--raw-retention-hours 48
--sql-book-retention-hours 24
--hot-history-hours 48
--research-universe <optional JSON path; otherwise provisional all>
--budget-gib 1
--min-free-gib 5
```

Hot quick history must outlive SQL book history. All configured durations must be
finite and positive. SQL expiry uses **durable ingestion wall time**, not API source
time or raw-file mtime; an old source fetched today is not immediately expired.
Expiry is strict `< cutoff`, so boundary-age rows survive until the next attempt.

Before each attempt, up to 16 eligible snapshots are processed. Cleanup verifies
both permanent checksums, product/level counts and policy coverage before deleting
new SQL rows, within a transaction. Children (books) expire before quick rows;
markers update in the same commit. A damaged/missing archive stops cleanup rather
than removing its recovery SQL. Large backlogs take multiple attempts to clear.
No cleanup path deletes permanent packs, metadata, the registry or backup files.
Pre-migration observations carry `legacy_protected=1` and are excluded from SQL
retention regardless of their age. No VACUUM is run automatically; freed SQLite
pages are reusable and do not imply a smaller file immediately.

Raw cleanup retains the original owned-name/direct-child/regular-file checks and
48-hour mtime window. Permanent packs are inside the database and cannot match the
raw filename cleanup. Neither migration nor profiling invokes raw cleanup.
Collection refuses V1/incomplete migrations **before** running any cleanup.

The 1 GiB default remains a stop threshold, not a promise of continuous operation.
The audit suggests the current SQL book layout needs roughly 10 GB for a 24-hour
cache at its sample cardinality, in addition to permanent growth, quick cache, raw,
legacy data, backups and headroom. Explicitly budget a supervised trial before
continuous operation. A smaller configured SQL cache can reduce working storage
without altering the one-minute permanent observations. Full-book retention for
excluded products is then correspondingly shorter; use a reviewed policy.

## Migration and recovery

```sh
.venv/bin/python tools/migrate_storage_v2.py
```

The CLI obtains the same `.collector.lock` used by the runner. It checks the V1
columns and integrity, reserves backup headroom, creates a consistent SQLite backup
in `data/backups/`, verifies integrity and SHA-256, then atomically installs V2
schema plus `backfilling` state. A `.partial` backup is never referenced as complete.
Backups are fsynced before their path/hash is committed. If interrupted before the
state commit, another run may create another backup; unreferenced/partial backups
are left for explicit inspection, never automatically removed.

Each source snapshot backfills in its own transaction, with all products' books
preserved regardless of the future collection policy. Existing quick rows and
level rows remain unchanged. The archive row keys act as restart checkpoints;
completed snapshots are skipped. Re-running verifies the recorded backup checksum,
resumes pending snapshots, verifies archives, then sets `ready`. Missing/changed
backups or malformed/incomplete V1 data fail closed. Source timestamps are never
invented and historical collection times are not replaced with migration time.

An interrupted `backfilling` database cannot collect or serve partial V2 dashboard
history. Resume the same command. Once ready, rerunning migration is a no-op.
Collection uses one transaction for all new SQL/pack/manifest rows. Failure before
commit rolls all of them back; a raw orphan remains possible under the existing
raw-before-SQL-commit durability contract. A successful source timestamp always
remains the dedup marker, even after its hot rows expire.

The pre-V2 backup is a rollback checkpoint for the migration, not a backup of later
V2 collection. To recover, stop collection, preserve the current DB and raw tree,
verify the recorded backup hash, and restore a copy to an isolated recovery root.
Do not overwrite later observations with the old backup. Regular consistent V2
backups are still needed and are not automatically scheduled by this change.

## Profiling

```sh
.venv/bin/python tools/profile_storage.py \
  --sqlite-cli /usr/bin/sqlite3 --output /tmp/v2-before.json
# After a separately budgeted supervised trial:
.venv/bin/python tools/profile_storage.py \
  --sqlite-cli /usr/bin/sqlite3 \
  --previous /tmp/v2-before.json --output /tmp/v2-after.json
```

The standard-library profiler reports total database/sidecar sizes, owned raw gzip
and legacy raw files, backups, snapshots, products, temporary SQL row counts,
protected legacy counts, permanent compact observation/book counts, compressed
payload sizes and bytes/snapshot. Optional dbstat reports table/index allocation
and total permanent B-tree allocation including metadata. Archive files are not
separate files in this implementation: their physical storage is part of SQLite.

Raw/backup inventories are separate. Temporary and protected old rows share the
original SQL tables, so their individual physical page sizes cannot be exactly
split with dbstat; row counts distinguish them and the combined table allocation
is reported. `sql_size_estimates` also reports a labeled row-weighted allocation
estimate for each class; different row widths limit its precision.
Payload lengths exclude B-tree overhead. Net SQL row decreases are
expected under V2 retention; permanent block/snapshot counts must not decrease.

`comparison.permanent_archive_delta` projects compact/books payload growth
separately. With dbstat, `permanent_btree_projection` includes measured permanent
B-tree allocation growth. SQLite total-file delta is not labeled permanent growth
under V2: it includes hot-cache fill and free-page reuse. Migration changes schema,
so start a fresh before/after baseline afterward; comparing across the migration is
intentionally refused. Exact counts/time-bounded profiling may contend with a live
writer; prefer stopped-session measurements. The tool creates no DB and never
runs cleanup or compaction.

## Validation and files

Implementation modules: `src/storage/{schema,features,codec,universe,archive,
retention,migration,research}.py`; ingestion integration in
`src/ingestion/{bazaar_collector,run_collector}.py`; archive-aware dashboard reads
in `src/dashboard/data.py` and availability messaging in `pages.py`.
Tools: `tools/migrate_storage_v2.py` and extended `tools/profile_storage.py`.
The Dockerfile includes these tools. For a mounted V1 root, run
`docker run --rm --mount type=bind,source="$PWD/data",target=/app/data --entrypoint python bazaartracker tools/migrate_storage_v2.py`
before starting the new collector image. The image was not built during validation.
Tests: `tests/test_storage_v2.py`, existing profiler/collector/dashboard suites.
No additional runtime dependencies are required.
Final verification: all 63 tests passed with
`.venv-dashboard/bin/python -m unittest discover -s tests -v`.

See [V2 validation evidence](storage/v2-validation.json),
[pre-migration profile](storage/v2-before-migration.json) and
[post-migration profile](storage/v2-after-migration.json). Validation includes
fixture tests for features/edge cases, universe changes, exact roundtrips,
rollback, expiry boundaries, corruption refusal, migration backup/interruption,
restart deduplication and archive-aware dashboard queries. Real-data verification
compares original SQL rows against the backup and checks every archived level.
An offline replay of existing raw data tests the **new** write/expiry/restart path
in a disposable root. No unattended collection or HTTP fetch is part of validation.
The [final profile](storage/v2-final-profile.json) includes SQL-class size estimates.

Measured archive sizes and extrapolated rates are summarized in the validation
report. They describe this short sample and provisional all-products policy, not a
sustained collection trial or guaranteed weekly capacity. Existing SQL history and
its backup remain, so the live database/directory initially **grows** during this
non-destructive migration. Savings apply to the representation of future permanent
observations, not to deletion of old data.

Measured on the 26-snapshot offline replay:

| Component | Measured bytes added | Extrapolated GB/week at 1,440 snapshots/day |
|---|---:|---:|
| Permanent compact payloads, all products | 3,845,457 | 1.491 |
| Permanent book payloads, provisional all products | 7,384,838 | 2.863 |
| All permanent B-trees including metadata/registry | 11,399,168 | 4.419 |

The actual permanent B-tree delta was about 11.40 MB for these observations,
compared with the audited V1 database stock of 189.15 MB for the same observations
(about 94% less space for the new permanent representation). Extrapolating the
new measured delta gives 0.631 GB/day, 18.940 GB/30 days and 110.484 GB/25 weeks.
These are **projections**, not observed day/week growth. They exclude temporary
caches, raw buffers, backup replication and future changes in product/book activity.
The provisional all-products policy exceeds the earlier 1–3 GB/week target. No
universe was narrowed to make the numbers fit.

The live database after additive migration is 200,577,024 bytes. It still contains
all original SQL rows plus the new packs. The verified V1 backup is 189,149,184
bytes. On the disposable replay, expiry removed the temporary rows but left the
database file allocation unchanged; its free pages are available for reuse.
