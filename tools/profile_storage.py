import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time

TABLES = ('snapshots', 'quick_status', 'order_book_levels')
OWNED = re.compile(r'bazaar-v1-[1-9][0-9]*\.json\.gz\Z')
DBSTAT_SQL = ('SELECT name, count(*) AS pages, sum(pgsize) AS bytes, '
              'sum(payload) AS payload_bytes, sum(unused) AS unused_bytes '
              'FROM dbstat GROUP BY name ORDER BY name')


def inventory(root):
    files, skipped = {}, []
    if not root.exists():
        return {'files': files, 'bytes': 0, 'file_count': 0, 'skipped_symlinks': skipped,
                'owned_snapshot_count': 0, 'owned_snapshot_bytes': 0}
    def walk(directory):
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                relative = str(path.relative_to(root))
                if entry.is_symlink():
                    skipped.append(relative)
                elif entry.is_dir(follow_symlinks=False):
                    walk(path)
                elif entry.is_file(follow_symlinks=False):
                    stat = entry.stat(follow_symlinks=False)
                    files[relative] = {'bytes': stat.st_size,
                        'allocated_bytes': stat.st_blocks * 512 if hasattr(stat, 'st_blocks') else None,
                        'mtime_ns': stat.st_mtime_ns,
                        'owned_snapshot': path.parent == root and bool(OWNED.fullmatch(path.name))}
    if root.is_symlink():
        raise ValueError('raw directory must not be a symlink')
    walk(root)
    return {'files': files, 'bytes': sum(f['bytes'] for f in files.values()),
            'file_count': len(files), 'skipped_symlinks': skipped,
            'owned_snapshot_count': sum(f['owned_snapshot'] for f in files.values()),
            'owned_snapshot_bytes': sum(f['bytes'] for f in files.values() if f['owned_snapshot'])}


def measure(database, raw, timeout=30, sqlite_cli=None):
    database, raw = Path(database).absolute(), Path(raw).absolute()
    if database.is_symlink():
        raise ValueError('database must not be a symlink')
    database, raw = database.resolve(), raw.resolve() if not raw.is_symlink() else raw
    stat = database.stat()
    started = time.time()
    report = {'format_version': 1, 'measured_at_utc': datetime.now(timezone.utc).isoformat(),
              'database': str(database), 'raw_directory': str(raw),
              'database_identity': [stat.st_dev, stat.st_ino], 'sqlite_version': sqlite3.sqlite_version,
              'database_bytes': stat.st_size,
              'database_allocated_bytes': stat.st_blocks * 512 if hasattr(stat, 'st_blocks') else None,
              'warnings': ['SQL counts are one read transaction; filesystem sizes may race an active collector. '
                           'Stop collection between measurements for clean comparisons.']}
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=min(timeout, 1))) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA query_only=ON')
        deadline = time.monotonic() + timeout
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        conn.execute('BEGIN')
        report['schema'] = [dict(r) for r in conn.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','index') ORDER BY name")]
        table_names = {r['name'] for r in report['schema'] if r['type'] == 'table'}
        counted = TABLES + tuple(t for t in ('compact_history', 'historical_books', 'v2_snapshots', 'products') if t in table_names)
        report['counts'] = {table: conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in counted}
        if 'compact_history' in table_names:
            report['storage_v2'] = {
                'migration': dict(conn.execute('SELECT * FROM storage_migrations WHERE version=2').fetchone()),
                'compact_history': dict(conn.execute('SELECT count(*) AS blocks, coalesce(sum(product_count),0) AS observations, coalesce(sum(length(payload)),0) AS payload_bytes FROM compact_history').fetchone()),
                'historical_books': dict(conn.execute('SELECT count(*) AS blocks, coalesce(sum(product_count),0) AS product_books, coalesce(sum(level_count),0) AS levels, coalesce(sum(length(payload)),0) AS payload_bytes FROM historical_books').fetchone()),
                'sql_row_classes': {},
                'note': 'Permanent gzip blocks live inside SQLite, not separate archive files. Payload bytes exclude B-tree overhead. Old protected SQL and temporary SQL share tables; dbstat cannot split their pages exactly.'}
            for table in ('quick_status', 'order_book_levels'):
                report['storage_v2']['sql_row_classes'][table] = {
                    label: conn.execute(f'SELECT count(*) FROM {table} q JOIN v2_snapshots v USING(source_updated_ms) WHERE v.legacy_protected=?', (flag,)).fetchone()[0]
                    for label, flag in [('legacy_protected', 1), ('temporary', 0)]}
            for name in ('compact_history', 'historical_books'):
                item = report['storage_v2'][name]
                item['payload_bytes_per_snapshot'] = item['payload_bytes'] / item['blocks'] if item['blocks'] else None
        report['product_count_distinct'] = conn.execute('SELECT count(DISTINCT product_id) FROM quick_status').fetchone()[0]
        report['latest_product_count'] = conn.execute('SELECT count(*) FROM quick_status WHERE source_updated_ms=(SELECT max(source_updated_ms) FROM snapshots)').fetchone()[0]
        if 'products' in table_names:
            report['product_count_distinct'] = conn.execute('SELECT count(*) FROM products').fetchone()[0]
            latest = conn.execute('SELECT product_count FROM snapshots ORDER BY source_updated_ms DESC LIMIT 1').fetchone()
            report['latest_product_count'] = latest[0] if latest else 0
        report['range'] = dict(conn.execute('SELECT min(source_updated_ms) AS first_source_ms, max(source_updated_ms) AS last_source_ms, min(collected_at_utc) AS first_collection, max(collected_at_utc) AS last_collection FROM snapshots').fetchone())
        report['pages'] = {key: conn.execute('PRAGMA ' + key).fetchone()[0] for key in ('page_count', 'page_size', 'freelist_count', 'auto_vacuum')}
        report['journal_mode'] = conn.execute('PRAGMA journal_mode').fetchone()[0]
        try:
            report['objects'] = [dict(r) for r in conn.execute(DBSTAT_SQL)]
            report['objects_source'] = 'Python SQLite dbstat in count transaction'
        except sqlite3.OperationalError as exc:
            if 'no such table: dbstat' not in str(exc):
                raise
            report['objects'] = None
            report['warnings'].append('Python SQLite lacks dbstat; use --sqlite-cli /usr/bin/sqlite3 if available.')
        conn.rollback()
    if report['objects'] is None and sqlite_cli:
        result = subprocess.run([sqlite_cli, '-readonly', '-json', str(database), DBSTAT_SQL],
                                capture_output=True, text=True, timeout=timeout, check=True)
        report['objects'] = json.loads(result.stdout)
        report['objects_source'] = 'SQLite CLI dbstat (separate read; may differ during collection)'
    report['sidecar_bytes'] = {}
    for suffix in ('-journal', '-wal', '-shm'):
        path = Path(str(database) + suffix)
        if path.is_symlink():
            raise ValueError('SQLite sidecar must not be a symlink')
        report['sidecar_bytes'][suffix] = path.stat().st_size if path.exists() else 0
    report['raw'] = inventory(raw)
    report['backups'] = inventory(database.parent / 'backups')
    if report.get('storage_v2') and report['objects'] is not None:
        report['storage_v2']['table_and_index_bytes'] = {
            table: sum(obj['bytes'] for obj in report['objects']
                       if obj['name'] in {entry['name'] for entry in report['schema'] if entry['tbl_name'] == table})
            for table in ('compact_history', 'historical_books', 'quick_status', 'order_book_levels')}
        report['storage_v2']['permanent_total_btree_bytes'] = sum(
            obj['bytes'] for obj in report['objects'] if obj['name'] in {
                entry['name'] for entry in report['schema'] if entry['tbl_name'] in (
                    'compact_history', 'historical_books', 'snapshots', 'v2_snapshots', 'products', 'storage_migrations')})
        report['storage_v2']['sql_size_estimates'] = {}
        for table, counts in report['storage_v2']['sql_row_classes'].items():
            total = sum(counts.values())
            allocated = report['storage_v2']['table_and_index_bytes'][table]
            report['storage_v2']['sql_size_estimates'][table] = {
                label + '_bytes_estimate': allocated * count / total if total else 0
                for label, count in counts.items()}
        report['storage_v2']['sql_size_estimate_method'] = (
            'Row-count weighted share of combined table/index B-tree bytes; approximate because row widths differ. '
            'Empty table roots and freelist pages are not attributed to a row class.')
    n = report['counts']['snapshots']
    report['average_per_stored_snapshot'] = {
        'database_bytes': report['database_bytes'] / n if n else None,
        'rows': {table: count / n if n else None for table, count in report['counts'].items()},
        'note': 'Stock averages, not measured marginal growth; DB includes indexes/free pages.'}
    owned = report['raw'].get('owned_snapshot_count', 0)
    report['mean_retained_raw_snapshot_bytes'] = report['raw'].get('owned_snapshot_bytes', 0) / owned if owned else None
    report['measurement_seconds'] = time.time() - started
    return report


def projections(bytes_per_snapshot, interval):
    if bytes_per_snapshot is None:
        return None
    daily = bytes_per_snapshot * 86400 / interval
    return {key: daily * days for key, days in [('day_bytes', 1), ('week_bytes', 7), ('month_30d_bytes', 30), ('weeks_25_bytes', 175)]}


def compare(before, after, interval=60):
    for key in ('format_version', 'database', 'database_identity', 'raw_directory', 'schema'):
        if before[key] != after[key]:
            raise ValueError(f'incompatible measurements: {key} changed')
    elapsed = (datetime.fromisoformat(after['measured_at_utc']) - datetime.fromisoformat(before['measured_at_utc'])).total_seconds()
    if elapsed <= 0:
        raise ValueError('previous measurement must be older')
    counts = {key: after['counts'][key] - before['counts'][key] for key in before['counts']}
    permanent = set(counts) - {'quick_status', 'order_book_levels'} if after.get('storage_v2') else set(counts)
    if any(counts[key] < 0 for key in permanent):
        raise ValueError('row counts decreased; cannot treat this interval as append-only growth')
    n = counts['snapshots']
    old, new = before['raw']['files'], after['raw']['files']
    added, removed = new.keys() - old.keys(), old.keys() - new.keys()
    changed = [key for key in new.keys() & old.keys() if new[key] != old[key]]
    retained_new_raw = [new[key]['bytes'] for key in added if new[key]['owned_snapshot']]
    db_delta = after['database_bytes'] - before['database_bytes']
    raw_mean = sum(retained_new_raw) / len(retained_new_raw) if retained_new_raw else None
    result = {'elapsed_seconds': elapsed, 'new_rows': counts,
        'database_delta_bytes': db_delta, 'raw_net_delta_bytes': after['raw']['bytes'] - before['raw']['bytes'],
        'raw_added_files': len(added), 'raw_removed_files': len(removed), 'raw_changed_files': changed,
        'raw_added_bytes': sum(new[k]['bytes'] for k in added),
        'raw_removed_bytes': sum(old[k]['bytes'] for k in removed),
        'new_retained_owned_raw_count': len(retained_new_raw),
        'database_bytes_per_new_snapshot': db_delta / n if n else None,
        'rows_per_new_snapshot': {key: value / n if n else None for key, value in counts.items()},
        'mean_new_retained_raw_bytes': raw_mean,
        'assumed_unique_snapshot_interval_seconds': interval,
        'permanent_database_projection': projections(db_delta / n, interval) if n and db_delta >= 0 else None,
        'raw_production_projection_sample_estimate': projections(raw_mean, interval),
        'raw_buffer_24h_bytes_estimate': raw_mean * 86400 / interval if raw_mean is not None else None,
        'raw_buffer_48h_bytes_estimate': raw_mean * 172800 / interval if raw_mean is not None else None,
        'observed_new_snapshots_per_hour': n * 3600 / elapsed,
        'notes': ['Projection assumes one unique committed snapshot per configured interval, not merely one HTTP request.',
                  'Raw net delta includes retention. Added surviving gzip files estimate production, not files already deleted between measurements.',
                  'Database file growth can understate new live data when SQLite reuses free pages; compare dbstat and freelist.',
                  'Observed wall-time rate includes pauses/errors and must not be mistaken for continuous collection.']}
    if before['objects'] is not None and after['objects'] is not None:
        previous = {r['name']: r['bytes'] for r in before['objects']}
        result['object_delta_bytes'] = {r['name']: r['bytes'] - previous.get(r['name'], 0) for r in after['objects']}
    if after.get('storage_v2'):
        for table in ('quick_status', 'order_book_levels'):
            if after['storage_v2']['sql_row_classes'][table]['legacy_protected'] < before['storage_v2']['sql_row_classes'][table]['legacy_protected']:
                raise ValueError('protected historical SQL row counts decreased')
        result['permanent_database_projection'] = None
        result['notes'].append('V2 database net growth mixes permanent archives, temporary SQL and page reuse; use permanent archive deltas below.')
        result['permanent_archive_delta'] = {}
        for name in ('compact_history', 'historical_books'):
            delta = after['storage_v2'][name]['payload_bytes'] - before['storage_v2'][name]['payload_bytes']
            result['permanent_archive_delta'][name] = {
                'payload_bytes': delta, 'payload_bytes_per_new_snapshot': delta / n if n else None,
                'projection_excluding_btree_overhead': projections(delta / n, interval) if n and delta >= 0 else None}
        if 'permanent_total_btree_bytes' in after['storage_v2'] and 'permanent_total_btree_bytes' in before['storage_v2']:
            delta = after['storage_v2']['permanent_total_btree_bytes'] - before['storage_v2']['permanent_total_btree_bytes']
            result['permanent_btree_delta_bytes'] = delta
            result['permanent_btree_projection'] = projections(delta / n, interval) if n and delta >= 0 else None
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=Path(__file__).resolve().parents[1] / 'data/bazaar.sqlite3')
    parser.add_argument('--raw-dir', type=Path)
    parser.add_argument('--output', type=Path, help='New JSON file outside database/raw directories; never overwrites')
    parser.add_argument('--previous', type=Path, help='Earlier JSON measurement')
    parser.add_argument('--sqlite-cli', help='Optional SQLite executable with dbstat support (e.g. /usr/bin/sqlite3)')
    parser.add_argument('--timeout', type=float, default=30, help='SQL read budget in seconds; counts may be expensive')
    parser.add_argument('--interval-seconds', type=float, default=60, help='Assumed unique snapshot interval for projections')
    args = parser.parse_args(argv)
    try:
        if not all(math.isfinite(v) and v > 0 for v in (args.timeout, args.interval_seconds)):
            raise ValueError('timeout and interval must be finite and positive')
        raw = args.raw_dir or args.database.parent / 'raw'
        if args.output:
            output = args.output.resolve()
            for protected in (args.database.resolve().parent, raw.resolve()):
                if output == protected or protected in output.parents:
                    raise ValueError('save measurements outside the database/raw directories')
        previous = json.loads(args.previous.read_text()) if args.previous else None
        report = measure(args.database, raw, args.timeout, args.sqlite_cli)
        if previous:
            report['comparison'] = compare(previous, report, args.interval_seconds)
        rendered = json.dumps(report, indent=2, sort_keys=True) + '\n'
        if args.output:
            with args.output.open('x') as stream:
                stream.write(rendered)
            print(f'Saved read-only measurement to {args.output}')
        else:
            print(rendered, end='')
        return 0
    except (OSError, ValueError, KeyError, sqlite3.Error, subprocess.SubprocessError) as exc:
        parser.exit(1, f'Profile failed (no database/raw writes requested): {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())
