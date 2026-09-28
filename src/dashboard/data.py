"""Bounded, read-only queries. No ingestion imports or network access."""
from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
import time

MAX_POINTS = 3000
MAX_ITEMS = 5000
GAP_SECONDS = 180
STALE_SECONDS = 180


class Unavailable(Exception):
    pass


@contextmanager
def connect(path):
    """No creation, write statements, temporary tables, or long-held read locks."""
    conn = None
    try:
        conn = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=0.15)
        conn.row_factory = sqlite3.Row
        # Whitelist only read opcodes, also denying ATTACH and writable PRAGMAs.
        allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION}
        conn.set_authorizer(lambda action, *_: sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
        deadline = time.monotonic() + 0.75
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        yield conn
    except sqlite3.Error as exc:
        raise Unavailable('Database unavailable, busy, incompatible, or query exceeded its 0.75s budget. Try Refresh. ' + str(exc)) from exc
    finally:
        if conn is not None:
            conn.close()


def rows(conn, sql, parameters=()):
    return [dict(row) for row in conn.execute(sql, parameters).fetchall()]


def health(path):
    with connect(path) as conn:
        summary = rows(conn, '''SELECT count(*) AS count, min(source_updated_ms) AS first_source,
            max(source_updated_ms) AS last_source, min(collected_at_utc) AS first_collection,
            max(collected_at_utc) AS last_collection FROM snapshots''')[0]
        latest = rows(conn, 'SELECT * FROM snapshots ORDER BY source_updated_ms DESC LIMIT 1')
    return {'summary': summary, 'latest': latest[0] if latest else None}


def items(path):
    with connect(path) as conn:
        found = rows(conn, 'SELECT DISTINCT product_id FROM quick_status LIMIT ?', (MAX_ITEMS + 1,))
    return sorted(row['product_id'] for row in found[:MAX_ITEMS]), len(found) > MAX_ITEMS


def timeline(path, start_ms, end_ms):
    with connect(path) as conn:
        result = rows(conn, '''SELECT source_updated_ms, collected_at_utc FROM snapshots
            WHERE source_updated_ms BETWEEN ? AND ? ORDER BY source_updated_ms DESC LIMIT ?''',
            (start_ms, end_ms, MAX_POINTS + 1))
    return list(reversed(result[:MAX_POINTS])), len(result) > MAX_POINTS


def history(path, item, start_ms, end_ms):
    with connect(path) as conn:
        result = rows(conn, '''SELECT q.*, s.collected_at_utc FROM quick_status q
            JOIN snapshots s ON s.source_updated_ms=q.source_updated_ms
            WHERE q.source_updated_ms BETWEEN ? AND ? AND q.product_id=?
            ORDER BY q.source_updated_ms DESC LIMIT ?''', (start_ms, end_ms, item, MAX_POINTS + 1))
    return list(reversed(result[:MAX_POINTS])), len(result) > MAX_POINTS


def current_item(path, item, end_ms):
    with connect(path) as conn:
        result = rows(conn, '''SELECT q.*, s.collected_at_utc FROM quick_status q
            JOIN snapshots s ON s.source_updated_ms=q.source_updated_ms
            WHERE q.source_updated_ms<=? AND q.product_id=?
            ORDER BY q.source_updated_ms DESC LIMIT 1''', (end_ms, item))
        if not result:
            return None, [], False
        latest = result[0]
        book = rows(conn, '''SELECT api_side, level_index, price_per_unit, amount, orders
            FROM order_book_levels WHERE source_updated_ms=? AND product_id=?
            ORDER BY api_side, level_index LIMIT 1001''', (latest['source_updated_ms'], item))
    return latest, book[:1000], len(book) > 1000


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def age_seconds(source_ms, now_ms):
    return (now_ms - source_ms) / 1000


def collection_ms(row):
    return datetime.fromisoformat(row['collected_at_utc']).timestamp() * 1000


def gaps(records):
    result = []
    for previous, current in zip(records, records[1:]):
        source_gap = (current['source_updated_ms'] - previous['source_updated_ms']) / 1000
        collected_gap = abs(collection_ms(current) - collection_ms(previous)) / 1000
        if max(source_gap, collected_gap) > GAP_SECONDS:
            result.append({'From source (UTC)': utc(previous['source_updated_ms']),
                           'To source (UTC)': utc(current['source_updated_ms']),
                           'Source gap (seconds)': source_gap,
                           'Collection gap (seconds)': collected_gap})
    return result


def chart_records(history_rows, snapshots):
    """Explicit groups prevent lines bridging outages or missing item observations."""
    positions = {row['source_updated_ms']: i for i, row in enumerate(snapshots)}
    result, segment, previous = [], 0, None
    for row in history_rows:
        if previous is not None:
            adjacent = (row['source_updated_ms'] in positions and previous['source_updated_ms'] in positions
                        and positions[row['source_updated_ms']] == positions[previous['source_updated_ms']] + 1)
            if not adjacent or gaps([previous, row]):
                segment += 1
        for column, label in [('buy_price', 'buyPrice'), ('sell_price', 'sellPrice')]:
            result.append({'source_utc': utc(row['source_updated_ms']), 'price': row[column],
                           'API field': label, 'segment': str(segment)})
        previous = row
    return result


def storage_sizes(database, raw_dir, max_entries=50000):
    """Read-only stat checks; never traverse symlinks; bound expensive directory scans."""
    def size(path):
        try:
            return path.stat().st_size if path.is_file() and not path.is_symlink() else 0
        except OSError:
            return 0
    database = Path(database)
    result = {'database': size(database), 'sidecars': sum(size(Path(str(database) + suffix))
              for suffix in ('-journal', '-wal', '-shm')), 'raw': 0, 'partial': False}
    pending, seen = [Path(raw_dir)], 0
    deadline = time.monotonic() + 0.75
    while pending:
        directory = pending.pop()
        if directory.is_symlink():
            continue
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    seen += 1
                    if seen > max_entries or time.monotonic() > deadline:
                        result['partial'] = True
                        return result
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False) and entry.name.endswith(('.json', '.json.gz', '.json.gz.tmp')):
                        result['raw'] += entry.stat(follow_symlinks=False).st_size
        except FileNotFoundError:
            pass
        except OSError:
            result['partial'] = True
    return result
