"""Only V2-owned temporary SQL is eligible; permanent packs and legacy rows are not."""
import math
import time
from . import archive, schema


def prune(conn, book_hours=24, quick_hours=48, now_ms=None, batch_size=16):
    if conn.in_transaction:
        raise ValueError('retention must run outside ingestion transactions')
    schema.require_ready(conn)
    if not all(math.isfinite(v) and v > 0 for v in (book_hours, quick_hours)) or quick_hours < book_hours:
        raise ValueError('retention must be positive; hot quick history must outlive SQL books')
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError('batch_size must be positive')
    now = int(time.time() * 1000) if now_ms is None else now_ms
    removed = {'order_book_levels': 0, 'quick_status': 0}
    # Newest source timestamps may arrive late; retention follows durable ingest time.
    candidates = conn.execute('''SELECT source_updated_ms,stored_at_ms,sql_books_pruned,quick_pruned
        FROM v2_snapshots WHERE legacy_protected=0 AND quick_pruned=0 AND
        ((sql_books_pruned=0 AND stored_at_ms<?) OR stored_at_ms<?)
        ORDER BY stored_at_ms LIMIT ?''',
        (now - book_hours * 3600000, now - quick_hours * 3600000, batch_size)).fetchall()
    for source, stored, books_pruned, quick_pruned in candidates:
        with conn:
            conn.execute('BEGIN IMMEDIATE')
            archive.verify_snapshot(conn, source)
            if not books_pruned and stored < now - book_hours * 3600000:
                removed['order_book_levels'] += conn.execute('DELETE FROM order_book_levels WHERE source_updated_ms=?', (source,)).rowcount
                conn.execute('UPDATE v2_snapshots SET sql_books_pruned=1 WHERE source_updated_ms=?', (source,))
            if not quick_pruned and stored < now - quick_hours * 3600000:
                removed['quick_status'] += conn.execute('DELETE FROM quick_status WHERE source_updated_ms=?', (source,)).rowcount
                conn.execute('UPDATE v2_snapshots SET quick_pruned=1 WHERE source_updated_ms=?', (source,))
    return removed
