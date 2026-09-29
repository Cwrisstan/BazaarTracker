"""Additive, resumable V1 backfill. No SQL/raw retention runs during migration."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import sqlite3
import uuid
from . import archive, schema, universe


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def backup(conn, storage):
    storage.check(storage.db.stat().st_size * 2)
    folder = storage.root / 'backups'
    if folder.is_symlink():
        raise ValueError('backup directory must not be a symlink')
    folder.mkdir(exist_ok=True)
    path = folder / ('before-v2-' + uuid.uuid4().hex + '.sqlite3')
    staging = path.with_suffix('.sqlite3.partial')
    # A partial backup never becomes the backup referenced by migration state.
    with closing(sqlite3.connect(staging)) as destination:
        conn.backup(destination)
        if destination.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('backup integrity check failed')
    with staging.open('rb') as stream:
        os.fsync(stream.fileno())
    os.replace(staging, path)
    descriptor = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path, digest(path)


def snapshot_input(conn, source):
    quick = [tuple(row) for row in conn.execute('SELECT * FROM quick_status WHERE source_updated_ms=? ORDER BY product_id', (source,))]
    products = {row[1]: {'buy_summary': [], 'sell_summary': []} for row in quick}
    for _, pid, side, index, price, amount, orders in conn.execute('SELECT * FROM order_book_levels WHERE source_updated_ms=? ORDER BY product_id,api_side,level_index', (source,)):
        if pid not in products or side not in products[pid] or index != len(products[pid][side]):
            raise ValueError('cannot migrate incomplete/noncontiguous V1 books')
        products[pid][side].append({'pricePerUnit': price, 'amount': amount, 'orders': orders})
    expected = conn.execute('SELECT product_count FROM snapshots WHERE source_updated_ms=?', (source,)).fetchone()[0]
    if len(quick) != expected:
        raise ValueError('cannot migrate incomplete V1 product history')
    return quick, products


def migrate(storage, after_snapshot=None):
    """Caller holds the collector lock. Every existing observation stays in place."""
    if not storage.db.is_file() or storage.db.is_symlink():
        raise ValueError('migration requires an existing regular V1 database')
    with closing(sqlite3.connect(storage.db.as_uri() + '?mode=rw', uri=True)) as conn:
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA synchronous=FULL')
        status = schema.state(conn)
        if status == 'ready':
            return {'state': 'ready', 'backfilled': 0}
        if status is None:
            if conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or conn.execute('PRAGMA foreign_key_check').fetchone():
                raise ValueError('source database integrity check failed')
            # Refuse unexpected tables lacking the audited V1 columns.
            expected = {'snapshots': ('source_updated_ms','collected_at_utc','product_count'),
                        'quick_status': ('source_updated_ms','product_id',*archive.QUICK_COLUMNS),
                        'order_book_levels': ('source_updated_ms','product_id','api_side','level_index','price_per_unit','amount','orders')}
            for table, columns in expected.items():
                if tuple(r[1] for r in conn.execute('PRAGMA table_info(' + table + ')')) != columns:
                    raise ValueError('unsupported V1 schema: ' + table)
            path, checksum = backup(conn, storage)
            with conn:
                conn.execute('BEGIN IMMEDIATE')
                schema.create(conn, datetime.now(timezone.utc).isoformat(), str(path), checksum)
        row = conn.execute('SELECT backup_path,backup_sha256 FROM storage_migrations WHERE version=2').fetchone()
        if not row or digest(row[0]) != row[1]:
            raise ValueError('migration backup missing or checksum changed; refusing resume')
        pending = [r[0] for r in conn.execute('SELECT source_updated_ms FROM snapshots WHERE source_updated_ms NOT IN (SELECT source_updated_ms FROM v2_snapshots) ORDER BY source_updated_ms')]
        count = 0
        for source in pending:
            storage.check()
            quick, products = snapshot_input(conn, source)
            with conn:
                conn.execute('BEGIN IMMEDIATE')
                archive.write_snapshot(conn, source, quick, products, universe.DEFAULT, legacy=True)
                archive.verify_snapshot(conn, source)
            count += 1
            if after_snapshot:
                after_snapshot(source)  # Tests can interrupt after a durable checkpoint.
        with conn:
            conn.execute('BEGIN IMMEDIATE')
            for (source,) in conn.execute('SELECT source_updated_ms FROM snapshots'):
                archive.verify_snapshot(conn, source)
            conn.execute("UPDATE storage_migrations SET state='ready',completed_at_utc=? WHERE version=2",
                         (datetime.now(timezone.utc).isoformat(),))
        return {'state': 'ready', 'backfilled': count, 'backup': row[0], 'backup_sha256': row[1]}
