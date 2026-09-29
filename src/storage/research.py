from contextlib import contextmanager
from pathlib import Path
import sqlite3
from . import archive, schema


@contextmanager
def connect(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=1)
    try:
        conn.execute('PRAGMA query_only=ON')
        schema.require_ready(conn)
        yield conn
    finally:
        conn.close()


def history(conn, product_id, start_ms=0, end_ms=2**63 - 1):
    for (source,) in conn.execute('SELECT source_updated_ms FROM compact_history WHERE source_updated_ms BETWEEN ? AND ? ORDER BY source_updated_ms', (start_ms, end_ms)):
        for observation in archive.observations(conn, source):
            if observation['product_id'] == product_id:
                yield observation
                break


def book_at(conn, source_ms, product_id):
    return archive.read_book(conn, source_ms, product_id, require_permanent=True)
