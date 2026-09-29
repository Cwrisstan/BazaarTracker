SCHEMA_VERSION = 2
DDL = (
    '''CREATE TABLE storage_migrations (
        version INTEGER PRIMARY KEY, state TEXT NOT NULL CHECK(state IN ('backfilling','ready')),
        backup_path TEXT, backup_sha256 TEXT, started_at_utc TEXT NOT NULL, completed_at_utc TEXT)''',
    '''CREATE TABLE products (
        product_key INTEGER PRIMARY KEY, product_id TEXT NOT NULL UNIQUE)''',
    '''CREATE TABLE compact_history (
        source_updated_ms INTEGER PRIMARY KEY REFERENCES snapshots(source_updated_ms),
        codec TEXT NOT NULL CHECK(codec='json-gzip-v1'), feature_version INTEGER NOT NULL CHECK(feature_version=1),
        product_count INTEGER NOT NULL, checksum TEXT NOT NULL, payload BLOB NOT NULL)''',
    '''CREATE TABLE historical_books (
        source_updated_ms INTEGER PRIMARY KEY REFERENCES snapshots(source_updated_ms),
        codec TEXT NOT NULL CHECK(codec='json-gzip-v1'), product_count INTEGER NOT NULL,
        level_count INTEGER NOT NULL, checksum TEXT NOT NULL, payload BLOB NOT NULL)''',
    '''CREATE TABLE v2_snapshots (
        source_updated_ms INTEGER PRIMARY KEY REFERENCES snapshots(source_updated_ms),
        stored_at_ms INTEGER NOT NULL, legacy_protected INTEGER NOT NULL CHECK(legacy_protected IN (0,1)),
        universe_json TEXT NOT NULL, input_level_count INTEGER NOT NULL,
        sql_books_pruned INTEGER NOT NULL DEFAULT 0 CHECK(sql_books_pruned IN (0,1)),
        quick_pruned INTEGER NOT NULL DEFAULT 0 CHECK(quick_pruned IN (0,1)),
        FOREIGN KEY(source_updated_ms) REFERENCES compact_history(source_updated_ms),
        FOREIGN KEY(source_updated_ms) REFERENCES historical_books(source_updated_ms))''',
    '''CREATE INDEX v2_retention ON v2_snapshots(legacy_protected, stored_at_ms)''',
)


def exists(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='storage_migrations'").fetchone() is not None


def state(conn):
    if not exists(conn):
        return None
    rows = conn.execute('SELECT version,state FROM storage_migrations').fetchall()
    if len(rows) != 1 or rows[0][0] != SCHEMA_VERSION:
        raise ValueError('unsupported Storage schema version')
    return rows[0][1]


def create(conn, now, backup_path=None, backup_sha256=None):
    for sql in DDL:
        conn.execute(sql)
    conn.execute('INSERT INTO storage_migrations VALUES(?,?,?,?,?,?)',
                 (SCHEMA_VERSION, 'backfilling' if backup_path else 'ready', backup_path,
                  backup_sha256, now, None if backup_path else now))


def require_ready(conn):
    if state(conn) != 'ready':
        raise ValueError('Storage V2 migration required/incomplete: run tools/migrate_storage_v2.py before collection')
