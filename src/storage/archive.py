import json
import time
from . import codec, features, universe

QUICK_COLUMNS = ('sell_price', 'sell_volume', 'sell_orders', 'buy_price', 'buy_volume',
                 'buy_orders', 'sell_moving_week', 'buy_moving_week')


def write_snapshot(conn, source, quick, products, policy, legacy=False, stored_at_ms=None):
    if not conn.in_transaction:
        raise ValueError('archive writes require the snapshot transaction')
    policy = universe.normalize(policy)
    compact, books = [], []
    input_levels = retained_levels = 0
    for row in sorted(quick, key=lambda r: r[1]):
        pid = row[1]
        conn.execute('INSERT OR IGNORE INTO products(product_id) VALUES(?)', (pid,))
        key = conn.execute('SELECT product_key FROM products WHERE product_id=?', (pid,)).fetchone()[0]
        product = products[pid]
        derived = features.primitives(product)
        compact.append([key, list(row[2:]), *[[derived[side][field] for field in features.SIDE_FIELDS]
                                             for side in features.SIDES]])
        input_levels += sum(len(product[side]) for side in features.SIDES)
        if universe.selected(policy, pid):
            book = [key, *[[[r['pricePerUnit'], r['amount'], r['orders']] for r in product[side]]
                           for side in features.SIDES]]
            books.append(book)
            retained_levels += len(book[1]) + len(book[2])
    packed, checksum = codec.encode('compact', source, compact)
    full, full_checksum = codec.encode('books', source, books)
    if codec.decode(packed, checksum, 'compact', source) != compact or codec.decode(full, full_checksum, 'books', source) != books:
        raise codec.ArchiveError('archive roundtrip failed')
    conn.execute('INSERT INTO compact_history VALUES(?,?,?,?,?,?)',
                 (source, codec.CODEC, features.FEATURE_VERSION, len(compact), checksum, packed))
    conn.execute('INSERT INTO historical_books VALUES(?,?,?,?,?,?)',
                 (source, codec.CODEC, len(books), retained_levels, full_checksum, full))
    conn.execute('INSERT INTO v2_snapshots(source_updated_ms,stored_at_ms,legacy_protected,universe_json,input_level_count) VALUES(?,?,?,?,?)',
                 (source, int(time.time() * 1000) if stored_at_ms is None else stored_at_ms,
                  int(legacy), json.dumps(policy, sort_keys=True), input_levels))


def unpack(conn, source, kind):
    table = {'compact': 'compact_history', 'books': 'historical_books'}[kind]
    found = conn.execute(f'SELECT codec,product_count,checksum,payload FROM {table} WHERE source_updated_ms=?', (source,)).fetchone()
    if found is None:
        raise codec.ArchiveError('missing permanent ' + kind + ' pack')
    if found[0] != codec.CODEC:
        raise codec.ArchiveError('unsupported archive codec')
    rows = codec.decode(found[3], found[2], kind, source)
    if len(rows) != found[1] or len({r[0] for r in rows}) != len(rows):
        raise codec.ArchiveError('archive product count/identity mismatch')
    return rows


def observations(conn, source):
    registry = dict(conn.execute('SELECT product_key,product_id FROM products'))
    collected = conn.execute('SELECT collected_at_utc FROM snapshots WHERE source_updated_ms=?', (source,)).fetchone()
    result = []
    for key, quick, buy, sell in unpack(conn, source, 'compact'):
        if key not in registry or len(quick) != len(QUICK_COLUMNS) or any(len(side) != len(features.SIDE_FIELDS) for side in (buy, sell)):
            raise codec.ArchiveError('invalid compact record')
        result.append({'source_updated_ms': source, 'product_id': registry[key],
                       'collected_at_utc': collected[0], **dict(zip(QUICK_COLUMNS, quick)),
                       'feature_version': features.FEATURE_VERSION,
                       'book_primitives': {side: dict(zip(features.SIDE_FIELDS, values))
                                           for side, values in zip(features.SIDES, (buy, sell))}})
    return result


def read_book(conn, source, product_id, require_permanent=True):
    key = conn.execute('SELECT product_key FROM products WHERE product_id=?', (product_id,)).fetchone()
    manifest = conn.execute('SELECT sql_books_pruned FROM v2_snapshots WHERE source_updated_ms=?', (source,)).fetchone()
    if manifest is None:
        return {'available': False, 'permanent': False, 'reason': 'not_archived', 'levels': []}
    for product, buy, sell in unpack(conn, source, 'books'):
        if key is not None and key[0] == product:
            levels = [{'api_side': side, 'level_index': index, 'price_per_unit': entry[0],
                       'amount': entry[1], 'orders': entry[2]}
                      for side, book in zip(features.SIDES, (buy, sell)) for index, entry in enumerate(book)]
            return {'available': True, 'permanent': True, 'reason': 'complete_api_summaries', 'levels': levels}
    if not require_permanent and not manifest[0]:
        if conn.execute('SELECT 1 FROM quick_status WHERE source_updated_ms=? AND product_id=?', (source, product_id)).fetchone():
            rows = conn.execute('SELECT api_side,level_index,price_per_unit,amount,orders FROM order_book_levels WHERE source_updated_ms=? AND product_id=? ORDER BY api_side,level_index', (source, product_id))
            return {'available': True, 'permanent': False, 'reason': 'temporary_sql',
                    'levels': [dict(zip(('api_side','level_index','price_per_unit','amount','orders'), row)) for row in rows]}
    return {'available': False, 'permanent': False, 'reason': 'outside_universe_or_product_absent', 'levels': []}


def verify_snapshot(conn, source):
    compact = unpack(conn, source, 'compact')
    books = unpack(conn, source, 'books')
    expected = conn.execute('SELECT product_count FROM snapshots WHERE source_updated_ms=?', (source,)).fetchone()[0]
    if len(compact) != expected:
        raise codec.ArchiveError('snapshot/compact count mismatch')
    expected_levels = conn.execute('SELECT level_count FROM historical_books WHERE source_updated_ms=?', (source,)).fetchone()[0]
    if sum(len(r[1]) + len(r[2]) for r in books) != expected_levels:
        raise codec.ArchiveError('book level count mismatch')
    policy_json = conn.execute('SELECT universe_json FROM v2_snapshots WHERE source_updated_ms=?', (source,)).fetchone()[0]
    policy = universe.normalize(json.loads(policy_json))
    ids = dict(conn.execute('SELECT product_key,product_id FROM products'))
    expected_keys = {r[0] for r in compact if universe.selected(policy, ids[r[0]])}
    if {r[0] for r in books} != expected_keys:
        raise codec.ArchiveError('archive does not cover recorded universe')
