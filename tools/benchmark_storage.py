"""Offline sizing experiment, not an archive writer or migration.

Requires pyarrow; reads V1 using mode=ro, writes only into an automatically removed
TemporaryDirectory, and emits sizes as JSON. Retains float64 prices/int64 counts.
"""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile


def benchmark(database):
    import pyarrow as pa
    import pyarrow.parquet as pq
    with tempfile.TemporaryDirectory(prefix='bazaar-sizing-') as folder, closing(
            sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        quick = [dict(r) for r in conn.execute('SELECT * FROM quick_status ORDER BY source_updated_ms,product_id')]
        products = {pid: i for i, pid in enumerate(sorted({r['product_id'] for r in quick}))}
        for row in quick:
            row['product_key'] = products[row.pop('product_id')]
        timestamps = [r[0] for r in conn.execute('SELECT source_updated_ms FROM snapshots ORDER BY source_updated_ms')]
        root = Path(folder)
        pq.write_table(pa.Table.from_pylist(quick), root / 'quick.parquet', compression='zstd')
        pq.write_table(pa.table({'product_id': list(products), 'product_key': list(products.values())}), root / 'products.parquet', compression='zstd')
        features = []
        schema = pa.schema([('source_updated_ms', pa.int64()), ('product_key', pa.int32()),
                            ('side', pa.int8()), ('level_index', pa.int32()),
                            ('price_per_unit', pa.float64()), ('amount', pa.int64()), ('orders', pa.int64())])
        with pq.ParquetWriter(root / 'levels.parquet', schema, compression='zstd') as writer:
            for stamp in timestamps:
                books = {}
                rows = []
                for r in conn.execute('SELECT * FROM order_book_levels WHERE source_updated_ms=? ORDER BY product_id,api_side,level_index', (stamp,)):
                    row = dict(r)
                    key = products[row.pop('product_id')]
                    side = row.pop('api_side')
                    books.setdefault((key, side), []).append(row)
                    rows.append({**row, 'product_key': key, 'side': 0 if side == 'buy_summary' else 1})
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                for q in (r for r in quick if r['source_updated_ms'] == stamp):
                    f = dict(q)
                    for side in ('buy_summary', 'sell_summary'):
                        book = sorted(books.get((q['product_key'], side), []), key=lambda r: r['price_per_unit'], reverse=side == 'sell_summary')
                        prefix = side.removesuffix('_summary')
                        f[prefix + '_best'] = book[0]['price_per_unit'] if book else None
                        f[prefix + '_level_count'] = len(book)
                        f[prefix + '_summary_depth'] = sum(r['amount'] for r in book)
                        for k in (1, 5, 10):
                            f[f'{prefix}_depth_{k}'] = sum(r['amount'] for r in book[:k])
                        for k in (5, 10):
                            f[f'{prefix}_notional_{k}'] = sum(r['price_per_unit'] * r['amount'] for r in book[:k])
                    features.append(f)
        pq.write_table(pa.Table.from_pylist(features), root / 'features.parquet', compression='zstd')
        # Verify lossless book roundtrip against source rows before using the size.
        checked = 0
        source = iter(conn.execute('SELECT source_updated_ms,product_id,api_side,level_index,price_per_unit,amount,orders FROM order_book_levels ORDER BY source_updated_ms,product_id,api_side,level_index'))
        reverse_products = {v: k for k, v in products.items()}
        for batch in pq.ParquetFile(root / 'levels.parquet').iter_batches():
            for row in batch.to_pylist():
                original = tuple(next(source))
                decoded = (row['source_updated_ms'], reverse_products[row['product_key']],
                           'buy_summary' if row['side'] == 0 else 'sell_summary', row['level_index'],
                           row['price_per_unit'], row['amount'], row['orders'])
                if decoded != original:
                    raise ValueError('book roundtrip mismatch')
                checked += 1
        if next(source, None) is not None:
            raise ValueError('book roundtrip omitted rows')
        with closing(sqlite3.connect(root / 'compact.sqlite3')) as trial:
            trial.execute('CREATE TABLE quick_status (source_updated_ms INTEGER,product_key INTEGER,sell_price REAL,sell_volume INTEGER,sell_orders INTEGER,buy_price REAL,buy_volume INTEGER,buy_orders INTEGER,sell_moving_week INTEGER,buy_moving_week INTEGER,PRIMARY KEY(source_updated_ms,product_key)) WITHOUT ROWID')
            columns = ('source_updated_ms','product_key','sell_price','sell_volume','sell_orders','buy_price','buy_volume','buy_orders','sell_moving_week','buy_moving_week')
            trial.executemany('INSERT INTO quick_status VALUES (?,?,?,?,?,?,?,?,?,?)', [tuple(r[k] for k in columns) for r in quick])
            trial.commit()
        return {'snapshots': len(timestamps), 'products': len(products), 'quick_rows': len(quick),
                'level_rows_roundtrip_verified': checked, 'pyarrow_version': pa.__version__,
                'bytes': {p.name: p.stat().st_size for p in root.iterdir()},
                'notes': ['26-snapshot sample is not a sustained daily benchmark; extrapolations are provisional.',
                          'Features include quick fields plus 16 book primitives, not a finalized production schema.',
                          'Compact SQLite file includes quick rows only; add product dictionary, metadata and any extra indexes.',
                          'Archive files omit production manifest/checksum/quality metadata; include these in a later acceptance benchmark.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=Path(__file__).resolve().parents[1] / 'data/bazaar.sqlite3')
    args = parser.parse_args()
    print(json.dumps(benchmark(args.database), indent=2))
