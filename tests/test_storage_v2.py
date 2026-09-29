"""Storage V2 contracts, crash recovery and retention on disposable databases."""
from contextlib import closing
from copy import deepcopy
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import threading
import unittest
from unittest.mock import patch, Mock

from src.ingestion import bazaar_collector as c
from src.storage import archive, codec, features, migration, research, retention, schema, universe
from src.dashboard import data
from tools.profile_storage import measure, compare


def payload(source=1000):
    return {'success': True, 'lastUpdated': source, 'products': {
        pid: {'product_id': pid, 'quick_status': {'productId': pid, **{
            key: 2.5 if key.endswith('Price') else 10 for key in c.QUICK_FIELDS}},
            'buy_summary': [{'pricePerUnit': 4.0, 'amount': 30, 'orders': 3},
                            {'pricePerUnit': 3.5, 'amount': 20, 'orders': 2}],
            'sell_summary': [{'pricePerUnit': 2.0, 'amount': 10, 'orders': 1}]}
        for pid in ('APPLE', 'PEAR')}}


def v1_database(path):
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript('''
            CREATE TABLE snapshots(source_updated_ms INTEGER PRIMARY KEY,collected_at_utc TEXT,product_count INTEGER);
            CREATE TABLE quick_status(source_updated_ms INTEGER,product_id TEXT,sell_price REAL,sell_volume INTEGER,sell_orders INTEGER,buy_price REAL,buy_volume INTEGER,buy_orders INTEGER,sell_moving_week INTEGER,buy_moving_week INTEGER,PRIMARY KEY(source_updated_ms,product_id));
            CREATE TABLE order_book_levels(source_updated_ms INTEGER,product_id TEXT,api_side TEXT,level_index INTEGER,price_per_unit REAL,amount INTEGER,orders INTEGER,PRIMARY KEY(source_updated_ms,product_id,api_side,level_index));
        ''')
        for source in (1000, 2000):
            _, quick, levels = c.validate(payload(source))
            conn.execute('INSERT INTO snapshots VALUES(?,?,?)', (source, data.utc(source), len(quick)))
            conn.executemany('INSERT INTO quick_status VALUES(?,?,?,?,?,?,?,?,?,?)', quick)
            conn.executemany('INSERT INTO order_book_levels VALUES(?,?,?,?,?,?,?)', levels)
        conn.commit()


def old_rows(conn):
    return {table: [tuple(row) for row in conn.execute('SELECT * FROM ' + table + ' ORDER BY 1,2')]
            for table in ('snapshots', 'quick_status', 'order_book_levels')}


class FeatureTests(unittest.TestCase):
    def test_sort_depth_imbalance_vwap_and_no_mutation(self):
        product = payload()['products']['APPLE']
        original = deepcopy(product)
        result = features.primitives(product)
        buy, sell = result['buy_summary'], result['sell_summary']
        self.assertEqual(buy['best_price'], 3.5)
        self.assertEqual(buy['depth_1'], 20)
        self.assertEqual(buy['depth_5'], 50)
        self.assertEqual(buy['depth_10'], 50)
        self.assertEqual(buy['level_count'], 2)
        self.assertEqual(buy['summary_depth'], 50)
        self.assertEqual(features.vwap(buy), 3.8)
        self.assertAlmostEqual(features.imbalance(buy['depth_5'], sell['depth_5']), 2/3)
        self.assertEqual(product, original)

    def test_empty_zero_quantity_zero_price_and_side_sort(self):
        empty = features.side_primitives([], 'buy_summary')
        self.assertIsNone(empty['best_price'])
        self.assertEqual(empty['flags'], features.EMPTY)
        self.assertEqual(empty['summary_depth'], 0)
        self.assertIsNone(features.vwap(empty))
        self.assertIsNone(features.imbalance(0, 0))
        self.assertIsNone(features.imbalance(None, 10))
        book = [{'pricePerUnit': 0, 'amount': 0, 'orders': 0}, {'pricePerUnit': 2, 'amount': 0, 'orders': 0}]
        side = features.side_primitives(book, 'sell_summary')
        self.assertEqual(side['best_price'], 2)
        self.assertTrue(side['flags'] & features.ZERO_PRICE)
        self.assertIsNone(features.vwap(side))

    def test_malformed_partial_and_large_notional(self):
        for book in (None, {}, [None], [{'pricePerUnit': 1, 'amount': 2}], [{'pricePerUnit': float('nan'), 'amount': 2, 'orders': 1}]):
            with self.subTest(book=book), self.assertRaises(ValueError):
                features.side_primitives(book, 'buy_summary')
        big = features.side_primitives([{'pricePerUnit': 1e308, 'amount': 2**63-1, 'orders': 1}], 'buy_summary')
        self.assertEqual(features.vwap(big), 1e308)
        self.assertGreater(len(big['notional_5']), 300)
        thirty = features.side_primitives([{'pricePerUnit': i, 'amount': i, 'orders': 1} for i in range(30)], 'buy_summary')
        self.assertTrue(thirty['flags'] & features.AT_API_LIMIT)
        self.assertEqual(thirty['depth_5'], 10)
        self.assertEqual(thirty['depth_10'], 45)


class V2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = c.Storage(self.temp.name, min_free=1, headroom=1024)
        self.conn = c.open_database(self.storage)
        self.addCleanup(lambda: self.conn.close())

    def persist(self, p=None, stored=100000):
        p = p or payload()
        c.persist(self.conn, self.storage, p)
        with self.conn:
            self.conn.execute('UPDATE v2_snapshots SET stored_at_ms=? WHERE source_updated_ms=?', (stored, p['lastUpdated']))
        return p['lastUpdated']

    def test_compact_and_books_exact_roundtrip_source_collection(self):
        p = payload()
        p['products']['PEAR']['buy_summary'] = []
        p['products']['PEAR']['sell_summary'] = []
        self.persist(p)
        found = archive.observations(self.conn, 1000)
        self.assertEqual(len(found), 2)
        self.assertEqual(found[0]['buy_price'], 2.5)
        self.assertEqual(found[0]['book_primitives']['buy_summary']['depth_1'], 20)
        self.assertNotEqual(found[0]['collected_at_utc'], data.utc(1000))
        for pid in p['products']:
            book = archive.read_book(self.conn, 1000, pid)
            self.assertTrue(book['available'])
            self.assertTrue(book['permanent'])
            for side in features.SIDES:
                decoded = [{'pricePerUnit': r['price_per_unit'], 'amount': r['amount'], 'orders': r['orders']}
                           for r in book['levels'] if r['api_side'] == side]
                self.assertEqual(decoded, p['products'][pid][side])
        archive.verify_snapshot(self.conn, 1000)

    def test_duplicate_restart_after_expiry(self):
        self.persist()
        retention.prune(self.conn, now_ms=100000+48*3600000+1)
        self.conn.close()
        self.conn = c.open_database(self.storage)
        self.assertFalse(c.persist(self.conn, self.storage, payload()))
        self.assertEqual(self.conn.execute('SELECT count(*) FROM compact_history').fetchone()[0], 1)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM order_book_levels').fetchone()[0], 0)
        with research.connect(self.storage.db) as reader:
            observations = list(research.history(reader, 'APPLE'))
            self.assertEqual(len(observations), 1)
            self.assertTrue(research.book_at(reader, 1000, 'APPLE')['available'])
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute('DELETE FROM compact_history')

    def test_retention_boundaries_permanent_and_legacy_protection(self):
        self.persist()
        before = self.conn.execute('SELECT payload FROM compact_history').fetchone()[0]
        self.assertEqual(retention.prune(self.conn, now_ms=100000+24*3600000)['order_book_levels'], 0)
        self.assertEqual(retention.prune(self.conn, now_ms=100000+24*3600000+1)['order_book_levels'], 6)
        self.assertEqual(retention.prune(self.conn, now_ms=100000+48*3600000)['quick_status'], 0)
        self.assertEqual(retention.prune(self.conn, now_ms=100000+48*3600000+1)['quick_status'], 2)
        self.assertEqual(self.conn.execute('SELECT payload FROM compact_history').fetchone()[0], before)
        self.persist(payload(2000))
        with self.conn:
            self.conn.execute('UPDATE v2_snapshots SET legacy_protected=1 WHERE source_updated_ms=2000')
        self.assertEqual(retention.prune(self.conn, now_ms=10**15), {'order_book_levels': 0, 'quick_status': 0})
        self.assertEqual(self.conn.execute('SELECT count(*) FROM order_book_levels').fetchone()[0], 6)

    def test_configurable_universe_missing_is_not_empty(self):
        self.storage.research_universe = universe.normalize({'version': 1, 'policy_id': 'test-selection', 'mode': 'include', 'products': ['PEAR']})
        self.persist()
        self.assertFalse(archive.read_book(self.conn, 1000, 'APPLE')['available'])
        self.assertTrue(archive.read_book(self.conn, 1000, 'APPLE', False)['available'])
        self.assertTrue(archive.read_book(self.conn, 1000, 'PEAR')['available'])
        self.assertEqual(len(archive.observations(self.conn, 1000)), 2)
        retention.prune(self.conn, now_ms=100000+24*3600000+1)
        current, book, _ = data.current_item(self.storage.db, 'APPLE', 1000)
        self.assertFalse(current['book_available'])
        self.assertEqual(book, [])
        self.storage.research_universe = universe.DEFAULT
        self.persist(payload(2000))
        self.assertFalse(archive.read_book(self.conn, 1000, 'APPLE')['available'])
        self.assertTrue(archive.read_book(self.conn, 2000, 'APPLE')['available'])

    def test_empty_universe_and_empty_products(self):
        self.storage.research_universe = {'version': 1, 'policy_id': 'explicit-none', 'mode': 'include', 'products': []}
        self.persist()
        self.assertEqual(self.conn.execute('SELECT product_count,level_count FROM historical_books').fetchone(), (0,0))
        self.persist({'success': True, 'lastUpdated': 2000, 'products': {}})
        archive.verify_snapshot(self.conn, 2000)
        self.assertEqual(archive.observations(self.conn, 2000), [])

    def test_archive_failure_rolls_back_every_table(self):
        with patch.object(archive.codec, 'encode', side_effect=codec.ArchiveError('injected')):
            with self.assertRaises(codec.ArchiveError):
                self.persist()
        for table in ('snapshots','quick_status','order_book_levels','products','compact_history','historical_books','v2_snapshots'):
            self.assertEqual(self.conn.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0)
        self.assertFalse(list(self.storage.raw.iterdir()))

    def test_raw_failure_and_commit_failure_roll_back_archives(self):
        with patch.object(self.storage, 'write_raw', side_effect=OSError('injected')):
            with self.assertRaises(OSError):
                self.persist()
        self.assertEqual(self.conn.execute('SELECT count(*) FROM compact_history').fetchone()[0], 0)
        self.conn.execute('CREATE TABLE broken(x INTEGER REFERENCES snapshots(source_updated_ms) DEFERRABLE INITIALLY DEFERRED)')
        self.conn.execute('CREATE TRIGGER break_v2 AFTER INSERT ON v2_snapshots BEGIN INSERT INTO broken VALUES(-1); END')
        with self.assertRaises(sqlite3.IntegrityError):
            self.persist()
        self.assertEqual(self.conn.execute('SELECT count(*) FROM historical_books').fetchone()[0], 0)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM products').fetchone()[0], 0)

    def test_corruption_prevents_pruning_and_detected_on_read(self):
        self.persist()
        with self.conn:
            self.conn.execute("UPDATE historical_books SET payload=x'00'")
        with self.assertRaises(codec.ArchiveError):
            retention.prune(self.conn, now_ms=10**15)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM order_book_levels').fetchone()[0], 6)
        self.assertEqual(self.conn.execute('SELECT sql_books_pruned FROM v2_snapshots').fetchone()[0], 0)
        with self.assertRaises(codec.ArchiveError):
            archive.read_book(self.conn, 1000, 'APPLE')

    def test_raw_cleanup_cannot_touch_permanent_packs_or_backups(self):
        self.persist()
        checksum = self.conn.execute('SELECT checksum FROM historical_books').fetchone()[0]
        backup_dir = self.storage.root / 'backups'; backup_dir.mkdir()
        protected = backup_dir / 'bazaar-v1-999.json.gz'; protected.write_bytes(b'backup')
        raw = next(self.storage.raw.glob('*.gz')); os.utime(raw, (0,0))
        self.assertEqual(self.storage.cleanup(now=48*3600), 0)
        self.assertEqual(self.storage.cleanup(now=48*3600+1), 1)
        self.assertTrue(protected.exists())
        self.assertEqual(self.conn.execute('SELECT checksum FROM historical_books').fetchone()[0], checksum)
        archive.verify_snapshot(self.conn, 1000)

    def test_dashboard_reads_archived_history_and_displays_empty_accurately(self):
        self.persist()
        self.persist(payload(2000))
        retention.prune(self.conn, now_ms=10**15)
        self.assertEqual(data.items(self.storage.db), (['APPLE','PEAR'], False))
        found, limited = data.history(self.storage.db, 'APPLE', 0, 3000)
        self.assertEqual([r['source_updated_ms'] for r in found], [1000,2000])
        self.assertFalse(limited)
        self.assertEqual(data.current_item(self.storage.db, 'APPLE', 3000)[0]['source_updated_ms'], 2000)
        self.assertEqual(len(data.market_snapshot(self.storage.db, 3000)[1]), 2)

    def test_profiler_separates_permanent_and_temporary_growth(self):
        self.persist()
        before = measure(self.storage.db, self.storage.raw)
        self.persist(payload(2000))
        retention.prune(self.conn, now_ms=10**15)
        after = measure(self.storage.db, self.storage.raw)
        before['measured_at_utc'] = '2026-01-01T00:00:00+00:00'
        after['measured_at_utc'] = '2026-01-01T01:00:00+00:00'
        diff = compare(before, after)
        self.assertEqual(diff['new_rows']['quick_status'], -2)
        self.assertIsNone(diff['permanent_database_projection'])
        self.assertGreater(diff['permanent_archive_delta']['compact_history']['payload_bytes'], 0)
        self.assertEqual(after['storage_v2']['compact_history']['observations'], 4)
        self.assertEqual(after['storage_v2']['historical_books']['levels'], 12)
        self.assertEqual(after['storage_v2']['sql_row_classes']['order_book_levels']['temporary'], 0)

    def test_startup_expires_raw_before_budget_check(self):
        from src.ingestion import run_collector as runner
        self.persist()
        old = next(self.storage.raw.glob('*.gz'))
        old.write_bytes(b'x' * 10000); os.utime(old, (0,0))
        budget = (self.storage.db.stat().st_size + 2048) / c.GIB
        args = runner.parser().parse_args(['--data-dir',str(self.storage.root), '--once',
            '--budget-gib',str(budget),'--min-free-gib','0.000001','--headroom-mib',str(1024/1024**2)])
        with patch.object(runner, 'fetch', return_value=payload()):
            self.assertEqual(runner.run(args, threading.Event(), Mock()), 0)
        self.assertFalse(old.exists())
        archive.verify_snapshot(self.conn, 1000)

    def test_retention_config_invalid_and_batch_limit(self):
        self.persist()
        self.persist(payload(2000))
        for options in ({'book_hours': 0}, {'book_hours': 49}, {'quick_hours': float('inf')}, {'batch_size': 0}):
            with self.assertRaises(ValueError):
                retention.prune(self.conn, **options)
        self.assertEqual(retention.prune(self.conn, now_ms=10**15, batch_size=1)['quick_status'], 2)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM quick_status').fetchone()[0], 2)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.storage = c.Storage(self.temp.name, min_free=1, headroom=1024)
        v1_database(self.storage.db)
        with closing(sqlite3.connect(self.storage.db)) as conn:
            self.original = old_rows(conn)

    def test_backup_backfill_preserves_every_original_observation(self):
        result = migration.migrate(self.storage)
        self.assertEqual(result['backfilled'], 2)
        self.assertEqual(migration.digest(result['backup']), result['backup_sha256'])
        with closing(sqlite3.connect(result['backup'])) as backup:
            self.assertEqual(old_rows(backup), self.original)
        with closing(c.open_database(self.storage)) as conn:
            self.assertEqual(old_rows(conn), self.original)
            self.assertEqual(conn.execute('SELECT count(*) FROM v2_snapshots WHERE legacy_protected=1').fetchone()[0], 2)
            retention.prune(conn, now_ms=10**15)
            self.assertEqual(old_rows(conn), self.original)
            self.assertEqual(len(archive.observations(conn, 1000)), 2)
        self.assertEqual(migration.migrate(self.storage)['backfilled'], 0)

    def test_interrupted_migration_detected_and_resumed(self):
        with self.assertRaises(RuntimeError):
            migration.migrate(self.storage, after_snapshot=lambda _: (_ for _ in ()).throw(RuntimeError('interrupted')))
        with self.assertRaises(ValueError):
            c.open_database(self.storage)
        with closing(sqlite3.connect(self.storage.db)) as conn:
            self.assertEqual(schema.state(conn), 'backfilling')
            self.assertEqual(conn.execute('SELECT count(*) FROM compact_history').fetchone()[0], 1)
            self.assertEqual(old_rows(conn), self.original)
        self.assertEqual(migration.migrate(self.storage)['backfilled'], 1)
        self.assertEqual(len(list((self.storage.root / 'backups').glob('*.sqlite3'))), 1)

    def test_missing_backup_refuses_resume_and_keeps_original(self):
        with self.assertRaises(RuntimeError):
            migration.migrate(self.storage, after_snapshot=lambda _: (_ for _ in ()).throw(RuntimeError('interrupted')))
        next((self.storage.root / 'backups').glob('*.sqlite3')).unlink()
        with self.assertRaises(FileNotFoundError):
            migration.migrate(self.storage)
        with closing(sqlite3.connect(self.storage.db)) as conn:
            self.assertEqual(old_rows(conn), self.original)

    def test_migration_transaction_failure_leaves_no_partial_snapshot(self):
        original_writer = archive.write_snapshot
        def fail(*args, **kwargs):
            original_writer(*args, **kwargs)
            raise RuntimeError('before commit')
        with patch.object(archive, 'write_snapshot', side_effect=fail), self.assertRaises(RuntimeError):
            migration.migrate(self.storage)
        with closing(sqlite3.connect(self.storage.db)) as conn:
            self.assertEqual(old_rows(conn), self.original)
            self.assertEqual(conn.execute('SELECT count(*) FROM compact_history').fetchone()[0], 0)
        self.assertEqual(migration.migrate(self.storage)['backfilled'], 2)

    def test_interrupted_backup_leaves_v1_ready_for_retry(self):
        with patch.object(migration.os, 'replace', side_effect=OSError('before backup publish')):
            with self.assertRaises(OSError):
                migration.migrate(self.storage)
        with closing(sqlite3.connect(self.storage.db)) as conn:
            self.assertIsNone(schema.state(conn))
            self.assertEqual(old_rows(conn), self.original)
        self.assertTrue(list((self.storage.root / 'backups').glob('*.partial')))
        self.assertEqual(migration.migrate(self.storage)['backfilled'], 2)

    def test_v1_runner_refuses_before_cleanup_or_fetch(self):
        from src.ingestion import run_collector as runner
        raw = self.storage.raw / 'bazaar-v1-1.json.gz'
        raw.write_bytes(b'original'); os.utime(raw, (0,0))
        args = runner.parser().parse_args(['--data-dir',str(self.storage.root),'--once','--min-free-gib','0.000001'])
        session = Mock()
        with self.assertRaises(ValueError):
            runner.run(args, threading.Event(), session)
        session.get.assert_not_called()
        self.assertEqual(raw.read_bytes(), b'original')

    def test_collection_refuses_v1_without_mutation(self):
        before = self.storage.db.read_bytes()
        with self.assertRaises(ValueError):
            c.open_database(self.storage)
        self.assertEqual(self.storage.db.read_bytes(), before)


class ConfigurationAndCodecTests(unittest.TestCase):
    def test_universe_validation(self):
        self.assertTrue(universe.selected(universe.load(), 'ANY'))
        for config in ({}, {**universe.DEFAULT, 'mode': 'top25'}, {**universe.DEFAULT, 'products': ['X']},
                       {**universe.DEFAULT, 'mode':'include','products':['X','X']}, {**universe.DEFAULT,'version':True}):
            with self.assertRaises(ValueError):
                universe.normalize(config)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'universe.json'
            path.write_text(json.dumps({'version':1,'policy_id':'my-policy','mode':'include','products':['PEAR']}))
            self.assertTrue(universe.selected(universe.load(path), 'PEAR'))
            self.assertFalse(universe.selected(universe.load(path), 'APPLE'))

    def test_codec_identity_checksum_and_size_limit(self):
        blob, checksum = codec.encode('books', 1000, [[1, [], []]])
        self.assertEqual(codec.decode(blob, checksum, 'books', 1000), [[1,[],[]]])
        for kind, source in [('compact',1000),('books',2000)]:
            with self.assertRaises(codec.ArchiveError):
                codec.decode(blob, checksum, kind, source)
        with self.assertRaises(codec.ArchiveError):
            codec.decode(blob+b'x', checksum, 'books', 1000)
        with patch.object(codec, 'MAX_DECODED_BYTES', 1), self.assertRaises(codec.ArchiveError):
            codec.decode(blob, checksum, 'books', 1000)
