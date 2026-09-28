from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import requests

from src.ingestion import bazaar_collector as c
from src.ingestion import run_collector as runner


def fixture():
    return {"success": True, "lastUpdated": 1700000000123, "products": {
        "TEST": {"product_id": "TEST", "quick_status": {
            "productId": "TEST", **{key: (2.5 if key.endswith("Price") else 10) for key in c.QUICK_FIELDS}},
            "buy_summary": [{"pricePerUnit": 3.5, "amount": 20, "orders": 2},
                            {"pricePerUnit": 4.0, "amount": 30, "orders": 3}],
            "sell_summary": [{"pricePerUnit": 2.0, "amount": 10, "orders": 1}]}}}


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = c.Storage(self.temp.name, min_free=1, headroom=1024)
        self.conn = c.open_database(self.storage)
        self.addCleanup(lambda: self.conn.close())

    def test_duplicate_and_reopen_and_timestamps(self):
        payload = fixture()
        collected = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertTrue(c.persist(self.conn, self.storage, payload, collected))
        self.assertFalse(c.persist(self.conn, self.storage, payload))
        self.conn.close()
        self.conn = c.open_database(self.storage)
        self.assertFalse(c.persist(self.conn, self.storage, payload))
        self.assertEqual(self.conn.execute("SELECT * FROM snapshots").fetchall(),
                         [(payload['lastUpdated'], collected.isoformat(), 1)])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM quick_status").fetchone()[0], 1)
        raw = next(self.storage.raw.glob('*.gz'))
        self.assertEqual(json.loads(gzip.decompress(raw.read_bytes())), payload)

    def test_reconstruct_books_and_empty(self):
        payload = fixture()
        c.persist(self.conn, self.storage, payload)
        for side in ('buy_summary', 'sell_summary'):
            rows = self.conn.execute('SELECT price_per_unit, amount, orders FROM order_book_levels WHERE api_side=? ORDER BY level_index', (side,))
            self.assertEqual([dict(zip(('pricePerUnit', 'amount', 'orders'), row)) for row in rows], payload['products']['TEST'][side])
        payload['lastUpdated'] += 1
        payload['products']['TEST']['buy_summary'] = []
        payload['products']['TEST']['sell_summary'] = []
        c.persist(self.conn, self.storage, payload)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM order_book_levels WHERE source_updated_ms=?', (payload['lastUpdated'],)).fetchone()[0], 0)

    def test_invalid_whole_snapshot(self):
        variants = []
        for timestamp in (None, True, 0, 1.5, '1700000000000', 2**64, 10**400):
            p = fixture(); p['lastUpdated'] = timestamp; variants.append(p)
        p = fixture(); del p['lastUpdated']; variants.append(p)
        p = fixture(); p['success'] = False; variants.append(p)
        p = fixture(); p['products'] = []; variants.append(p)
        for entry in ({'amount': 1, 'orders': 1}, {'pricePerUnit': float('nan'), 'amount': 1, 'orders': 1}, {'pricePerUnit': 2, 'amount': -1, 'orders': 1}, None):
            p = fixture(); p['products']['TEST']['buy_summary'].append(entry); variants.append(p)
        p = fixture(); p['products']['TEST']['quick_status']['productId'] = 'OTHER'; variants.append(p)
        for payload in variants:
            with self.subTest(payload=payload), self.assertRaises(c.InvalidPayload):
                c.persist(self.conn, self.storage, payload)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0], 0)
        self.assertEqual(list(self.storage.raw.iterdir()), [])

    def test_transaction_failure_rolls_back_all_rows(self):
        self.conn.execute("CREATE TRIGGER fail_level BEFORE INSERT ON order_book_levels BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            c.persist(self.conn, self.storage, fixture())
        for table in ('snapshots', 'quick_status', 'order_book_levels'):
            self.assertEqual(self.conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)

    def test_commit_failure_leaves_recoverable_orphan(self):
        # Deferred FK fails at commit, after raw publication.
        self.conn.execute('CREATE TABLE broken(x INTEGER REFERENCES snapshots(source_updated_ms) DEFERRABLE INITIALLY DEFERRED)')
        self.conn.execute('CREATE TRIGGER break_commit AFTER INSERT ON snapshots BEGIN INSERT INTO broken VALUES (1); END')
        with self.assertRaises(sqlite3.IntegrityError):
            c.persist(self.conn, self.storage, fixture())
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0], 0)
        self.assertEqual(len(list(self.storage.raw.glob('*.gz'))), 1)
        self.conn.execute('DROP TRIGGER break_commit')
        self.conn.close()
        self.conn = c.open_database(self.storage)
        self.assertTrue(c.persist(self.conn, self.storage, fixture()))

    def test_interrupted_atomic_write_restart(self):
        with patch.object(c.os, 'replace', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                c.persist(self.conn, self.storage, fixture())
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0], 0)
        self.assertEqual(len(list(self.storage.raw.glob('*.tmp'))), 1)
        self.conn.close()
        self.conn = c.open_database(self.storage)
        self.assertTrue(c.persist(self.conn, self.storage, fixture()))
        self.assertFalse(list(self.storage.raw.glob('*.tmp')))

    def test_storage_budget_and_free_space(self):
        self.storage.budget = self.storage.usage() + self.storage.headroom + 1
        with self.assertRaises(c.StorageFull):
            c.persist(self.conn, self.storage, fixture())
        self.storage.budget = c.GIB
        with patch.object(c.shutil, 'disk_usage', return_value=Mock(free=0)):
            with self.assertRaises(c.StorageFull):
                c.persist(self.conn, self.storage, fixture())
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0], 0)
        self.assertFalse(list(self.storage.raw.iterdir()))

    def test_retention_and_accounting(self):
        names = ['bazaar-v1-1.json.gz', 'bazaar-v1-2.json.gz.tmp', 'bazaar-v1-3.json.gz', 'unrelated.json.gz', 'legacy.json', 'hypixel_bazaar.db']
        for name in names:
            p = self.storage.raw / name
            p.write_bytes(b'x' * 100)
            os.utime(p, (0, 0))
        recent = self.storage.raw / names[2]
        os.utime(recent, (200000, 200000))
        link = self.storage.raw / 'bazaar-v1-4.json.gz'
        link.symlink_to(self.storage.db)
        sidecar = Path(str(self.storage.db) + '-wal')
        sidecar.write_bytes(b'z' * 200)
        before = self.storage.usage()
        self.assertEqual(self.storage.cleanup(now=200000), 2)
        self.assertEqual(before - self.storage.usage(), 200)
        for name in names[2:]:
            self.assertTrue((self.storage.raw / name).exists())
        self.assertTrue(link.is_symlink())
        self.assertTrue(self.storage.db.exists())
        self.assertTrue(sidecar.exists())

    def test_legacy_database_untouched(self):
        legacy = self.storage.raw / 'hypixel_bazaar.db'
        with sqlite3.connect(legacy) as conn:
            conn.execute('CREATE TABLE product_quick_status(timestamp TEXT)')
            conn.execute("INSERT INTO product_quick_status VALUES ('old')")
        before = legacy.read_bytes()
        c.persist(self.conn, self.storage, fixture())
        self.assertEqual(legacy.read_bytes(), before)

    def test_retention_keeps_structured_history(self):
        c.persist(self.conn, self.storage, fixture())
        raw = next(self.storage.raw.glob('*.gz'))
        os.utime(raw, (0, 0))
        self.assertEqual(self.storage.cleanup(now=200000), 1)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0], 1)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM order_book_levels').fetchone()[0], 3)


class FetchTests(unittest.TestCase):
    def response(self, status=200):
        response = Mock(status_code=status, headers={})
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.json.return_value = fixture()
        return response

    def test_timeout_and_network(self):
        for error in (requests.Timeout(), requests.ConnectionError()):
            session = Mock(); session.get.side_effect = error
            with self.assertRaises(c.FetchError) as ctx:
                c.fetch(session, timeout=7)
            self.assertTrue(ctx.exception.retryable)
            session.get.assert_called_once_with(c.HYPIXEL_URL, timeout=7)

    def test_throttle_server_and_permanent_errors(self):
        for code in (429, 503, 401):
            session = Mock(); response = self.response(code)
            response.headers = {'Retry-After': '120'}
            session.get.return_value = response
            with self.assertRaises(c.FetchError) as ctx:
                c.fetch(session)
            self.assertEqual(ctx.exception.retryable, code != 401)
            if code != 401:
                self.assertEqual(ctx.exception.retry_after, 120)

    def test_backoff_and_dates(self):
        self.assertEqual([c.backoff(n) for n in (1, 2, 3, 30)], [2, 4, 8, 300])
        self.assertEqual(c.backoff(30, 600), 600)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(c.retry_after_seconds('Thu, 01 Jan 2026 00:02:00 GMT', now), 120)
        for value in ('bad', None, '-1', 'nan'):
            self.assertEqual(c.retry_after_seconds(value), 0)

    def test_bad_json(self):
        session = Mock(); session.get.return_value = self.response()
        session.get.return_value.json.side_effect = ValueError()
        with self.assertRaises(c.InvalidPayload):
            c.fetch(session)

    def test_scheduler_bounds_wait_and_stops(self):
        with tempfile.TemporaryDirectory() as root:
            args = runner.parser().parse_args(['--data-dir', root, '--duration', '60', '--min-free-gib', '0.000001'])
            stop = Mock(); stop.is_set.side_effect = [False, True, True]
            session = Mock(); session.get.return_value = self.response(429)
            session.get.return_value.headers = {'Retry-After': '600'}
            self.assertEqual(runner.run(args, stop, session, monotonic=lambda: 0), 0)
            stop.wait.assert_called_once_with(60)

    def test_one_shot(self):
        with tempfile.TemporaryDirectory() as root:
            args = runner.parser().parse_args(['--data-dir', root, '--once', '--min-free-gib', '0.000001'])
            session = Mock(); session.get.return_value = self.response()
            self.assertEqual(runner.run(args, threading.Event(), session), 0)
            session.get.assert_called_once()


if __name__ == '__main__':
    unittest.main()
