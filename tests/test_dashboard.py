"""All databases here are disposable fixtures; never use the collector database."""
from contextlib import closing
import hashlib
import importlib.util
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from src.dashboard import data


def fixture(path, end):
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.executescript('''
        CREATE TABLE snapshots(source_updated_ms INTEGER PRIMARY KEY, collected_at_utc TEXT, product_count INTEGER);
        CREATE TABLE quick_status(source_updated_ms INTEGER, product_id TEXT, sell_price REAL,
          sell_volume INTEGER, sell_orders INTEGER, buy_price REAL, buy_volume INTEGER,
          buy_orders INTEGER, sell_moving_week INTEGER, buy_moving_week INTEGER,
          PRIMARY KEY(source_updated_ms, product_id));
        CREATE TABLE order_book_levels(source_updated_ms INTEGER, product_id TEXT, api_side TEXT,
          level_index INTEGER, price_per_unit REAL, amount INTEGER, orders INTEGER,
          PRIMARY KEY(source_updated_ms, product_id, api_side, level_index));
        ''')
        for offset in (7200, 600, 540, 60):
            stamp = end - offset * 1000
            conn.execute('INSERT INTO snapshots VALUES(?, ?, 2)', (stamp, data.utc(stamp)))
            for item in ('APPLE', 'PEAR'):
                conn.execute('INSERT INTO quick_status VALUES(?, ?, 2, 20, 3, 4, 40, 5, 100, 200)', (stamp, item))
                if item == 'APPLE':
                    conn.execute("INSERT INTO order_book_levels VALUES(?, ?, 'buy_summary', 0, 4, 10, 2)", (stamp, item))


class DashboardDataTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / 'fixture.sqlite3'
        self.end = int(time.time() * 1000)
        fixture(self.db, self.end)

    def test_health(self):
        h = data.health(self.db)
        self.assertEqual(h['summary']['count'], 4)
        self.assertEqual(h['latest']['product_count'], 2)
        self.assertEqual(h['latest']['source_updated_ms'], self.end - 60000)
        self.assertTrue(h['latest']['collected_at_utc'].endswith('+00:00'))

    def test_missing_and_empty_and_schema_error(self):
        missing = self.db.parent / 'missing.db'
        with self.assertRaises(data.Unavailable):
            data.health(missing)
        self.assertFalse(missing.exists())
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute('DELETE FROM snapshots')
            conn.execute('DELETE FROM quick_status')
        self.assertIsNone(data.health(self.db)['latest'])
        self.assertEqual(data.items(self.db), ([], False))
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute('DROP TABLE snapshots')
        with self.assertRaises(data.Unavailable):
            data.health(self.db)

    def test_selection_windows_and_injection(self):
        self.assertEqual(data.items(self.db), (['APPLE', 'PEAR'], False))
        for hours, count in ((1, 3), (6, 4), (24, 4)):
            found, limited = data.history(self.db, 'APPLE', self.end - hours * 3600000, self.end)
            self.assertEqual(len(found), count)
            self.assertFalse(limited)
            self.assertTrue(all(row['product_id'] == 'APPLE' for row in found))
        self.assertEqual(data.history(self.db, "' OR 1=1 --", 0, self.end)[0], [])

    def test_books_empty_and_stale(self):
        current, book, limited = data.current_item(self.db, 'APPLE', self.end)
        self.assertEqual(book[0]['api_side'], 'buy_summary')
        self.assertEqual(book[0]['price_per_unit'], 4)
        self.assertEqual(current['buy_orders'], 5)
        self.assertFalse(limited)
        self.assertEqual(data.current_item(self.db, 'PEAR', self.end)[1], [])
        self.assertGreater(data.age_seconds(current['source_updated_ms'], self.end + 3600000), data.STALE_SECONDS)

    def test_gaps_and_segments(self):
        snapshots, _ = data.timeline(self.db, self.end - 3600000, self.end)
        history, _ = data.history(self.db, 'APPLE', self.end - 3600000, self.end)
        self.assertEqual(len(data.gaps(snapshots)), 1)
        points = data.chart_records(history, snapshots)
        self.assertEqual([p['segment'] for p in points[::2]], ['0', '0', '1'])
        # Missing an item in an otherwise healthy sequence must also split lines.
        contiguous = [dict(history[0]), dict(history[1]), dict(history[1])]
        contiguous[2]['source_updated_ms'] += 60000
        contiguous[2]['collected_at_utc'] = data.utc(contiguous[2]['source_updated_ms'])
        points = data.chart_records([contiguous[0], contiguous[2]], contiguous)
        self.assertEqual([p['segment'] for p in points[::2]], ['0', '1'])

    def test_readonly_queries_and_file_unchanged(self):
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        for query in ('CREATE TABLE forbidden(x)', 'DELETE FROM snapshots', 'PRAGMA user_version=2', "ATTACH DATABASE ':memory:' AS other"):
            with self.subTest(query=query), self.assertRaises(data.Unavailable):
                with data.connect(self.db) as conn:
                    conn.execute(query)
        with self.assertRaises(data.Unavailable):
            with data.connect(self.db) as conn:
                conn.set_authorizer(None)
                conn.execute('DELETE FROM snapshots')  # URI mode=ro independently denies writes.
        data.health(self.db)
        data.items(self.db)
        data.timeline(self.db, 0, self.end)
        data.history(self.db, 'APPLE', 0, self.end)
        data.current_item(self.db, 'APPLE', self.end)
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before)
        self.assertEqual(sorted(p.name for p in self.db.parent.iterdir()), ['fixture.sqlite3'])

    def test_busy_database(self):
        with closing(sqlite3.connect(self.db)) as writer, writer:
            writer.execute('BEGIN EXCLUSIVE')
            started = time.monotonic()
            with self.assertRaises(data.Unavailable):
                data.health(self.db)
            self.assertLess(time.monotonic() - started, 1)

    def test_caps_and_storage(self):
        with patch.object(data, 'MAX_POINTS', 2):
            result, limited = data.history(self.db, 'APPLE', 0, self.end)
            self.assertTrue(limited)
            self.assertEqual(len(result), 2)
        raw = self.db.parent / 'raw'
        raw.mkdir()
        (raw / 'new.json.gz').write_bytes(b'12345')
        (raw / 'legacy.json').write_bytes(b'123')
        (raw / 'old.db').write_bytes(b'1234567890')
        sizes = data.storage_sizes(self.db, raw)
        self.assertEqual(sizes['raw'], 8)
        self.assertEqual(sizes['database'], self.db.stat().st_size)
        self.assertTrue(data.storage_sizes(self.db, raw, max_entries=1)['partial'])


@unittest.skipUnless(importlib.util.find_spec('streamlit'), 'install requirements-dashboard.txt for UI tests')
class DashboardAppTests(unittest.TestCase):
    def test_app_selection_window_refresh_and_stale(self):
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'test.sqlite3'
            fixture(db, int(time.time() * 1000) - 600000)
            with patch.dict(os.environ, {'BAZAAR_DASHBOARD_DB': str(db), 'BAZAAR_DASHBOARD_RAW': str(Path(folder) / 'raw')}):
                app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'src/dashboard/app.py')).run(timeout=30)
                self.assertFalse(app.exception)
                self.assertTrue(any('stale' in w.value for w in app.warning))
                app.selectbox[1].select('PEAR').run()
                self.assertFalse(app.exception)
                self.assertTrue(any('Empty stored book' in w.value for w in app.info))
                app.selectbox[0].select(6).run()
                self.assertFalse(app.exception)
                old = app.session_state['revision']
                app.button[0].click().run()
                self.assertNotEqual(old, app.session_state['revision'])
                self.assertFalse(app.exception)

    def test_app_empty_and_missing(self):
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'test.sqlite3'
            with patch.dict(os.environ, {'BAZAAR_DASHBOARD_DB': str(db)}):
                app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'src/dashboard/app.py')).run()
                self.assertFalse(app.exception)
                self.assertTrue(app.warning)
                fixture(db, int(time.time() * 1000))
                with closing(sqlite3.connect(db)) as conn, conn:
                    conn.execute('DELETE FROM snapshots')
                app.button[0].click().run()
                self.assertFalse(app.exception)
                self.assertTrue(any('No snapshots' in message.value for message in app.info))


if __name__ == '__main__':
    unittest.main()
