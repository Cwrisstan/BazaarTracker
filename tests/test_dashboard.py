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

    def test_market_snapshot_endpoints_limits_and_no_fallback(self):
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        snapshot, products, limited = data.market_snapshot(self.db, self.end)
        self.assertEqual(snapshot['source_updated_ms'], self.end - 60000)
        self.assertEqual([row['product_id'] for row in products], ['APPLE', 'PEAR'])
        self.assertFalse(limited)
        self.assertTrue(all(row['source_updated_ms'] == snapshot['source_updated_ms'] for row in products))
        # A missing one-hour baseline must not become a two-hour comparison.
        self.assertEqual(data.market_snapshot(self.db, self.end - 3600000, 180000), (None, [], False))
        baseline, _, _ = data.market_snapshot(self.db, self.end - 540000, 180000)
        self.assertEqual(baseline['source_updated_ms'], self.end - 540000)
        self.assertEqual(data.market_snapshot(self.db, 0), (None, [], False))
        with patch.object(data, 'MAX_ITEMS', 1):
            _, products, limited = data.market_snapshot(self.db, self.end)
            self.assertEqual([row['product_id'] for row in products], ['APPLE'])
            self.assertTrue(limited)
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("DELETE FROM quick_status WHERE product_id='PEAR' AND source_updated_ms=?", (self.end - 60000,))
        self.assertEqual([row['product_id'] for row in data.market_snapshot(self.db, self.end)[1]], ['APPLE'])

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


@unittest.skipUnless(importlib.util.find_spec('streamlit') and importlib.util.find_spec('plotly'), 'install requirements-dashboard.txt for UI tests')
class DashboardAppTests(unittest.TestCase):
    def test_chart_changes_with_selected_item(self):
        import json
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'test.sqlite3'
            fixture(db, int(time.time() * 1000))
            with closing(sqlite3.connect(db)) as conn, conn:
                conn.execute("UPDATE quick_status SET buy_price=40, sell_price=20 WHERE product_id='PEAR'")
            with patch.dict(os.environ, {'BAZAAR_DASHBOARD_DB': str(db)}):
                app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'src/dashboard/app.py')).run()
                app.radio(key='page').set_value('Item Explorer').run()
                identities = {}
                for item, prices in [('APPLE', {2, 4}), ('PEAR', {20, 40}), ('APPLE', {2, 4})]:
                    app.selectbox(key='product').select(item).run()
                    self.assertFalse(app.exception)
                    chart = app.get('plotly_chart')[0].proto
                    spec = json.loads(chart.spec)
                    self.assertIn(item, spec['layout']['title']['text'])
                    self.assertEqual({v for trace in spec['data'] for v in trace['y'] if v is not None}, prices)
                    identities[item] = chart.id
                self.assertNotEqual(identities['APPLE'], identities['PEAR'])
                for mode in ['Shared scale', 'Indexed (first = 100)', 'Separate scales']:
                    app.selectbox(key='price_mode').select(mode).run()
                    self.assertFalse(app.exception)
                app.multiselect(key='price_series').set_value([]).run()
                self.assertTrue(any('Select a price series' in w.value for w in app.info))

    def test_pages_windows_filters_refresh_and_stale(self):
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / 'test.sqlite3'
            fixture(db, int(time.time() * 1000) - 600000)
            with patch.dict(os.environ, {'BAZAAR_DASHBOARD_DB': str(db), 'BAZAAR_DASHBOARD_RAW': str(Path(folder) / 'raw')}):
                app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / 'src/dashboard/app.py')).run(timeout=30)
                self.assertFalse(app.exception)
                self.assertTrue(any('stale' in w.value for w in app.warning))
                self.assertEqual(len(app.get('plotly_chart')), 2)
                self.assertTrue(any('Not enough historical data' in w.value for w in app.info))
                app.selectbox(key='change_period').select('Recorded range').run()
                app.radio(key='page').set_value('Item Explorer').run()
                app.selectbox(key='product').select('PEAR').run()
                self.assertFalse(app.exception)
                self.assertTrue(any('Empty stored book' in w.value for w in app.info))
                for label in ['1h', '6h', '24h', '7d', 'All']:
                    app.segmented_control(key='item_window').set_value(label).run()
                    self.assertFalse(app.exception)
                app.radio(key='page').set_value('Scanner').run()
                app.text_input(key='scanner_search').set_value('PEAR').run()
                self.assertFalse(app.exception)
                self.assertEqual(list(app.dataframe[0].value['Product']), ['PEAR'])
                app.number_input(key='scan_volume').set_value(100000).run()
                self.assertTrue(any('No products match' in w.value for w in app.info))
                app.radio(key='page').set_value('Collector').run()
                self.assertFalse(app.exception)
                self.assertTrue(any('Unique snapshots' == m.label for m in app.metric))
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
