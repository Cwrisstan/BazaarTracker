"""Profiler safety and delta accounting against disposable fixture databases."""
from copy import deepcopy
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest

from tools.profile_storage import compare, inventory, main, measure, projections


class StorageProfilerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'data' / 'bazaar.sqlite3'
        self.db.parent.mkdir()
        self.raw = self.db.parent / 'raw'
        self.raw.mkdir()
        conn = sqlite3.connect(self.db)
        conn.executescript('''
            CREATE TABLE snapshots(source_updated_ms INTEGER PRIMARY KEY, collected_at_utc TEXT, product_count INTEGER);
            CREATE TABLE quick_status(source_updated_ms INTEGER, product_id TEXT);
            CREATE TABLE order_book_levels(source_updated_ms INTEGER, product_id TEXT);
            INSERT INTO snapshots VALUES(1,'2026-01-01T00:00:00+00:00',1);
            INSERT INTO quick_status VALUES(1,'TEST');
            INSERT INTO order_book_levels VALUES(1,'TEST');
        ''')
        conn.close()
        (self.raw / 'bazaar-v1-1.json.gz').write_bytes(b'raw fixture')

    def test_measure_does_not_change_data_and_counts_legacy_separately(self):
        (self.raw / 'legacy.db').write_bytes(b'legacy')
        (self.raw / 'link').symlink_to(self.db)
        before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.db.parent.rglob('*') if p.is_file()}
        report = measure(self.db, self.raw)
        self.assertEqual(report['counts'], dict(snapshots=1, quick_status=1, order_book_levels=1))
        self.assertEqual(report['product_count_distinct'], 1)
        self.assertEqual(report['raw']['file_count'], 2)
        self.assertEqual(report['raw']['owned_snapshot_count'], 1)
        self.assertEqual(report['raw']['bytes'], 17)
        self.assertEqual(report['raw']['skipped_symlinks'], ['link'])
        self.assertEqual(before, {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.db.parent.rglob('*') if p.is_file()})

    def test_missing_database_is_not_created(self):
        missing = self.root / 'missing.sqlite3'
        with self.assertRaises(FileNotFoundError):
            measure(missing, self.raw)
        self.assertFalse(missing.exists())

    def test_retention_net_delta_is_not_raw_production(self):
        before = measure(self.db, self.raw)
        after = deepcopy(before)
        before['measured_at_utc'] = '2026-01-01T00:00:00+00:00'
        after['measured_at_utc'] = '2026-01-01T01:00:00+00:00'
        after['counts'] = {k: v + 1 for k, v in before['counts'].items()}
        after['database_bytes'] += 4096
        after['raw']['files'] = {'bazaar-v1-2.json.gz': dict(bytes=7, owned_snapshot=True)}
        after['raw']['bytes'] = 7
        diff = compare(before, after)
        self.assertEqual(diff['raw_net_delta_bytes'], -4)
        self.assertEqual(diff['mean_new_retained_raw_bytes'], 7)
        self.assertEqual(diff['database_bytes_per_new_snapshot'], 4096)
        self.assertEqual(diff['raw_removed_files'], 1)
        self.assertEqual(diff['raw_buffer_48h_bytes_estimate'], 7 * 2880)
        after['counts'] = before['counts']
        self.assertIsNone(compare(before, after)['permanent_database_projection'])
        after['database_identity'] = [0, 0]
        with self.assertRaises(ValueError):
            compare(before, after)

    def test_output_cannot_overwrite_data_or_previous_measurement(self):
        with self.assertRaises(SystemExit):
            main(['--database', str(self.db), '--output', str(self.raw / 'new.json')])
        destination = self.root / 'measurement.json'
        args = ['--database', str(self.db), '--output', str(destination)]
        self.assertEqual(main(args), 0)
        original = destination.read_bytes()
        with self.assertRaises(SystemExit):
            main(args)
        self.assertEqual(destination.read_bytes(), original)

    def test_projection_units_and_missing_raw(self):
        self.assertEqual(projections(1000, 60)['week_bytes'], 1000 * 1440 * 7)
        self.assertEqual(inventory(self.root / 'absent')['file_count'], 0)
