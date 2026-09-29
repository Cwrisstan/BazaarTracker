"""Deterministic analytics and chart checks; never use the collector database."""
import importlib.util
import unittest
from src.dashboard import analytics as a, data


class AnalyticsTests(unittest.TestCase):
    def test_spreads_and_price_change(self):
        self.assertEqual(a.calculate_spread(100, 80), 20)
        self.assertEqual(a.calculate_spread_pct(100, 80), 20)
        self.assertEqual(a.calculate_spread_pct(80, 100), -25)
        self.assertAlmostEqual(a.calculate_price_change(80, 100), 25)
        for invalid in (None, 0, -1, float('nan'), float('inf')):
            self.assertIsNone(a.calculate_spread_pct(invalid, 80))
            self.assertIsNone(a.calculate_spread(100, invalid))
            self.assertIsNone(a.calculate_price_change(invalid, 100))

    def test_missing_volume_and_baseline(self):
        row = dict(product_id='TEST', buy_price=100, sell_price=80,
                   buy_volume=0, sell_volume=20, buy_orders=None, sell_orders=2)
        metric = a.market_metrics([row])[0]
        self.assertEqual(metric['total_volume'], 20)
        self.assertEqual(metric['two_sided_volume'], 0)
        self.assertIsNone(metric['total_orders'])
        self.assertIsNone(metric['price_change_pct'])
        self.assertEqual(len(a.filter_market([metric])), 1)
        self.assertEqual(a.filter_market([metric], minimum_side_volume=1), [])
        self.assertEqual(a.filter_market([metric], change_min=0), [])
        self.assertEqual(a.filter_market([metric], spread_max=10), [])
        self.assertEqual(a.filter_market([metric], search='other'), [])
        self.assertIsNone(a.calculate_volume_metrics({**row, 'buy_volume': None})['total_volume'])

    def test_depth_sorted_separately_and_no_mutation(self):
        levels = [dict(api_side=side, price_per_unit=p, amount=q, orders=1)
                  for side, p, q in [('buy_summary', 12, 4), ('sell_summary', 8, 7),
                                     ('buy_summary', 10, 2), ('sell_summary', 9, 3)]]
        before = [dict(row) for row in levels]
        depth = a.calculate_order_book_depth(levels)
        self.assertEqual([row['price_per_unit'] for row in depth], [10, 12, 9, 8])
        self.assertEqual([row['cumulative_quantity'] for row in depth], [2, 6, 3, 10])
        self.assertEqual(levels, before)
        levels[2]['amount'] = None
        self.assertEqual([row['cumulative_quantity'] for row in a.calculate_order_book_depth(levels)][:2], [None, None])
        self.assertEqual(a.calculate_order_book_depth([]), [])


@unittest.skipUnless(importlib.util.find_spec('plotly'), 'install dashboard dependencies')
class ChartTests(unittest.TestCase):
    def test_gap_and_missing_item_separators_all_histories(self):
        from src.dashboard import charts
        stamps = [1000000, 1060000, 1120000, 1500000]
        snapshots = [dict(source_updated_ms=t, collected_at_utc=data.utc(t)) for t in stamps]
        history = [{**row, 'buy_price': 10, 'sell_price': 8, 'buy_volume': 30, 'sell_volume': 40}
                   for row in (snapshots[0], snapshots[2], snapshots[3])]
        x, y = charts.history_values(history, snapshots, lambda r: r['buy_price'])
        self.assertEqual(y, [10, None, 10, None, 10])
        for kind in ['spread', 'volume']:
            chart = charts.history_chart(history, snapshots, kind)
            self.assertEqual(list(chart.data[0].x), x)
            self.assertFalse(chart.data[0].connectgaps)
        indexed = charts.price_history(history, snapshots, 'TEST', ['buyPrice'], 'Indexed (first = 100)')
        self.assertEqual(list(indexed.data[0].y), [100, None, 100, None, 100])
        history[0]['buy_price'] = 0
        indexed = charts.price_history(history, snapshots, 'TEST', ['buyPrice'], 'Indexed (first = 100)')
        self.assertTrue(all(v is None for v in indexed.data[0].y))

    def test_order_book_aggregate_markers_and_empty_market(self):
        from src.dashboard import charts
        chart = charts.order_book([dict(api_side='buy_summary', price_per_unit=10, amount=5, orders=2)],
                                  dict(buy_price=11, sell_price=8))
        self.assertEqual(list(chart.data[0].y), [5])
        self.assertEqual([shape.x0 for shape in chart.layout.shapes], [11, 8])
        fig, count = charts.opportunity([])
        self.assertEqual(count, 0)
        self.assertEqual(len(fig.data[0].x), 0)
