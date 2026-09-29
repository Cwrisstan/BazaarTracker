import math


def valid(value, positive=False):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and (value > 0 if positive else value >= 0))


def calculate_spread(buy_price, sell_price):
    if not (valid(buy_price, True) and valid(sell_price, True)):
        return None
    return buy_price - sell_price


def calculate_spread_pct(buy_price, sell_price):
    spread = calculate_spread(buy_price, sell_price)
    return None if spread is None else 100 * spread / buy_price


def calculate_price_change(previous, current):
    if not (valid(previous, True) and valid(current, True)):
        return None
    return 100 * (current / previous - 1)


def calculate_volume_metrics(row):
    def total(a, b):
        return row[a] + row[b] if valid(row.get(a)) and valid(row.get(b)) else None
    return {'total_volume': total('buy_volume', 'sell_volume'),
            'total_orders': total('buy_orders', 'sell_orders'),
            'two_sided_volume': min(row['buy_volume'], row['sell_volume'])
            if valid(row.get('buy_volume')) and valid(row.get('sell_volume')) else None}


def market_metrics(current, baseline=()):
    previous = {row['product_id']: row for row in baseline}
    result = []
    for row in current:
        old = previous.get(row['product_id'], {})
        result.append({**row, **calculate_volume_metrics(row),
                       'spread': calculate_spread(row.get('buy_price'), row.get('sell_price')),
                       'spread_pct': calculate_spread_pct(row.get('buy_price'), row.get('sell_price')),
                       'price_change_pct': calculate_price_change(old.get('buy_price'), row.get('buy_price'))})
    return result


def calculate_order_book_depth(levels):
    result = []
    for side in ('buy_summary', 'sell_summary'):
        selected = [row for row in levels if row['api_side'] == side]
        if any(not valid(row.get('price_per_unit')) for row in selected):
            continue
        selected = sorted(selected, key=lambda row: row['price_per_unit'], reverse=side == 'sell_summary')
        cumulative = 0
        for row in selected:
            cumulative = cumulative + row['amount'] if cumulative is not None and valid(row.get('amount')) else None
            result.append({**row, 'cumulative_quantity': cumulative})
    return result


def filter_market(records, search='', minimum_volume=0, minimum_side_volume=0,
                  spread_min=None, spread_max=None, price_min=None, price_max=None,
                  change_min=None, change_max=None):
    def within(value, low, high):
        if low is None and high is None:
            return True
        return value is not None and (low is None or value >= low) and (high is None or value <= high)
    return [row for row in records
            if search.upper() in row['product_id'].upper()
            and within(row['total_volume'], minimum_volume if minimum_volume > 0 else None, None)
            and within(row['two_sided_volume'], minimum_side_volume if minimum_side_volume > 0 else None, None)
            and within(row['spread_pct'], spread_min, spread_max)
            and within(row['buy_price'], price_min, price_max)
            and within(row['price_change_pct'], change_min, change_max)]
