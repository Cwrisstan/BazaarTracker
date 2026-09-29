"""Versioned API-side primitives; no bid/ask reinterpretation or fill assumptions."""
from decimal import Decimal, localcontext
import math

FEATURE_VERSION = 1
SIDES = ('buy_summary', 'sell_summary')
SIDE_FIELDS = ('best_price', 'level_count', 'summary_depth', 'depth_1', 'depth_5',
               'depth_10', 'notional_5', 'notional_10', 'flags')
EMPTY, AT_API_LIMIT, ZERO_PRICE = 1, 2, 4


def side_primitives(book, side):
    if side not in SIDES or not isinstance(book, list):
        raise ValueError('expected an API summary array and known side')
    for entry in book:
        if not isinstance(entry, dict):
            raise ValueError('malformed book entry')
        price = entry.get('pricePerUnit')
        if (isinstance(price, bool) or not isinstance(price, (int, float))
                or not math.isfinite(price) or price < 0):
            raise ValueError('invalid book price')
        for field in ('amount', 'orders'):
            value = entry.get(field)
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ValueError('invalid book ' + field)
    # Stable sorting preserves API order among equal prices; archive keeps original order.
    ordered = sorted(book, key=lambda row: row['pricePerUnit'], reverse=side == 'sell_summary')
    result = {'best_price': ordered[0]['pricePerUnit'] if ordered else None,
              'level_count': len(book), 'summary_depth': sum(r['amount'] for r in book),
              'flags': (EMPTY if not book else 0) | (AT_API_LIMIT if len(book) >= 30 else 0)
                       | (ZERO_PRICE if any(r['pricePerUnit'] == 0 for r in book) else 0)}
    for k in (1, 5, 10):
        result[f'depth_{k}'] = sum(r['amount'] for r in ordered[:k])
    # Decimal text avoids overflow/rounding in price*large-quantity accumulation.
    # It reflects the decimal representation of the validated API numbers.
    with localcontext() as context:
        context.prec = 400
        for k in (5, 10):
            result[f'notional_{k}'] = str(sum((Decimal(str(r['pricePerUnit'])) * r['amount']
                                             for r in ordered[:k]), Decimal(0)))
    return result


def primitives(product):
    return {side: side_primitives(product.get(side), side) for side in SIDES}


def imbalance(buy_depth, sell_depth):
    if buy_depth is None or sell_depth is None:
        return None
    if buy_depth < 0 or sell_depth < 0:
        raise ValueError('depth must be nonnegative')
    total = buy_depth + sell_depth
    return (buy_depth - sell_depth) / total if total else None


def vwap(side, k=5):
    if k not in (5, 10):
        raise ValueError('only depth 5 and 10 notionals are retained')
    depth = side[f'depth_{k}']
    return float(Decimal(side[f'notional_{k}']) / depth) if depth else None
