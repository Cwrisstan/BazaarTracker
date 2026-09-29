import time
from statistics import median

import streamlit as st
from src.dashboard import analytics, charts, data

WINDOWS = {'1h': 3600000, '6h': 21600000, '24h': 86400000, '7d': 604800000, 'All': None}
COLUMNS = {'product_id': 'Product', 'buy_price': 'Buy Price', 'sell_price': 'Sell Price',
           'spread': 'Spread', 'spread_pct': 'Spread %', 'buy_volume': 'Buy Volume',
           'sell_volume': 'Sell Volume', 'total_volume': 'Total Volume',
           'two_sided_volume': 'Smaller-side Volume', 'total_orders': 'Orders',
           'price_change_pct': 'Buy Price Change %'}


@st.cache_data(max_entries=64, show_spinner=False)
def cached(kind, path, revision, *args):
    return getattr(data, kind)(path, *args)


def fmt(value, digits=2):
    return '—' if value is None else f'{value:,.{digits}f}'


def compact(value):
    if value is None:
        return '—'
    for scale, suffix in ((1e12, 'T'), (1e9, 'B'), (1e6, 'M'), (1e3, 'K')):
        if abs(value) >= scale:
            return f'{value / scale:,.2f}{suffix}'
    return fmt(value)


def metrics(entries):
    for column, (label, value, help_text) in zip(st.columns(len(entries)), entries):
        column.metric(label, value, help=help_text)


def plot(fig, key):
    st.plotly_chart(fig, width='stretch', key=key, config={'displaylogo': False})


def table(records, columns=None):
    chosen = columns or list(COLUMNS)
    st.dataframe([{COLUMNS[key]: row.get(key) for key in chosen} for row in records],
                 hide_index=True, width='stretch',
                 column_config={COLUMNS[key]: st.column_config.NumberColumn(format='%.2f')
                                for key in ('buy_price', 'sell_price', 'spread', 'spread_pct', 'price_change_pct') if key in chosen})


def stale(stamp, label='Latest snapshot'):
    age = data.age_seconds(stamp, int(time.time() * 1000))
    if age > data.STALE_SECONDS:
        st.warning(f'{label} is stale · {age / 60:,.1f} minutes old. Displaying stored data.')
    elif age < 0:
        st.warning(f'{label} is future-dated relative to this machine’s clock.')
    return age


def semantics():
    with st.expander('Metric definitions & data limits'):
        st.markdown('**Prices:** API `buyPrice` / `sellPrice` weighted aggregates, not executable quotes. '
                    '**Spread:** buyPrice − sellPrice. **Spread %:** 100 × spread / buyPrice; '
                    'undefined if either price is missing or nonpositive. Negative spreads remain visible. '
                    '**Volume:** standing order quantities, not completed trades. **Orders:** active order counts. '
                    'No fees, slippage, fill probability, or guaranteed profit are implied. '
                    '[Hypixel API definitions](https://api.hypixel.net/#tag/SkyBlock/paths/~1v2~1skyblock~1bazaar/get).')


def window(db, revision, end, health, key):
    label = st.segmented_control('History window', list(WINDOWS), default='24h', key=key)
    duration = WINDOWS[label or '24h']
    start = health['summary']['first_source'] if duration is None else end - duration
    snapshots, limited = cached('timeline', db, revision, start, end)
    if limited:
        st.warning('Coverage limited to the newest 3,000 snapshots. Older observations are omitted.')
    return start, snapshots


def coverage(snapshots):
    if snapshots:
        st.caption(f"Observed: {data.utc(snapshots[0]['source_updated_ms'])} → {data.utc(snapshots[-1]['source_updated_ms'])}. "
                   'Endpoints do not establish continuous coverage.')
    else:
        st.info('No snapshots in this time window. Try a longer window or All.')


def explorer(db, raw, revision, end, health):
    ids, limited = cached('items', db, revision)
    if limited:
        st.warning('Item selector limited to 5,000 stored IDs.')
    if not ids:
        st.info('No stored products available.')
        return
    item = st.selectbox('Product search', ids, key='product')
    current, book, limited = cached('current_item', db, revision, item, end)
    if current is None:
        st.info('No item snapshot at or before the refresh time.')
        return
    values = analytics.market_metrics([current])[0]
    metrics([('Buy Price', compact(current['buy_price']), f"API buyPrice · {fmt(current['buy_price'])} coins per unit"),
             ('Sell Price', compact(current['sell_price']), f"API sellPrice · {fmt(current['sell_price'])} coins per unit"),
             ('Spread', compact(values['spread']), f"buyPrice − sellPrice · {fmt(values['spread'])} coins per unit"),
             ('Spread %', fmt(values['spread_pct']) + '%' if values['spread_pct'] is not None else '—', 'Spread divided by buyPrice'),
             ('Volume', compact(values['total_volume']), f"Standing buyVolume + sellVolume = {fmt(values['total_volume'], 0)}; not traded volume"),
             ('Orders', compact(values['total_orders']), f"Active buyOrders + sellOrders = {fmt(values['total_orders'], 0)}")])
    if current['source_updated_ms'] != health['latest']['source_updated_ms']:
        stale(current['source_updated_ms'], 'Selected item / book')
    st.caption(f"Item snapshot: {data.utc(current['source_updated_ms'])} · independent of the history window.")
    if current['source_updated_ms'] < health['latest']['source_updated_ms']:
        st.warning('This item is absent from the latest market snapshot. Cards and book show its last stored observation.')
    st.subheader('Price history')
    start, snapshots = window(db, revision, end, health, 'item_window')
    history, clipped = cached('history', db, revision, item, start, end)
    if clipped:
        st.warning('Item history limited to the newest 3,000 observations; no downsampling.')
    coverage(history)
    controls = st.columns([1, 2])
    mode = controls[0].selectbox('Price display', ['Separate scales', 'Shared scale', 'Indexed (first = 100)'], key='price_mode')
    series = controls[1].multiselect('Price series', ['buyPrice', 'sellPrice'], default=['buyPrice', 'sellPrice'], key='price_series')
    if history and series:
        plot(charts.price_history(history, snapshots, item, series, mode), f'prices_{item}')
        st.caption('Independent y scales in separate mode; aligned time axes. Click a legend to toggle a series. '
                   'Indexed mode uses each series’ first displayed price. Lines break across gaps or missing observations; zero prices are not plotted.')
    elif not series:
        st.info('Select a price series to plot.')
    else:
        st.info('Not enough historical data yet.')
    if len(history) < 2:
        st.info('Not enough historical data yet to show a trend.')
    if history:
        left, right = st.columns(2)
        with left:
            st.subheader('Spread history')
            plot(charts.history_chart(history, snapshots, 'spread'), f'spread_{item}')
        with right:
            st.subheader('Volume / liquidity')
            plot(charts.history_chart(history, snapshots, 'volume'), f'volume_{item}')
        st.caption('Volume histories use stored standing quantities. Snapshot changes are not completed trades.')
    st.subheader('Order-book depth')
    if limited:
        st.warning('Book display limited to 1,000 levels in total. Cumulative depth is partial.')
    if not current.get('book_available', True):
        st.warning('Historical book unavailable: this product was outside the recorded research universe and temporary SQL depth has expired. Compact features cannot reconstruct full depth.')
    elif book:
        plot(charts.order_book(book, current), f'depth_{item}')
    else:
        st.info('Empty stored book; no levels fabricated.')
    st.caption('Cumulative quantity from the nearest price outward on each API side. Only stored summary levels are shown, '
               'not the entire market book. Dotted lines mark aggregate prices; they are not guaranteed fills.')
    with st.expander('View raw order book'):
        st.caption(f"Collected: {current['collected_at_utc']}")
        st.dataframe([{'API prefix': side, 'Standing units': current[f'{side}_volume'], 'Active orders': current[f'{side}_orders']}
                      for side in ('buy', 'sell')], hide_index=True)
        for side, column in zip(('buy_summary', 'sell_summary'), st.columns(2)):
            with column:
                st.markdown(f'**{side}**')
                levels = [{k: v for k, v in row.items() if k != 'api_side'} for row in book if row['api_side'] == side]
                if levels:
                    st.dataframe(levels, hide_index=True)
                elif current.get('book_available', True):
                    st.info('Empty stored book; no levels fabricated.')
                else:
                    st.info('Historical depth unavailable; not an empty book.')
    semantics()


def market_data(db, revision, end, health):
    snapshot, current, limited = cached('market_snapshot', db, revision, end)
    if snapshot is None:
        st.info('No snapshot at or before the refresh time.')
        return None, [], False
    if limited:
        st.warning('Market limited to the first 5,000 product IDs. Rankings and aggregate cards cover only this subset.')
    if snapshot['source_updated_ms'] != health['latest']['source_updated_ms']:
        stale(snapshot['source_updated_ms'], 'Displayed market snapshot')
    st.caption(f"Market snapshot: {data.utc(snapshot['source_updated_ms'])} · {len(current):,} products displayed.")
    period = st.selectbox('Change period', ['1h', '6h', '24h', '7d', 'Recorded range'], key='change_period')
    target = health['summary']['first_source'] if period == 'Recorded range' else snapshot['source_updated_ms'] - WINDOWS[period]
    baseline, previous, baseline_limited = cached('market_snapshot', db, revision, target, data.GAP_SECONDS * 1000)
    if baseline_limited:
        st.warning('Comparison baseline limited to 5,000 product IDs. Products absent from that subset have no change metric.')
    if baseline is None or baseline['source_updated_ms'] >= snapshot['source_updated_ms']:
        previous = []
        st.info('Not enough historical data yet for this change period.')
    else:
        st.caption(f"Change endpoints: {data.utc(baseline['source_updated_ms'])} → {data.utc(snapshot['source_updated_ms'])}. "
                   'buyPrice change only; endpoint comparison does not imply continuous coverage. '
                   'Fixed periods require a baseline within 180 seconds before the target; periods end at the stored market snapshot.')
    return snapshot, analytics.market_metrics(current, previous), limited


def overview(db, raw, revision, end, health):
    cards = st.container()
    snapshot, records, limited = market_data(db, revision, end, health)
    if not records:
        st.info('No products in this market snapshot.')
        return
    spreads = [row['spread_pct'] for row in records if row['spread_pct'] is not None]
    volumes = [row['total_volume'] for row in records]
    total = sum(volumes) if all(value is not None for value in volumes) else None
    with cards:
        metrics([('Tracked Items' if not limited else 'Displayed Items', fmt(len(records), 0), 'Products present in this market snapshot'),
             ('Snapshot Count', fmt(health['summary']['count'], 0), 'All stored snapshots'),
             ('Standing Units', compact(total), f"{fmt(total, 0)} units across displayed products; heterogeneous units, not coins or traded volume"),
             ('Median Spread %', fmt(median(spreads)) + '%' if spreads else '—', f'{len(spreads):,} products with valid positive prices'),
             ('Snapshot Age', f"{data.age_seconds(snapshot['source_updated_ms'], int(time.time()*1000))/60:,.1f}m", 'Age of displayed source snapshot')])
    minimum = st.number_input('Minimum standing units on each side', min_value=0, value=1000, step=100, key='overview_liquidity')
    ranked = analytics.filter_market(records, minimum_side_volume=minimum)
    st.caption(f'Liquidity filter: {len(ranked):,} of {len(records):,} products. Applies to all rankings and the map below.')
    left, right = st.columns(2)
    with left:
        st.subheader('Top movers')
        movers = sorted((row for row in ranked if row['price_change_pct'] is not None), key=lambda row: abs(row['price_change_pct']), reverse=True)[:10]
        if movers:
            table(movers, ['product_id', 'price_change_pct', 'buy_price', 'total_volume'])
            st.caption('Ranked by absolute buyPrice change; signed changes are shown.')
        else:
            st.info('Not enough historical data yet, or no products match the liquidity filter.')
    with right:
        st.subheader('Largest spreads')
        spread_rows = sorted((row for row in ranked if row['spread_pct'] is not None), key=lambda row: row['spread_pct'], reverse=True)[:10]
        if spread_rows:
            table(spread_rows, ['product_id', 'spread_pct', 'total_volume', 'two_sided_volume'])
        else:
            st.info('No products with valid prices match the liquidity filter.')
    st.subheader('Most active items')
    plot(charts.activity(ranked), 'activity')
    st.caption('Activity proxy: standing order quantities. This is not a ranking of completed trades.')
    st.subheader('Market opportunity map')
    fig, shown = charts.opportunity(ranked)
    plot(fig, 'opportunities')
    st.caption(f'{shown:,} items plotted. Nonpositive or missing total volume and invalid spreads are excluded. '
               'Bubble area represents smaller-side standing quantity; minimum marker size keeps zero-sided books visible. '
               'A market scanner, not a guaranteed-profit detector.')
    semantics()


def scanner(db, raw, revision, end, health):
    _, records, _ = market_data(db, revision, end, health)
    if not records:
        st.info('No products in this market snapshot.')
        return
    with st.expander('Scanner filters', expanded=True):
        search = st.text_input('Product contains', key='scanner_search')
        left, right = st.columns(2)
        volume = left.number_input('Minimum total standing units', min_value=0, value=0, key='scan_volume')
        liquidity = right.number_input('Minimum standing units on each side', min_value=0, value=0, key='scan_liquidity')
        bounds = {}
        for name, prefix in [('Spread %', 'spread'), ('Buy Price', 'price'), ('Buy Price Change %', 'change')]:
            if st.checkbox(f'Filter {name}', key=f'filter_{prefix}'):
                a, b = st.columns(2)
                bounds[f'{prefix}_min'] = a.number_input(f'Minimum {name}', value=0.0, key=f'{prefix}_min')
                bounds[f'{prefix}_max'] = b.number_input(f'Maximum {name}', value=100.0 if prefix != 'price' else 1000000.0, key=f'{prefix}_max')
                if bounds[f'{prefix}_min'] > bounds[f'{prefix}_max']:
                    st.warning(f'{name}: minimum exceeds maximum. No products will match.')
        st.caption('Active metric filters exclude unknown values. With filters off, unknowns remain blank.')
    filtered = analytics.filter_market(records, search, volume, liquidity, **bounds)
    left, right = st.columns([3, 1])
    sort_by = left.selectbox('Sort by', list(COLUMNS), index=4, format_func=COLUMNS.get, key='scanner_sort')
    descending = right.checkbox('Descending', value=True)
    known = sorted((row for row in filtered if row.get(sort_by) is not None), key=lambda row: row[sort_by], reverse=descending)
    ordered = known + [row for row in filtered if row.get(sort_by) is None]
    st.caption(f'{len(ordered):,} / {len(records):,} products · click column headings to sort the table.')
    if ordered:
        table(ordered)
    else:
        st.info('No products match these filters.')
    semantics()


def collector(db, raw, revision, end, health):
    latest, summary = health['latest'], health['summary']
    sizes = cached('storage_sizes', db, revision, raw)
    metrics([('Database', f"{sizes['database']/1024**2:,.2f} MiB", 'Logical file size'),
             ('SQLite sidecars', f"{sizes['sidecars']/1024**2:,.2f} MiB", 'Journal / WAL / SHM'),
             ('Raw snapshots', f"{sizes['raw']/1024**2:,.2f} MiB", 'JSON / gzip / staging files')])
    if sizes['partial']:
        st.warning('Raw size is a partial lower bound: scan limit or filesystem access error.')
    st.subheader('Collector health')
    metrics([('Unique snapshots', fmt(summary['count'], 0), None),
             ('Latest products', fmt(latest['product_count'], 0), None),
             ('Source age', f"{data.age_seconds(latest['source_updated_ms'], int(time.time()*1000)):,.0f}s", 'Stale threshold: 180 seconds')])
    st.caption('Staleness does not establish whether the collector process is running.')
    st.write(f"Database: {db}")
    st.write(f"Latest source timestamp (UTC): {data.utc(latest['source_updated_ms'])}")
    st.write(f"Latest source collection timestamp (UTC): {latest['collected_at_utc']}")
    st.write(f"Most recent collection timestamp (UTC): {summary['last_collection']}")
    st.write(f"Recorded source range: {data.utc(summary['first_source'])} → {data.utc(summary['last_source'])}")
    st.write(f"Recorded collection range: {summary['first_collection']} → {summary['last_collection']}")
    st.subheader('Coverage & gaps')
    _, snapshots = window(db, revision, end, health, 'collector_window')
    coverage(snapshots)
    st.caption('Gaps exceed 180 seconds in source or collection time. Endpoints do not establish continuous coverage.')
    gaps = data.gaps(snapshots)
    if gaps:
        st.dataframe(gaps, hide_index=True, width='stretch')
    else:
        st.info('No substantial internal gaps found in the displayed observations.' if len(snapshots) > 1 else 'Not enough observations to assess internal gaps.')
