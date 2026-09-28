"""Launch with streamlit run src/dashboard/app.py; never starts ingestion."""
import os
from pathlib import Path
import sys
import time

import altair as alt
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.dashboard import data

st.set_page_config(page_title='BazaarTracker', layout='wide')
st.title('BazaarTracker')
st.caption('Stored snapshots only • Read-only • Manual refresh • All times UTC')
root = Path(__file__).resolve().parents[2]
db = os.environ.get('BAZAAR_DASHBOARD_DB', str(root / 'data/bazaar.sqlite3'))
raw = os.environ.get('BAZAAR_DASHBOARD_RAW', str(Path(db).parent / 'raw'))

# Bounded in-memory caching, keyed to each manual refresh; no DB/file scanning on reruns.
@st.cache_data(max_entries=64, show_spinner=False)
def cached(kind, path, revision, *args):
    return getattr(data, kind)(path, *args)

if 'revision' not in st.session_state:
    st.session_state.revision = time.time_ns()
    st.session_state.as_of = int(time.time() * 1000)
if st.button('Refresh'):
    st.session_state.revision = time.time_ns()
    st.session_state.as_of = int(time.time() * 1000)
revision, end = st.session_state.revision, st.session_state.as_of
st.caption(f'View refreshed at {data.utc(end)}. Database: {db}')
hours = st.selectbox('Time window (ending at refresh time)', [1, 6, 24], format_func=lambda h: f'{h} hour' + ('s' if h != 1 else ''))
start = end - hours * 3600 * 1000

sizes = cached('storage_sizes', db, revision, raw)
a, b, c = st.columns(3)
a.metric('Database', f'{sizes["database"] / 1024**2:.2f} MiB')
b.metric('SQLite sidecars', f'{sizes["sidecars"] / 1024**2:.2f} MiB')
c.metric('Raw JSON / gzip / staging files', f'{sizes["raw"] / 1024**2:.2f} MiB')
if sizes['partial']:
    st.warning('Raw size is a partial lower bound: scan limit or filesystem access error.')

try:
    health = cached('health', db, revision)
    latest = health['latest']
    if latest is None:
        st.info('No snapshots recorded yet. Use Refresh after the collector writes data.')
        st.stop()
    st.subheader('Collector health')
    a, b, c = st.columns(3)
    a.metric('Unique snapshots', health['summary']['count'])
    b.metric('Products in latest source snapshot', latest['product_count'])
    age = data.age_seconds(latest['source_updated_ms'], int(time.time() * 1000))
    c.metric('Latest source snapshot age', f'{age:.0f} seconds')
    st.write(f"Latest source timestamp (UTC): {data.utc(latest['source_updated_ms'])}")
    st.write(f"Collection timestamp for that snapshot (UTC): {latest['collected_at_utc']}")
    st.write(f"Most recent collection timestamp (UTC): {health['summary']['last_collection']}")
    st.caption(f"Recorded source range (UTC): {data.utc(health['summary']['first_source'])} → {data.utc(health['summary']['last_source'])}")
    st.caption(f"Recorded collection range (UTC): {health['summary']['first_collection']} → {health['summary']['last_collection']}")
    if age > data.STALE_SECONDS:
        st.warning('Latest source snapshot is stale (older than 180 seconds). This does not establish whether the collector process is running.')
    elif age < 0:
        st.warning('Source timestamp is in the future relative to this machine’s clock.')
    snapshots, truncated = cached('timeline', db, revision, start, end)
    st.subheader('Coverage in selected time window')
    st.caption('Gaps exceed 180 seconds in source or collection time. Range endpoints do not imply continuous coverage.')
    if truncated:
        st.warning('Coverage is limited to the newest 3,000 snapshots in this window; older coverage is omitted.')
    gaps = data.gaps(snapshots)
    if gaps:
        st.dataframe(gaps, hide_index=True)
    else:
        st.info('No substantial internal gaps found in the displayed observations.' if len(snapshots) > 1 else 'Not enough observations to assess internal gaps.')
    if snapshots:
        st.caption(f"Observed window: {data.utc(snapshots[0]['source_updated_ms'])} → {data.utc(snapshots[-1]['source_updated_ms'])}. No coverage is asserted before or after these observations.")
    else:
        st.info('No snapshots in this time window.')
    ids, limited = cached('items', db, revision)
    if limited:
        st.warning('Item selector limited to 5,000 stored IDs.')
    if not ids:
        st.info('No stored products available.')
        st.stop()
    st.subheader('Item explorer')
    item = st.selectbox('Stored product ID', ids)
    history, limited = cached('history', db, revision, item, start, end)
    st.caption('API quick-status aggregate prices: buyPrice and sellPrice. Not best executable quotes. No forward filling; lines break at gaps or missing item observations.')
    if limited:
        st.warning('Showing only the newest 3,000 item observations in this window; no downsampling is applied.')
    points = data.chart_records(history, snapshots)
    if points:
        chart = alt.Chart(alt.Data(values=points)).mark_line(point=True).encode(
            x=alt.X('source_utc:T', title='Source timestamp (UTC)', scale=alt.Scale(type='utc')),
            y=alt.Y('price:Q', title='API quick-status aggregate price', scale=alt.Scale(zero=False)),
            color='API field:N', detail='segment:N',
            tooltip=['source_utc:N', 'API field:N', 'price:Q'])
        st.altair_chart(chart, width='stretch')
    else:
        st.info('No observations for this item in the selected time window.')
    current, book, limited = cached('current_item', db, revision, item, end)
    if current is None:
        st.info('No item snapshot at or before the refresh time.')
        st.stop()
    st.subheader('Latest stored item / order books')
    st.caption('Latest available item snapshot at or before refresh time, independent of the chart window.')
    st.write(f"Source timestamp (UTC): {data.utc(current['source_updated_ms'])}")
    st.write(f"Collection timestamp (UTC): {current['collected_at_utc']}")
    if data.age_seconds(current['source_updated_ms'], int(time.time() * 1000)) > data.STALE_SECONDS:
        st.warning('This item’s book is stale (source older than 180 seconds).')
    st.dataframe([{'API side prefix': side, 'Stored volume': current[f'{side}_volume'],
                   'Stored order count': current[f'{side}_orders']} for side in ('buy', 'sell')], hide_index=True)
    if limited:
        st.warning('Book display limited to 1,000 levels in total.')
    for side, column in zip(('buy_summary', 'sell_summary'), st.columns(2)):
        with column:
            st.markdown(f'**{side}**')
            levels = [{k: v for k, v in row.items() if k != 'api_side'} for row in book if row['api_side'] == side]
            if levels:
                st.dataframe(levels, hide_index=True)
            else:
                st.info('Empty stored book; no levels fabricated.')
    st.caption('API side names preserved. Snapshots are not a complete trade feed and do not guarantee fills.')
except (data.Unavailable, OSError, ValueError) as exc:
    st.warning(str(exc))
    st.info('No data was changed. Try Refresh once the database is available.')
