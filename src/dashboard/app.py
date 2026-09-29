"""Launch with streamlit run src/dashboard/app.py; never starts ingestion."""
import os
from pathlib import Path
import sys
import time

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.dashboard import data, pages

st.set_page_config(page_title='BazaarTracker | Market terminal', page_icon='📈', layout='wide')
st.markdown('''<style>
.block-container {padding-top: 2rem; padding-bottom: 2rem; max-width: 1600px;}
[data-testid="stMetric"] {background: rgba(100,116,139,.10); border: 1px solid rgba(148,163,184,.18); border-radius: 8px; padding: 12px;}
[data-testid="stMetricValue"] {font-size: clamp(1rem, 1.8vw, 1.65rem);}
h1 {letter-spacing: -.04em;} h2, h3 {letter-spacing: -.02em;}
</style>''', unsafe_allow_html=True)
root = Path(__file__).resolve().parents[2]
db = os.environ.get('BAZAAR_DASHBOARD_DB', str(root / 'data/bazaar.sqlite3'))
raw = os.environ.get('BAZAAR_DASHBOARD_RAW', str(Path(db).parent / 'raw'))

if 'revision' not in st.session_state:
    st.session_state.revision = time.time_ns()
    st.session_state.as_of = int(time.time() * 1000)
with st.sidebar:
    st.title('BazaarTracker')
    st.caption('SKYBLOCK · MARKET TERMINAL')
    page = st.radio('Workspace', ['Market Overview', 'Item Explorer', 'Scanner', 'Collector'], key='page')
    if st.button('Refresh', width='stretch'):
        st.session_state.revision = time.time_ns()
        st.session_state.as_of = int(time.time() * 1000)
    st.caption('Stored snapshots · read-only\n\nManual refresh · all times UTC')
    st.caption(f"View refreshed\n\n{data.utc(st.session_state.as_of)}")

revision, end = st.session_state.revision, st.session_state.as_of
st.title(page)
try:
    health = pages.cached('health', db, revision)
    if health['latest'] is None:
        st.info('No snapshots recorded yet. Use Refresh after the collector writes data.')
        st.stop()
    pages.stale(health['latest']['source_updated_ms'])
    render = {'Market Overview': pages.overview, 'Item Explorer': pages.explorer,
              'Scanner': pages.scanner, 'Collector': pages.collector}[page]
    render(db, raw, revision, end, health)
except (data.Unavailable, OSError, ValueError) as exc:
    st.warning(str(exc))
    st.info('No data was changed. Try Refresh once the database is available.')
