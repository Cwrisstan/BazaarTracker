"""Plotly figures independent of Streamlit; extension points for future overlays."""
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from src.dashboard import analytics, data

COLORS = ['#38bdf8', '#fbbf24']


def style(fig, title, height=360):
    fig.update_layout(title=title, height=height, margin=dict(l=12, r=16, t=55, b=20),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      font=dict(color='#dbe5ef', size=12), hovermode='x unified',
                      legend=dict(orientation='h', y=1.12, x=0),
                      colorway=COLORS)
    fig.update_xaxes(gridcolor='rgba(148,163,184,0.12)', zeroline=False)
    fig.update_yaxes(gridcolor='rgba(148,163,184,0.12)', zeroline=False)
    return fig


def history_values(history, snapshots, getter):
    """Insert null separators across collection/source gaps and absent items."""
    segments = data.chart_records(history, snapshots)[::2]
    x, y, previous = [], [], None
    for row, segment in zip(history, segments):
        if previous is not None and segment['segment'] != previous:
            x.append(None)
            y.append(None)
        x.append(data.utc(row['source_updated_ms']))
        y.append(getter(row))
        previous = segment['segment']
    return x, y


def price_history(history, snapshots, item, series, mode):
    separate = mode == 'Separate scales' and len(series) > 1
    fig = make_subplots(rows=len(series) if separate else 1, cols=1,
                        shared_xaxes=True, vertical_spacing=0.12)
    for index, field in enumerate(series):
        column = 'buy_price' if field == 'buyPrice' else 'sell_price'
        base = history[0].get(column) if history else None
        def value(row):
            price = row.get(column)
            if not analytics.valid(price, True):
                return None
            if mode == 'Indexed (first = 100)':
                return price / base * 100 if analytics.valid(base, True) else None
            return price
        x, y = history_values(history, snapshots, value)
        panel = index + 1 if separate else 1
        fig.add_trace(go.Scatter(x=x, y=y, name=field, mode='lines+markers',
                                marker=dict(size=4), line=dict(color=COLORS[0 if field == 'buyPrice' else 1]), connectgaps=False), row=panel, col=1)
        fig.update_yaxes(title_text=f'{field} (coins)' if separate else ('Index' if mode.startswith('Indexed') else 'Coins / unit'), row=panel, col=1)
    fig.update_xaxes(title_text='Source time (UTC)', row=len(series) if separate else 1, col=1)
    return style(fig, f'{item} · aggregate prices', 480 if separate else 360)


def history_chart(history, snapshots, kind):
    fig = go.Figure()
    fields = [('Spread %', lambda row: analytics.calculate_spread_pct(row.get('buy_price'), row.get('sell_price')))] if kind == 'spread' else [
        ('buyVolume', lambda row: row.get('buy_volume')), ('sellVolume', lambda row: row.get('sell_volume'))]
    for name, getter in fields:
        x, y = history_values(history, snapshots, getter)
        fig.add_trace(go.Scatter(x=x, y=y, name=name, mode='lines+markers', connectgaps=False, marker=dict(size=4)))
    fig.update_xaxes(title='Source time (UTC)')
    fig.update_yaxes(title='Spread / buyPrice (%)' if kind == 'spread' else 'Standing units')
    return style(fig, 'Aggregate spread' if kind == 'spread' else 'Standing order quantities', 300)


def order_book(levels, current):
    fig = go.Figure()
    depth = analytics.calculate_order_book_depth(levels)
    for index, side in enumerate(('buy_summary', 'sell_summary')):
        rows = [row for row in depth if row['api_side'] == side]
        if not rows:
            continue
        fig.add_trace(go.Scatter(x=[row['price_per_unit'] for row in rows],
                                y=[row['cumulative_quantity'] for row in rows],
                                customdata=[[row['amount'], row['orders']] for row in rows],
                                name=side, mode='lines+markers', line=dict(shape='hv', color=COLORS[index]),
                                fill='tozeroy', connectgaps=False,
                                hovertemplate='Price: %{x:,.3f}<br>Cumulative: %{y:,.0f}<br>Level units: %{customdata[0]:,}<br>Orders: %{customdata[1]:,}<extra>%{fullData.name}</extra>'))
    for index, field in enumerate(('buy_price', 'sell_price')):
        if analytics.valid(current.get(field), True):
            fig.add_vline(x=current[field], line_dash='dot', line_color=COLORS[index],
                          annotation_text='buyPrice aggregate' if index == 0 else 'sellPrice aggregate',
                          annotation_position='top right' if index == 0 else 'bottom left')
    style(fig, 'Stored order-book depth', 420)
    fig.update_layout(hovermode='closest')
    fig.update_xaxes(title='Price (coins / unit)')
    fig.update_yaxes(title='Cumulative stored quantity', rangemode='tozero')
    return fig


def activity(records):
    ranked = sorted((row for row in records if row['total_volume'] is not None),
                    key=lambda row: row['total_volume'], reverse=True)[:12][::-1]
    fig = go.Figure(go.Bar(x=[row['total_volume'] for row in ranked], y=[row['product_id'] for row in ranked],
                           orientation='h', marker_color=COLORS[0], hovertemplate='%{y}<br>Standing units: %{x:,}<extra></extra>'))
    fig.update_xaxes(title='Total standing units (both API sides)')
    return style(fig, 'Most active items · standing-volume proxy', 420)


def opportunity(records):
    eligible = [row for row in records if row['spread_pct'] is not None and row['total_volume'] is not None
                and row['total_volume'] > 0 and row['two_sided_volume'] is not None]
    # Bubble area scales with the smaller side's quantity. The minimum diameter
    # keeps zero-sided books visible; their exact zero remains in hover text.
    size = [row['two_sided_volume'] for row in eligible]
    fig = go.Figure(go.Scatter(x=[row['total_volume'] for row in eligible], y=[row['spread_pct'] for row in eligible],
        mode='markers', text=[row['product_id'] for row in eligible],
        customdata=[[row['buy_price'], row['sell_price'], row['spread'], row['buy_volume'], row['sell_volume'], row['two_sided_volume']] for row in eligible],
        marker=dict(size=size, sizemode='area', sizeref=2 * max(size, default=1) / 42**2 or 1, sizemin=4,
                    color=[row['spread_pct'] for row in eligible], colorscale='Teal', opacity=0.75, showscale=True,
                    colorbar=dict(title='Spread %')),
        hovertemplate='%{text}<br>buyPrice: %{customdata[0]:,.3f}<br>sellPrice: %{customdata[1]:,.3f}<br>Spread: %{customdata[2]:,.3f}<br>Spread: %{y:.2f}%<br>Total units: %{x:,}<br>buyVolume: %{customdata[3]:,}<br>sellVolume: %{customdata[4]:,}<br>Smaller side: %{customdata[5]:,}<extra></extra>'))
    style(fig, 'Market opportunity map', 480)
    fig.update_layout(hovermode='closest')
    fig.update_xaxes(type='log', title='Total standing units (log scale)')
    fig.update_yaxes(title='Aggregate spread / buyPrice (%)')
    return fig, len(eligible)
