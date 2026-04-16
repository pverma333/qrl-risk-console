import streamlit as st
from config import fetch_latest_trade_date, fetch_market_summary
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(
    page_title="QRL Console",
    page_icon="📊",
    layout="wide",
)

# Rename sidebar label from "app" to "Home"
st.markdown(
    """
    <style>
    [data-testid="stSidebarNav"] li:first-child a span {
        display: none;
    }
    [data-testid="stSidebarNav"] li:first-child a::before {
        content: "Home";
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Header ---
st.title("QRL Console")
st.markdown("#### Institutional Risk Engine for Indian Index Derivatives")
st.markdown("*EOD Analytics • Black-Scholes Pricing • Historical VaR Simulation*")
st.divider()

# --- Feature Grid ---
col1, col2, col3, col4 = st.columns(4)
container_height = 200

with col1:
    with st.container(height=container_height):
        st.subheader("Market Explorer")
        st.write("Analyze Index chains. View settlement prices, IV, and Greeks. Includes IV smile and delta charts.")
    st.page_link("pages/1_Market_Explorer.py", label="→ Open Market Explorer", use_container_width=False)

with col2:
    with st.container(height=container_height):
        st.subheader("Portfolio Risk")
        st.write("Upload positions for EOD pricing and MTM. Stress-test against spot, vol, and rate shocks.")
    st.page_link("pages/2_Portfolio_Risk.py", label="→ Open Portfolio Risk", use_container_width=False)

with col3:
    with st.container(height=container_height):
        st.subheader("Scenario Lab")
        st.write("Single-contract stress testing. Apply custom spot, vol, and rate shocks to calculate shocked pricing.")
    st.page_link("pages/3_Scenario_Lab.py", label="→ Open Scenario Lab", use_container_width=False)

with col4:
    with st.container(height=container_height):
        st.subheader("VaR / CVaR")
        st.write("252-day historical simulation. Compute VaR/CVaR at 95%/99% confidence with PnL distributions.")
    st.page_link("pages/4_VaR_CVaR.py", label="→ Open VaR / CVaR", use_container_width=False)

# Gap between feature grid and Market Glimpse
st.markdown("<br>", unsafe_allow_html=True)
st.divider()

# --- Market Glimpse ---
st.markdown("## Market Glimpse")

latest_date = fetch_latest_trade_date()

st.caption(f"Data as of {latest_date.strftime('%d %b %Y')} (latest available EOD)")

if latest_date:
    market_data = fetch_market_summary(latest_date)

    if market_data:
        # Build chart pivot once
        chart_df = pd.DataFrame(market_data['chart_data'])
        chart_pivot = pd.DataFrame()
        if not chart_df.empty:
            chart_df['date'] = pd.to_datetime(chart_df['date'])
            chart_pivot = chart_df.pivot(index='date', columns='symbol', values='close')

        def make_sparkline(symbol: str, series: pd.Series, color: str) -> go.Figure:
            y_min = series.min()
            y_max = series.max()
            y_pad = (y_max - y_min) * 0.10
            if y_pad == 0:
                y_pad = y_min * 0.01

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=series.index,
                y=series.values,
                mode='lines',
                line=dict(color=color, width=2),
                fill='tozeroy',
                fillcolor=color.replace(')', ', 0.08)').replace('rgb', 'rgba'),
                hovertemplate='%{x|%d %b %Y}<br>₹%{y:,.2f}<extra></extra>',
            ))

            fig.update_layout(
                height=130,
                margin=dict(l=0, r=0, t=0, b=0),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(
                    showgrid=False,
                    zeroline=False,
                    tickformat='%b',
                    tickmode='auto',
                    nticks=6,
                    showline=False,
                    tickfont=dict(size=10, color='#888888'),
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(255,255,255,0.05)',
                    zeroline=False,
                    range=[y_min - y_pad, y_max + y_pad],
                    tickformat=',.0f',
                    showline=False,
                    tickfont=dict(size=10, color='#888888'),
                    nticks=4,
                ),
                showlegend=False,
                hovermode='x unified',
            )

            return fig

        index_colors = {
            'NIFTY':      'rgb(99, 179, 237)',
            'BANKNIFTY':  'rgb(154, 117, 234)',
            'FINNIFTY':   'rgb(72, 199, 142)',
            'MIDCPNIFTY': 'rgb(246, 173, 85)',
        }

        # --- Rates and Volatility ---
        st.markdown("#### Rates and Volatility")
        rate_cols = st.columns(4)

        with rate_cols[0]:
            vix_val = market_data['vix']['value']
            st.metric(
                label="India VIX",
                value=f"{vix_val:.2f}" if vix_val is not None else "N/A"
            )

        with rate_cols[1]:
            y_3m = market_data['yields']['rate_3m']
            st.metric(
                label="G-Bond 3M",
                value=f"{y_3m:.2f}%" if y_3m is not None else "N/A"
            )

        with rate_cols[2]:
            y_6m = market_data['yields']['rate_6m']
            st.metric(
                label="G-Bond 6M",
                value=f"{y_6m:.2f}%" if y_6m is not None else "N/A"
            )

        with rate_cols[3]:
            y_1y = market_data['yields']['rate_1y']
            st.metric(
                label="G-Bond 1Y",
                value=f"{y_1y:.2f}%" if y_1y is not None else "N/A"
            )

        st.divider()

        # --- Per-index rows ---
        st.markdown("#### Index Performance")

        for idx in market_data['indices']:
            symbol = idx['symbol']
            color = index_colors.get(symbol, 'rgb(200, 200, 200)')

            metric_col, chart_col = st.columns([1, 3])

            with metric_col:
                st.markdown("<div style='padding-top: 16px;'>", unsafe_allow_html=True)
                # Pass numeric-leading delta string so Streamlit reads sign correctly
                # f"{change:+,.2f}" produces "-34.55" or "+388.65"
                # Streamlit checks for leading "-" to apply red color
                st.metric(
                    label=idx['display_name'],
                    value=f"₹{idx['close']:,.2f}",
                    delta=f"{idx['change']:+,.2f} ({idx['change_pct']:+.2f}%)"
                )
                st.caption(
                    f"O: {idx['open']:,.2f}   "
                    f"H: {idx['high']:,.2f}   "
                    f"L: {idx['low']:,.2f}"
                )
                st.markdown("</div>", unsafe_allow_html=True)

            with chart_col:
                if not chart_pivot.empty and symbol in chart_pivot.columns:
                    series = chart_pivot[symbol].dropna()
                    fig = make_sparkline(symbol, series, color)
                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
                else:
                    st.caption("No chart data available")

    else:
        st.warning("Unable to fetch market data. Please try again later.")
else:
    st.warning("Unable to determine latest trade date.")

# --- Footer ---
st.divider()
st.caption(
    "**Data:** NSE EOD (₹) | **Model:** Black-Scholes (Actual/365) | "
    "**Rates:** Linear Interpolation (3M/6M/1Y G-Bond) | **Dividends:** NSE Index Yield"
)
