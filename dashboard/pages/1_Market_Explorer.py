import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import sys
import time
from pathlib import Path
from datetime import date as dt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import API_BASE, VALID_SYMBOLS

@st.cache_data(ttl=3600)
def get_latest_trade_date():
    from config import fetch_latest_trade_date
    return fetch_latest_trade_date()

st.set_page_config(page_title="Market Explorer", layout="wide")
st.title("Market Explorer")
st.caption(
    "View the full option chain for any index, date, and expiry. "
    "Implied volatility (IV) is solved from settlement prices using Black-Scholes. "
    "Greeks are computed analytically at each strike."
)
st.divider()

CHAIN_COLUMN_LABELS = {
    "strike":        "Strike",
    "option_type":   "Type",
    "settle":        "Settle",
    "iv":            "Implied Volatility (IV)",
    "delta":         "Delta",
    "gamma":         "Gamma",
    "vega":          "Vega",
    "theta":         "Theta",
    "rho":           "Rho",
    "open_interest": "Open Interest (OI)",
    "dte":           "Days To Expiry",
}

def fetch_expiries(symbol: str, trade_date: str) -> list[str]:
    for attempt in range(3):
        try:
            r = requests.get(
                f"{API_BASE}/chain/expiries/{symbol}/{trade_date}",
                timeout=30,
            )
            if r.status_code == 200:
                return r.json().get("expiries", [])
            return []
        except Exception:
            if attempt < 2:
                time.sleep(3)
                continue
            return []

def fetch_chain(symbol: str, trade_date: str, expiry_date: str) -> dict | None:
    for attempt in range(3):
        try:
            r = requests.get(
                f"{API_BASE}/chain/{symbol}/{trade_date}/{expiry_date}",
                timeout=60,
            )
            if r.status_code == 200:
                return r.json()
            st.error(f"API error {r.status_code}: {r.json().get('detail', 'Unknown error')}")
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(3)
                continue
            st.error(f"Connection error after 3 attempts: {e}")
            return None

def fetch_vix(trade_date: str) -> float | None:
    for attempt in range(3):
        try:
            r = requests.get(f"{API_BASE}/vix/{trade_date}", timeout=30)
            if r.status_code == 200:
                return r.json().get("close")
            return None
        except Exception:
            if attempt < 2:
                time.sleep(3)
                continue
            return None

def render_results(symbol: str, trade_date_str: str, expiry_date_str: str):
    data = st.session_state.get("me_chain_data")
    vix  = st.session_state.get("me_vix")

    if data is None or data["row_count"] == 0:
        st.warning("No data returned for this combination.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Symbol",      symbol)
    col2.metric("Rows",        data["row_count"])

    iv_display = f"{data.get('iv_avg', 0):.2f}" if data.get('iv_avg') else "N/A"
    col3.metric("Chain IV (%)", iv_display)

    col4.metric("India VIX",   f"{vix:.2f}" if vix else "N/A")

    st.divider()

    df = pd.DataFrame(data["rows"])
    df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.date
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date

    # Cast strike to int — removes trailing decimal zeros
    df["strike"] = df["strike"].astype(int)

    ce_df = df[df["option_type"] == "CE"].copy()
    pe_df = df[df["option_type"] == "PE"].copy()

    st.subheader("IV Smile")
    st.caption(
        "Implied volatility plotted across strikes for calls (CE) and puts (PE). "
        "A U-shaped or skewed curve is normal — deep OTM options tend to have higher IV (volatility smile/skew)."
    )
    iv_df = pd.concat([
        ce_df[["strike", "iv"]].rename(columns={"iv": "IV"}).assign(Type="CE"),
        pe_df[["strike", "iv"]].rename(columns={"iv": "IV"}).assign(Type="PE"),
    ]).dropna(subset=["IV"])

    if not iv_df.empty:
        fig = px.line(
            iv_df, x="strike", y="IV", color="Type",
            labels={"strike": "Strike", "IV": "Implied Volatility"},
            color_discrete_map={"CE": "#2196F3", "PE": "#F44336"},
        )
        fig.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No IV data available for smile chart.")

    st.divider()

    st.subheader("Option Chain")
    st.caption(
        "Settlement price and Greeks for each strike. "
        "IV = implied volatility solved from settle price. "
        "Delta = price sensitivity to spot move. "
        "Gamma = rate of change of delta. "
        "Vega = sensitivity to 1% IV change. "
        "Theta = daily time decay (₹ per day). "
        "Rho = sensitivity to 1% rate change. "
        "OI = open interest (number of open contracts). "
        "DTE = calendar days to expiry."
    )

    display_cols = ["strike", "option_type", "settle", "iv", "delta", "gamma", "vega", "theta", "rho", "open_interest", "dte"]
    chain_display = df[display_cols].copy()

    # Format Greeks to 2 decimal places; gamma to 4 (small values)
    chain_display["settle"] = chain_display["settle"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
    chain_display["iv"]     = chain_display["iv"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
    chain_display["delta"]  = chain_display["delta"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
    chain_display["gamma"]  = chain_display["gamma"].map(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
    chain_display["vega"]   = chain_display["vega"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
    chain_display["theta"]  = chain_display["theta"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
    chain_display["rho"]    = chain_display["rho"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")

    chain_display = chain_display.rename(columns=CHAIN_COLUMN_LABELS)
    st.dataframe(chain_display, use_container_width=True, height=400)

    st.divider()

    st.subheader("Delta by Strike")
    st.caption(
        "Delta ranges from 0 to +1 for calls and 0 to -1 for puts. "
        "ATM options have delta near ±0.5. Deep ITM options approach ±1."
    )
    delta_df = df[["strike", "option_type", "delta"]].dropna(subset=["delta"])
    if not delta_df.empty:
        fig2 = px.bar(
            delta_df, x="strike", y="delta", color="option_type",
            barmode="group",
            labels={"strike": "Strike", "delta": "Delta", "option_type": "Type"},
            color_discrete_map={"CE": "#2196F3", "PE": "#F44336"},
        )
        fig2.update_layout(height=300, margin=dict(t=20, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    st.subheader("Export")
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download Chain CSV",
        data=csv,
        file_name=f"chain_{symbol}_{trade_date_str}_{expiry_date_str}.csv",
        mime="text/csv",
    )

# ── Sidebar ──
with st.sidebar:
    st.header("Controls")

    symbol = st.selectbox(
        "Index",
        VALID_SYMBOLS,
        help="Select the index whose option chain you want to view.",
    )
    trade_date = st.date_input(
        "Trade Date",
        value=get_latest_trade_date(),
        help="EOD date for which chain data is loaded. Must be a trading day (weekdays, non-holiday).",
    )
    trade_date_str = str(trade_date)

    expiries = fetch_expiries(symbol, trade_date_str)
    if not expiries:
        st.warning("No expiries found for this date. Try a trading day.")
        st.stop()

    expiry_date = st.selectbox(
        "Expiry",
        expiries,
        help="Contract expiry date. NSE index options expire on the last Thursday of the month.",
    )
    expiry_date_str = str(expiry_date)

    load = st.button("Load Chain", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Panel")
    st.caption("Pricing model: Black-Scholes")
    st.caption("Day count: 365")
    st.caption("Rate interpolation: Linear (3M/6M/1Y G-Bond yields)")
    st.caption(f"Data as-of: {trade_date_str}")

# ── Load on button click ──
if load:
    with st.spinner("Loading option chain..."):
        data = fetch_chain(symbol, trade_date_str, expiry_date_str)
        vix  = fetch_vix(trade_date_str)
    st.session_state["me_chain_data"]  = data
    st.session_state["me_vix"]         = vix
    st.session_state["me_symbol"]      = symbol
    st.session_state["me_trade_date"]  = trade_date_str
    st.session_state["me_expiry_date"] = expiry_date_str

# ── Render from session state ──
if st.session_state.get("me_chain_data") is not None:
    render_results(
        st.session_state["me_symbol"],
        st.session_state["me_trade_date"],
        st.session_state["me_expiry_date"],
    )
