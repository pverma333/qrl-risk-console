import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path
from datetime import date as dt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import API_BASE, VALID_SYMBOLS

st.set_page_config(page_title="Market Explorer", layout="wide")
st.title("Market Explorer")
st.caption("Browse option chains, IV smile, and Greeks for any index, date, and expiry.")
st.divider()


def fetch_expiries(symbol: str, trade_date: str) -> list[str]:
    try:
        r = requests.get(
            f"{API_BASE}/chain/expiries/{symbol}/{trade_date}",
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("expiries", [])
        return []
    except Exception:
        return []


def fetch_chain(symbol: str, trade_date: str, expiry_date: str) -> dict | None:
    try:
        r = requests.get(
            f"{API_BASE}/chain/{symbol}/{trade_date}/{expiry_date}",
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
        st.error(f"API error {r.status_code}: {r.json().get('detail', 'Unknown error')}")
        return None
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None


def fetch_vix(trade_date: str) -> float | None:
    try:
        r = requests.get(f"{API_BASE}/vix/{trade_date}", timeout=10)
        if r.status_code == 200:
            return r.json().get("close")
        return None
    except Exception:
        return None


#Sidebar
with st.sidebar:
    st.header("Controls")
    symbol     = st.selectbox("Index", VALID_SYMBOLS)
    trade_date = st.date_input("Trade Date", value=dt(2026, 3, 13))
    trade_date_str = str(trade_date)

    expiries = fetch_expiries(symbol, trade_date_str)
    if not expiries:
        st.warning("No expiries found for this date. Try a trading day.")
        st.stop()

    expiry_date     = st.selectbox("Expiry", expiries)
    expiry_date_str = str(expiry_date)

    load = st.button("Load Chain", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Panel")
    st.caption(f"Pricing model: Black-Scholes")
    st.caption(f"Day count: 365")
    st.caption(f"Rate interpolation: Linear (3M/6M/1Y)")
    st.caption(f"Data as-of: {trade_date_str}")

if load:
    with st.spinner("Loading option chain..."):
        data = fetch_chain(symbol, trade_date_str, expiry_date_str)

    if data is None or data["row_count"] == 0:
        st.warning("No data returned for this combination.")
        st.stop()

    vix = fetch_vix(trade_date_str)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Symbol", symbol)
    col2.metric("Rows", data["row_count"])
    col3.metric("IV Computed", data["iv_computed_count"])
    col4.metric("India VIX", f"{vix:.2f}" if vix else "N/A")

    st.divider()

    df = pd.DataFrame(data["rows"])
    df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.date
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date

    ce_df = df[df["option_type"] == "CE"].copy()
    pe_df = df[df["option_type"] == "PE"].copy()

    st.subheader("IV Smile")
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

    display_cols = ["strike", "option_type", "settle", "iv", "delta", "gamma", "vega", "theta", "rho", "open_interest", "dte"]
    chain_display = df[display_cols].copy()

    chain_display["iv"]    = chain_display["iv"].map(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
    chain_display["delta"] = chain_display["delta"].map(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
    chain_display["gamma"] = chain_display["gamma"].map(lambda x: f"{x:.6f}" if pd.notna(x) else "—")
    chain_display["vega"]  = chain_display["vega"].map(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
    chain_display["theta"] = chain_display["theta"].map(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
    chain_display["rho"]   = chain_display["rho"].map(lambda x: f"{x:.4f}" if pd.notna(x) else "—")

    st.dataframe(chain_display, use_container_width=True, height=400)

    st.divider()

    st.subheader("Delta Heatmap")
    delta_df = df[["strike", "option_type", "delta"]].dropna(subset=["delta"])
    if not delta_df.empty:
        fig2 = px.bar(
            delta_df, x="strike", y="delta", color="option_type",
            barmode="group",
            labels={"strike": "Strike", "delta": "Delta"},
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
