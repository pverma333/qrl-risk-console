import streamlit as st
import requests
import pandas as pd
import json
import time
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import API_BASE, VALID_SYMBOLS, SHOCK_DEFAULTS

@st.cache_data(ttl=3600)
def get_latest_trade_date():
    from config import fetch_latest_trade_date
    return fetch_latest_trade_date()

st.set_page_config(page_title="Portfolio Risk", layout="wide")
st.title("Portfolio Risk")
st.caption("Upload a portfolio CSV. Get MtM PnL, scenario PnL, and net Greeks under custom shocks.")
st.divider()


def call_portfolio_api(
    trade_date: str,
    spot_shock_pct: float,
    vol_shock_abs: float,
    rate_shock_bps: float,
    csv_bytes: bytes,
) -> dict | None:
    for attempt in range(3):
        try:
            r = requests.post(
                f"{API_BASE}/portfolio/analyze",
                data={
                    "trade_date":     trade_date,
                    "spot_shock_pct": spot_shock_pct,
                    "vol_shock_abs":  vol_shock_abs,
                    "rate_shock_bps": rate_shock_bps,
                },
                files={"file": ("portfolio.csv", csv_bytes, "text/csv")},
                timeout=90,
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


def render_results():
    result = st.session_state.get("pr_result")
    if result is None:
        return

    meta      = st.session_state.get("pr_meta", {})
    summary   = result.get("summary", {})
    positions = result.get("positions", [])

    st.subheader("Summary")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Positions",          len(positions))
    col2.metric("Total MtM PnL",      f"₹{summary.get('total_mtm_pnl', 0):,.0f}")
    col3.metric("Total Scenario PnL", f"₹{summary.get('total_scenario_pnl', 0):,.0f}")
    col4.metric("Net Delta",          f"{summary.get('net_delta', 0):.2f}")

    st.divider()

    st.subheader("Net Greeks")
    gcol1, gcol2, gcol3, gcol4, gcol5 = st.columns(5)
    gcol1.metric("Delta", f"{summary.get('net_delta', 0):.4f}")
    gcol2.metric("Gamma", f"{summary.get('net_gamma', 0):.6f}")
    gcol3.metric("Vega",  f"{summary.get('net_vega', 0):.4f}")
    gcol4.metric("Theta", f"{summary.get('net_theta', 0):.4f}")
    gcol5.metric("Rho",   f"{summary.get('net_rho', 0):.4f}")

    st.divider()

    st.subheader("Position Detail")
    pos_df = pd.DataFrame(positions)

    if pos_df.empty:
        st.info("No positions returned.")
        return

    display_cols = ["symbol", "expiry_date", "strike", "option_type",
                    "quantity", "lot_size", "entry_price", "current_price",
                    "mtm_pnl", "scenario_pnl", "status"]
    display_cols = [c for c in display_cols if c in pos_df.columns]
    pos_display  = pos_df[display_cols].copy()

    for col in ["mtm_pnl", "scenario_pnl"]:
        if col in pos_display.columns:
            pos_display[col] = pos_display[col].apply(
                lambda x: f"₹{x:,.0f}" if pd.notna(x) else "—"
            )
    for col in ["current_price", "entry_price"]:
        if col in pos_display.columns:
            pos_display[col] = pos_display[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "—"
            )

    has_status = "status" in pos_df.columns

    def highlight_no_data(row):
        if has_status and row.get("status") == "no_data":
            return ["background-color: #fff3cd"] * len(row)
        return [""] * len(row)

    st.dataframe(pos_display.style.apply(highlight_no_data, axis=1), use_container_width=True)

    if has_status:
        no_data_count = pos_df[pos_df["status"] == "no_data"].shape[0]
        if no_data_count > 0:
            st.warning(
                f"{no_data_count} position(s) returned no_data — "
                "no curated data found for that contract on this trade date."
            )

    st.divider()

    st.subheader("Export")
    export_col1, export_col2 = st.columns(2)

    with export_col1:
        st.download_button(
            label="Download Results CSV",
            data=pos_df.to_csv(index=False).encode("utf-8"),
            file_name=f"portfolio_risk_{meta.get('trade_date', 'unknown')}.csv",
            mime="text/csv",
        )

    with export_col2:
        manifest = {
            "trade_date":         meta.get("trade_date"),
            "pricing_model":      "Black-Scholes",
            "day_count":          365,
            "rate_interpolation": "linear_3M_6M_1Y",
            "shocks": {
                "spot_shock_pct": meta.get("spot_shock_pct"),
                "vol_shock_abs":  meta.get("vol_shock_abs"),
                "rate_shock_bps": meta.get("rate_shock_bps"),
            },
            "summary": summary,
        }
        st.download_button(
            label="Download Run Manifest JSON",
            data=json.dumps(manifest, indent=2),
            file_name=f"portfolio_manifest_{meta.get('trade_date', 'unknown')}.json",
            mime="application/json",
        )


# ── Sidebar ──
with st.sidebar:
    st.header("Controls")

    trade_date     = st.date_input("Trade Date", value=get_latest_trade_date())
    trade_date_str = str(trade_date)

    st.subheader("Shock Parameters")
    spot_shock_pct = st.slider("Spot Shock (%)",        min_value=-10.0,  max_value=10.0,  value=SHOCK_DEFAULTS["spot_shock_pct"], step=0.5)
    vol_shock_abs  = st.slider("Vol Shock (vol points)", min_value=-10.0, max_value=10.0,  value=SHOCK_DEFAULTS["vol_shock_abs"],  step=0.5)
    rate_shock_bps = st.slider("Rate Shock (bps)",      min_value=-100.0, max_value=100.0, value=SHOCK_DEFAULTS["rate_shock_bps"], step=5.0)

    st.subheader("Portfolio Upload")
    uploaded_file = st.file_uploader("Upload positions CSV", type=["csv"])

    analyze = st.button("Analyze Portfolio", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Panel")
    st.caption("Pricing model: Black-Scholes")
    st.caption("Day count: 365")
    st.caption("Rate interpolation: Linear (3M/6M/1Y)")
    st.caption(f"Trade date: {trade_date_str}")
    st.caption(f"Spot shock: {spot_shock_pct:+.1f}%")
    st.caption(f"Vol shock: {vol_shock_abs:+.1f} vol pts")
    st.caption(f"Rate shock: {rate_shock_bps:+.0f} bps")

    st.divider()
    st.subheader("CSV Format")
    st.code(
        "symbol,expiry_date,strike,option_type,quantity,entry_date,entry_price\n"
        "NIFTY,2026-03-24,22500,CE,2,2026-03-10,120.50\n"
        "NIFTY,2026-03-24,22000,PE,-1,2026-03-10,95.25\n"
        "NIFTY,2026-03-30,0,XX,1,2026-03-10,22150.00",
        language="text",
    )


# ── Analyze on button click ──
if analyze:
    if uploaded_file is None:
        st.warning("Upload a portfolio CSV first.")
        st.stop()

    csv_bytes = uploaded_file.read()
    with st.spinner("Analyzing portfolio..."):
        result = call_portfolio_api(
            trade_date_str, spot_shock_pct, vol_shock_abs, rate_shock_bps, csv_bytes
        )

    if result is not None:
        st.session_state["pr_result"] = result
        st.session_state["pr_meta"]   = {
            "trade_date":     trade_date_str,
            "spot_shock_pct": spot_shock_pct,
            "vol_shock_abs":  vol_shock_abs,
            "rate_shock_bps": rate_shock_bps,
        }


# ── Render from session state ──
render_results()
