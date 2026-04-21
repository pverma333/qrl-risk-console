import io
import streamlit as st
import requests
import pandas as pd
import json
import time
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import API_BASE, VALID_SYMBOLS, SHOCK_DEFAULTS, fetch_market_summary

@st.cache_data(ttl=3600)
def get_latest_trade_date():
    from config import fetch_latest_trade_date
    return fetch_latest_trade_date()

st.set_page_config(page_title="Portfolio Risk", layout="wide")
st.title("Portfolio Risk")
st.caption(
    "Analyze EOD Mark-to-Market and Scenario PnL for Index F&O positions. "
    "Apply spot, vol, and rate shocks to assess aggregated risk and net Greeks."
)
st.divider()

POSITION_COLUMN_LABELS = {
    "symbol":        "Symbol",
    "expiry_date":   "Expiry",
    "strike":        "Strike",
    "option_type":   "Type",
    "quantity":      "Quantity",
    "lot_size":      "Lot Size",
    "entry_price":   "Entry",
    "current_price": "Current Price",
    "mtm_pnl":       "Mark-to-Market PnL",
    "scenario_pnl":  "Scenario PnL",
    "status":        "Status",
}


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


def render_market_context(trade_date_str: str, portfolio_symbols: list[str]):
    """
    Fetch and display spot close for portfolio symbols and India VIX
    for the selected trade date. Uses existing /market/summary endpoint.
    """
    market_data = fetch_market_summary(trade_date_str)
    if not market_data:
        return

    # Filter indices to only symbols present in the portfolio
    relevant_indices = [
        idx for idx in market_data['indices']
        if idx['symbol'] in portfolio_symbols
    ]

    if not relevant_indices and market_data['vix']['value'] is None:
        return

    st.subheader("Market Context")
    st.caption(f"Spot and volatility data for {trade_date_str}")

    # Build columns: one per relevant index + one for VIX
    n_cols = len(relevant_indices) + 1
    cols = st.columns(n_cols)

    for i, idx in enumerate(relevant_indices):
        with cols[i]:
            st.metric(
                label=f"{idx['display_name']} Spot",
                value=f"₹{idx['close']:,.2f}",
                delta=f"{idx['change']:+,.2f} ({idx['change_pct']:+.2f}%)"
            )

    # VIX always in last column
    with cols[-1]:
        vix_val = market_data['vix']['value']
        st.metric(
            label="India VIX",
            value=f"{vix_val:.2f}" if vix_val is not None else "N/A"
        )

    st.divider()


def render_results():
    result = st.session_state.get("pr_result")
    if result is None:
        return

    meta      = st.session_state.get("pr_meta", {})
    summary   = result.get("summary", {})
    positions = result.get("positions", [])

    # Extract unique symbols from positions for market context filter
    portfolio_symbols = list({p['symbol'] for p in positions})

    # --- Market Context: spot + VIX for trade date ---
    render_market_context(meta.get("trade_date", ""), portfolio_symbols)

    st.subheader("Performance Summary")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Positions", len(positions), help="Total position rows processed.")
    col2.metric(
        "MtM PnL",
        f"₹{summary.get('total_mtm_pnl', 0):,.0f}",
        help="Mark-to-Market: EOD settle vs. entry price.",
    )
    col3.metric(
        "Scenario PnL",
        f"₹{summary.get('total_scenario_pnl', 0):,.0f}",
        help="Net PnL under current shock parameters.",
    )
    col4.metric(
        "Net Delta",
        f"{summary.get('net_delta', 0):.2f}",
        help="₹ gain/loss per 1-point move in underlying.",
    )

    st.divider()

    st.subheader("Net Portfolio Greeks")
    st.caption("Aggregated risk metrics weighted by quantity and lot size.")
    gcol1, gcol2, gcol3, gcol4, gcol5 = st.columns(5)
    gcol1.metric("Delta", f"{summary.get('net_delta', 0):.2f}", help="₹ P&L per 1-pt spot move.")
    gcol2.metric("Gamma", f"{summary.get('net_gamma', 0):.4f}", help="Delta sensitivity to spot.")
    gcol3.metric("Vega", f"{summary.get('net_vega', 0):.2f}", help="₹ P&L per 1% move in IV.")
    gcol4.metric("Theta", f"{summary.get('net_theta', 0):.2f}", help="Daily time decay (₹).")
    gcol5.metric("Rho", f"{summary.get('net_rho', 0):.2f}", help="₹ P&L per 1% move in rates.")

    st.divider()

    st.subheader("Position Breakdown")
    st.caption("Detailed MTM and Scenario analysis per contract.")
    pos_df = pd.DataFrame(positions)

    if pos_df.empty:
        st.info("No position data found.")
        return

    if "strike" in pos_df.columns:
        pos_df["strike"] = pos_df["strike"].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) and float(x) != 0 else x
        )

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

    pos_display = pos_display.rename(columns=POSITION_COLUMN_LABELS)

    has_status = "Status" in pos_display.columns

    def highlight_no_data(row):
        if has_status and row.get("Status") == "no_data":
            return ["background-color: #fff3cd"] * len(row)
        return [""] * len(row)

    st.dataframe(
        pos_display.style.apply(highlight_no_data, axis=1),
        use_container_width=True,
        hide_index=True
    )

    if "status" in pos_df.columns:
        no_data_count = pos_df[pos_df["status"] == "no_data"].shape[0]
        if no_data_count > 0:
            st.warning(
                f"{no_data_count} contract(s) not found for the selected date. "
                f"Check strike and expiry."
            )

    st.divider()

    st.subheader("Data Export")
    export_col1, export_col2 = st.columns(2)

    with export_col1:
        st.download_button(
            label="Download CSV Results",
            data=pos_df.to_csv(index=False).encode("utf-8"),
            file_name=f"risk_report_{meta.get('trade_date', 'unknown')}.csv",
            mime="text/csv",
            use_container_width=True,
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
            label="Download JSON Manifest",
            data=json.dumps(manifest, indent=2),
            file_name=f"risk_manifest_{meta.get('trade_date', 'unknown')}.json",
            mime="application/json",
            use_container_width=True,
        )


with st.sidebar:
    st.header("Risk Controls")

    trade_date = st.date_input(
        "Trade Date",
        value=get_latest_trade_date(),
        help="Date for EOD pricing and valuation.",
    )
    trade_date_str = str(trade_date)

    st.subheader("Stress Parameters")
    st.caption("Apply shocks to model portfolio impact.")

    spot_shock_pct = st.slider(
        "Spot Shock (%)",
        -10.0, 10.0,
        SHOCK_DEFAULTS["spot_shock_pct"], 0.5,
        help="Percentage move in underlying spot price.",
    )
    vol_shock_abs = st.slider(
        "Vol Shock (pts)",
        -10.0, 10.0,
        SHOCK_DEFAULTS["vol_shock_abs"], 0.5,
        help="Absolute change in implied volatility points.",
    )
    rate_shock_bps = st.slider(
        "Rate Shock (bps)",
        -100.0, 100.0,
        SHOCK_DEFAULTS["rate_shock_bps"], 5.0,
        help="Basis point change in risk-free rate.",
    )

    st.subheader("Portfolio Input")
    uploaded_file = st.file_uploader(
        "Upload CSV",
        type=["csv"],
        help="Requires: symbol, expiry_date, strike, option_type, quantity, entry_date, entry_price.",
    )

    analyze = st.button("Run Risk Analysis", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Log")
    st.caption(f"**Model:** Black-Scholes (Act/365)")
    st.caption(f"**Rates:** Linear G-Bond Interpolation")
    st.caption(f"**Valuation Date:** {trade_date_str}")

    st.divider()
    with st.expander("CSV Format Guide"):
        st.code(
            "symbol,expiry_date,strike,option_type,quantity,entry_date,entry_price\n"
            "NIFTY,2026-03-24,22500,CE,50,2026-03-10,120.50\n"
            "NIFTY,2026-03-30,0,XX,75,2026-03-10,22150.00",
            language="text",
        )
        st.caption("Qty: + Long, - Short | Type: CE/PE/XX (Futures)")

if analyze:
    if uploaded_file is None:
        st.warning("Upload a portfolio CSV first.")
        st.stop()

    csv_bytes = uploaded_file.read()
    with st.spinner("Calculating..."):
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

render_results()
