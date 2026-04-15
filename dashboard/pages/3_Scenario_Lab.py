import streamlit as st
import requests
import pandas as pd
import json
import time
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents))
from config import API_BASE, VALID_SYMBOLS, SHOCK_DEFAULTS

@st.cache_data(ttl=3600)
def get_latest_trade_date():
    from config import fetch_latest_trade_date
    return fetch_latest_trade_date()

st.set_page_config(page_title="Scenario Lab", layout="wide")
st.title("Scenario Lab")
st.caption(
    "Stress test individual contracts. Apply spot, vol, and rate shocks to reprice via Black-Scholes. "
    "Analyzes shocked valuation, PnL impact, and base-level Greeks."
)
st.divider()


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


def fetch_strikes(symbol: str, trade_date: str, expiry_date: str) -> list[float]:
    for attempt in range(3):
        try:
            r = requests.get(
                f"{API_BASE}/chain/{symbol}/{trade_date}/{expiry_date}",
                timeout=60,
            )
            if r.status_code == 200:
                rows = r.json().get("rows", [])
                return sorted(set(row["strike"] for row in rows))
            return []
        except Exception:
            if attempt < 2:
                time.sleep(3)
                continue
            return []


def call_scenario_api(payload: dict) -> dict | None:
    for attempt in range(3):
        try:
            r = requests.post(
                f"{API_BASE}/scenario/",
                json=payload,
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


def render_results():
    result = st.session_state.get("sl_result")
    if result is None:
        return

    method = result.get("method", "unknown")

    st.subheader("Valuation Under Shock")
    st.caption("Figures are per unit. PnL is scaled by lot size and quantity.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Base Price",
        f"{result.get('base_price', 0):.2f}" if result.get("base_price") is not None else "—",
        help="EOD settlement price on trade date.",
    )
    col2.metric(
        "Shocked Price",
        f"{result.get('shocked_price', 0):.2f}" if result.get("shocked_price") is not None else "—",
        help="Model price after applying spot, vol, and rate shocks.",
    )
    col3.metric(
        "PnL per Lot",
        f"₹{result.get('scenario_pnl', 0) / max(result.get('quantity', 1), 1):,.2f}",
        help="Scenario PnL per lot. Scaled by lot size.",
    )
    col4.metric(
        "Total PnL",
        f"₹{result.get('total_pnl', 0):,.0f}",
        help="Net impact: (Shocked - Base) × Quantity × Lot Size.",
    )

    st.caption("Note: Single-contract mode does not account for entry price/MTM.")

    method_labels = {
        "full_reprice":   "Full Revaluation: BSM model applied with shocked inputs.",
        "greeks_approx":  "Greek Approximation: Taylor expansion used (accuracy reduced for >5% spot move).",
        "futures_linear": "Linear Payoff: Vol/rate sensitivity not applicable for futures.",
        "no_data":        "Incomplete Data: Contract unavailable for selected trade date.",
    }
    method_colors = {
        "full_reprice":   "info",
        "greeks_approx":  "warning",
        "futures_linear": "info",
        "no_data":        "error",
    }
    color = method_colors.get(method, "info")
    label = method_labels.get(method, f"Method: {method}")
    if color == "info":
        st.info(label)
    elif color == "warning":
        st.warning(label)
    elif color == "error":
        st.error(label)

    st.divider()

    st.subheader("Greeks at Base")
    st.caption("Analytical Greeks calculated at unshocked settlement price.")
    gcol1, gcol2, gcol3, gcol4, gcol5 = st.columns(5)

    def fmt(val):
        return f"{val:.2f}" if val is not None else "—"

    gcol1.metric("Delta", fmt(result.get("delta")), help="₹ P&L per 1-pt spot move.")
    gcol2.metric("Gamma", f"{result.get('gamma'):.4f}" if result.get("gamma") is not None else "—", help="Delta sensitivity to spot.")
    gcol3.metric("Vega",  fmt(result.get("vega")),  help="₹ P&L per 1% move in IV.")
    gcol4.metric("Theta", fmt(result.get("theta")), help="Daily time decay (₹).")
    gcol5.metric("Rho",   fmt(result.get("rho")),   help="₹ P&L per 1% move in rates.")

    st.divider()

    st.subheader("Data Export")
    meta = st.session_state.get("sl_meta", {})
    manifest = {
        **meta,
        "pricing_model":      "Black-Scholes",
        "day_count":          365,
        "rate_interpolation": "linear_3M_6M_1Y",
        "result":             result,
    }
    st.download_button(
        label="Download Run Manifest JSON",
        data=json.dumps(manifest, indent=2),
        file_name=f"scenario_{meta.get('symbol', '')}_{meta.get('trade_date', '')}.json",
        mime="application/json",
        use_container_width=True
    )


# ── Sidebar ──
with st.sidebar:
    st.header("Risk Controls")

    symbol = st.selectbox(
        "Index",
        VALID_SYMBOLS,
        help="Target underlying index.",
    )
    trade_date = st.date_input(
        "Trade Date",
        value=get_latest_trade_date(),
        help="Valuation date for settlement prices and IV.",
    )
    trade_date_str = str(trade_date)

    expiries = fetch_expiries(symbol, trade_date_str)
    if not expiries:
        st.warning("No expiries found. Select a valid trading day.")
        st.stop()

    expiry_date = st.selectbox(
        "Expiry",
        expiries,
        help="Contract expiration date.",
    )
    expiry_date_str = str(expiry_date)

    option_type = st.selectbox(
        "Option Type",
        ["CE", "PE", "XX"],
        help="CE: Call, PE: Put, XX: Futures.",
    )

    if option_type == "XX":
        strike = 0.0
        st.caption("Strike N/A for futures.")
    else:
        strikes = fetch_strikes(symbol, trade_date_str, expiry_date_str)
        if not strikes:
            st.warning("No strikes found for expiry.")
            st.stop()
        strike = st.selectbox(
            "Strike",
            strikes,
            help="Option exercise price.",
        )

    quantity = st.number_input(
        "Quantity (Lots)",
        min_value=-100, max_value=100, value=1, step=1,
        help="Position sizing. Positive: Long, Negative: Short.",
    )

    st.subheader("Shock Parameters")
    st.caption("Applied simultaneously for revaluation.")

    spot_shock_pct = st.slider(
        "Spot Shock (%)",
        min_value=-10.0, max_value=10.0,
        value=SHOCK_DEFAULTS["spot_shock_pct"], step=0.5,
        help="Percentage move in underlying spot.",
    )
    vol_shock_abs = st.slider(
        "Vol Shock (pts)",
        min_value=-10.0, max_value=10.0,
        value=SHOCK_DEFAULTS["vol_shock_abs"], step=0.5,
        help="Absolute change in implied volatility points.",
    )
    rate_shock_bps = st.slider(
        "Rate Shock (bps)",
        min_value=-100.0, max_value=100.0,
        value=SHOCK_DEFAULTS["rate_shock_bps"], step=5.0,
        help="Basis point change in risk-free rate.",
    )

    run = st.button("Run Scenario", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Log")
    st.caption(f"**Model:** Black-Scholes (Act/365)")
    st.caption(f"**Rates:** Linear G-Bond Interpolation")
    st.caption(f"**Valuation Date:** {trade_date_str}")
    st.caption(f"**Active Shocks:** {spot_shock_pct:+.1f}% Spot, {vol_shock_abs:+.1f} Vol, {rate_shock_bps:+.0f} bps Rate")


# ── Execution Logic ──
if run:
    if quantity == 0:
        st.warning("Quantity cannot be zero.")
        st.stop()

    payload = {
        "symbol":         symbol,
        "trade_date":     trade_date_str,
        "expiry_date":    expiry_date_str,
        "strike":         float(strike),
        "option_type":    option_type,
        "quantity":       int(quantity),
        "spot_shock_pct": spot_shock_pct,
        "vol_shock_abs":  vol_shock_abs,
        "rate_shock_bps": rate_shock_bps,
    }

    with st.spinner("Calculating..."):
        result = call_scenario_api(payload)

    if result is not None:
        st.session_state["sl_result"] = result
        st.session_state["sl_meta"]   = {
            "symbol":      symbol,
            "trade_date":  trade_date_str,
            "expiry_date": expiry_date_str,
            "strike":      float(strike),
            "option_type": option_type,
            "quantity":    int(quantity),
            "shocks": {
                "spot_shock_pct": spot_shock_pct,
                "vol_shock_abs":  vol_shock_abs,
                "rate_shock_bps": rate_shock_bps,
            },
        }


# ── Render from session state ──
render_results()
