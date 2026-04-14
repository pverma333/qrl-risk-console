import streamlit as st
import requests
import pandas as pd
import json
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import API_BASE, VALID_SYMBOLS, SHOCK_DEFAULTS

@st.cache_data(ttl=3600)
def get_latest_trade_date():
    from config import fetch_latest_trade_date
    return fetch_latest_trade_date()

st.set_page_config(page_title="Scenario Lab", layout="wide")
st.title("Scenario Lab")
st.caption("Stress test a single contract under custom spot, vol, and rate shocks.")
st.divider()


def fetch_expiries(symbol: str, trade_date: str) -> list[str]:
    try:
        r = requests.get(f"{API_BASE}/chain/expiries/{symbol}/{trade_date}", timeout=10)
        if r.status_code == 200:
            return r.json().get("expiries", [])
        return []
    except Exception:
        return []


def fetch_strikes(symbol: str, trade_date: str, expiry_date: str) -> list[float]:
    try:
        r = requests.get(f"{API_BASE}/chain/{symbol}/{trade_date}/{expiry_date}", timeout=15)
        if r.status_code == 200:
            rows = r.json().get("rows", [])
            return sorted(set(row["strike"] for row in rows))
        return []
    except Exception:
        return []


def call_scenario_api(payload: dict) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}/scenario/", json=payload, timeout=15)
        if r.status_code == 200:
            return r.json()
        st.error(f"API error {r.status_code}: {r.json().get('detail', 'Unknown error')}")
        return None
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None


def render_results():
    result = st.session_state.get("sl_result")
    if result is None:
        return

    method = result.get("method", "unknown")

    st.subheader("Scenario Result")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Base Price",           f"{result.get('base_price', 0):.2f}" if result.get('base_price') is not None else "—")
    col2.metric("Shocked Price",        f"{result.get('shocked_price', 0):.2f}" if result.get('shocked_price') is not None else "—")
    col3.metric("Scenario PnL per Lot", f"₹{result.get('scenario_pnl', 0) / max(result.get('quantity', 1), 1):,.2f}")
    col4.metric("Total PnL",            f"₹{result.get('total_pnl', 0):,.0f}")
    st.caption("MtM PnL is not available for single-contract scenario analysis — no entry price provided. Use Portfolio Risk page for MtM computation.")

    method_labels = {
        "full_reprice":   "✅ full_reprice — BS repriced under shocked parameters",
        "greeks_approx":  "⚠️ greeks_approx — Greeks approximation, accuracy degrades beyond 5% spot shock",
        "futures_linear": "ℹ️ futures_linear — linear futures payoff, no vol/rate sensitivity",
        "no_data":        "❌ no_data — contract not found in curated layer for this trade date",
    }
    st.info(method_labels.get(method, f"Method: {method}"))

    st.divider()

    st.subheader("Greeks at Base")
    gcol1, gcol2, gcol3, gcol4, gcol5 = st.columns(5)

    def fmt_greek(val):
        return f"{val:.4f}" if val is not None else "—"

    gcol1.metric("Delta", fmt_greek(result.get("delta")))
    gcol2.metric("Gamma", fmt_greek(result.get("gamma")))
    gcol3.metric("Vega",  fmt_greek(result.get("vega")))
    gcol4.metric("Theta", fmt_greek(result.get("theta")))
    gcol5.metric("Rho",   fmt_greek(result.get("rho")))

    st.divider()

    st.subheader("Export")
    meta = st.session_state.get("sl_meta", {})
    manifest = {
        **meta,
        "pricing_model":     "Black-Scholes",
        "day_count":         365,
        "rate_interpolation":"linear_3M_6M_1Y",
        "result":            result,
    }
    st.download_button(
        label="Download Run Manifest JSON",
        data=json.dumps(manifest, indent=2),
        file_name=f"scenario_{meta.get('symbol', '')}_{meta.get('trade_date', '')}_{int(meta.get('strike', 0))}_{meta.get('option_type', '')}.json",
        mime="application/json",
    )


# ── Sidebar ──
with st.sidebar:
    st.header("Controls")

    symbol         = st.selectbox("Index", VALID_SYMBOLS)
    trade_date = st.date_input("Trade Date", value=get_latest_trade_date())
    trade_date_str = str(trade_date)

    expiries = fetch_expiries(symbol, trade_date_str)
    if not expiries:
        st.warning("No expiries found for this date. Try a trading day.")
        st.stop()

    expiry_date     = st.selectbox("Expiry", expiries)
    expiry_date_str = str(expiry_date)

    option_type = st.selectbox("Option Type", ["CE", "PE", "XX"])

    if option_type == "XX":
        strike = 0.0
        st.caption("Strike is not applicable for futures (XX).")
    else:
        strikes = fetch_strikes(symbol, trade_date_str, expiry_date_str)
        if not strikes:
            st.warning("No strikes found for this expiry.")
            st.stop()
        strike = st.selectbox("Strike", strikes)

    quantity = st.number_input(
        "Quantity (positive = long, negative = short)",
        min_value=-100, max_value=100, value=1, step=1,
    )

    st.subheader("Shock Parameters")
    spot_shock_pct = st.slider("Spot Shock (%)",        min_value=-10.0,  max_value=10.0,  value=SHOCK_DEFAULTS["spot_shock_pct"], step=0.5)
    vol_shock_abs  = st.slider("Vol Shock (vol points)", min_value=-10.0, max_value=10.0,  value=SHOCK_DEFAULTS["vol_shock_abs"],  step=0.5)
    rate_shock_bps = st.slider("Rate Shock (bps)",      min_value=-100.0, max_value=100.0, value=SHOCK_DEFAULTS["rate_shock_bps"], step=5.0)

    run = st.button("Run Scenario", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Panel")
    st.caption("Pricing model: Black-Scholes")
    st.caption("Day count: 365")
    st.caption("Rate interpolation: Linear (3M/6M/1Y)")
    st.caption(f"Trade date: {trade_date_str}")
    st.caption(f"Spot shock: {spot_shock_pct:+.1f}%")
    st.caption(f"Vol shock: {vol_shock_abs:+.1f} vol pts")
    st.caption(f"Rate shock: {rate_shock_bps:+.0f} bps")


# ── Run on button click ──
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

    with st.spinner("Running scenario..."):
        result = call_scenario_api(payload)

    if result is not None:
        st.session_state["sl_result"] = result
        st.session_state["sl_meta"]   = {
            "symbol":         symbol,
            "trade_date":     trade_date_str,
            "expiry_date":    expiry_date_str,
            "strike":         float(strike),
            "option_type":    option_type,
            "quantity":       int(quantity),
            "shocks": {
                "spot_shock_pct": spot_shock_pct,
                "vol_shock_abs":  vol_shock_abs,
                "rate_shock_bps": rate_shock_bps,
            },
        }


# ── Render from session state ──
render_results()
