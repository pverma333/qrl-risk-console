import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import json
import time
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import API_BASE, VALID_SYMBOLS

@st.cache_data(ttl=3600)
def get_latest_trade_date():
    from config import fetch_latest_trade_date
    return fetch_latest_trade_date()

st.set_page_config(page_title="VaR / CVaR", layout="wide")
st.title("VaR / CVaR")
st.caption(
    "Historical simulation: each of the past N trading days' actual NIFTY spot returns is applied to your portfolio. "
    "VaR is the loss not exceeded on X% of those days. CVaR is the average loss on the days that breach VaR. "
    "Vol and rate are held constant — this is a spot-return-driven simulation."
)
st.divider()


def call_var_api(
    symbol: str,
    trade_date: str,
    lookback_days: int,
    csv_bytes: bytes,
) -> dict | None:
    for attempt in range(3):
        try:
            r = requests.post(
                f"{API_BASE}/var/analyze",
                data={
                    "symbol":        symbol,
                    "trade_date":    trade_date,
                    "lookback_days": lookback_days,
                },
                files={"file": ("portfolio.csv", csv_bytes, "text/csv")},
                timeout=120,
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
    result = st.session_state.get("var_result")
    if result is None:
        return

    var_summary = result.get("summary", {})
    scenarios   = result.get("pnl_distribution", [])
    meta        = st.session_state.get("var_meta", {})

    st.subheader("VaR / CVaR Summary")
    st.caption(
        "VaR and CVaR are reported as positive loss magnitudes. "
        "VaR 95% = the loss exceeded only on the worst 5% of historical days. "
        "CVaR 95% = average loss on those worst 5% of days (also called Expected Shortfall)."
    )
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric(
        "Scenarios",
        var_summary.get("scenario_count", len(scenarios)),
        help="Number of historical trading days used in the simulation.",
    )
    col2.metric(
        "VaR 95%",
        f"₹{var_summary.get('var_95', 0):,.0f}",
        help="Loss not exceeded on 95% of historical days. Breached on ~13 days out of 252.",
    )
    col3.metric(
        "VaR 99%",
        f"₹{var_summary.get('var_99', 0):,.0f}",
        help="Loss not exceeded on 99% of historical days. Breached on ~3 days out of 252.",
    )
    col4.metric(
        "CVaR 95%",
        f"₹{var_summary.get('cvar_95', 0):,.0f}",
        help="Average loss on the worst 5% of days. Always greater than VaR 95%.",
    )
    col5.metric(
        "CVaR 99%",
        f"₹{var_summary.get('cvar_99', 0):,.0f}",
        help="Average loss on the worst 1% of days. This is the tail risk figure.",
    )

    st.divider()

    if scenarios:
        scenario_df = pd.DataFrame(scenarios)
        scenario_df = scenario_df.sort_values("portfolio_pnl")

        worst = scenario_df.iloc[0]
        best  = scenario_df.iloc[-1]

        wcol, bcol = st.columns(2)
        wcol.metric(
            f"Worst Day — {worst.get('date', '')}",
            f"₹{worst.get('portfolio_pnl', 0):,.0f}",
            delta=f"{worst.get('spot_return_pct', 0):.2f}% spot",
            delta_color="inverse",
            help="The single worst portfolio PnL day in the lookback window.",
        )
        bcol.metric(
            f"Best Day — {best.get('date', '')}",
            f"₹{best.get('portfolio_pnl', 0):,.0f}",
            delta=f"{best.get('spot_return_pct', 0):.2f}% spot",
            help="The single best portfolio PnL day in the lookback window.",
        )

        st.divider()

        st.subheader("PnL Distribution")
        st.caption(
            "Each bar represents the number of historical days that produced that PnL range. "
            "Vertical lines show VaR and CVaR thresholds. "
            "Losses are on the left (negative PnL); gains on the right."
        )
        pnl_values = scenario_df["portfolio_pnl"].tolist()
        var_95  = -var_summary.get("var_95",  0)
        var_99  = -var_summary.get("var_99",  0)
        cvar_95 = -var_summary.get("cvar_95", 0)
        cvar_99 = -var_summary.get("cvar_99", 0)

        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=pnl_values, nbinsx=40, name="PnL Distribution",
            marker_color="#4C9BE8", opacity=0.75,
        ))
        for val, label, color in [
            (var_95,  "VaR 95%",  "#FFA500"),
            (var_99,  "VaR 99%",  "#FF4500"),
            (cvar_95, "CVaR 95%", "#FFD700"),
            (cvar_99, "CVaR 99%", "#FF0000"),
        ]:
            fig.add_vline(
                x=val, line_dash="dash", line_color=color,
                annotation_text=label, annotation_position="top",
            )

        fig.update_layout(
            xaxis_title="Portfolio PnL (₹)", yaxis_title="Number of Days",
            height=400, margin=dict(l=40, r=40, t=40, b=40),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#FAFAFA"),
        )
        fig.update_xaxes(gridcolor="#333333")
        fig.update_yaxes(gridcolor="#333333")
        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        st.subheader("Scenario Detail")
        st.caption("All historical scenarios ranked by portfolio PnL (worst first). Spot Return is the daily arithmetic return of the index.")
        display_df = scenario_df[
            [c for c in ["date", "spot_return_pct", "portfolio_pnl"] if c in scenario_df.columns]
        ].copy()
        if "portfolio_pnl" in display_df.columns:
            display_df["portfolio_pnl"] = display_df["portfolio_pnl"].apply(
                lambda x: f"₹{x:,.0f}"
            )
        if "spot_return_pct" in display_df.columns:
            display_df["spot_return_pct"] = display_df["spot_return_pct"].apply(
                lambda x: f"{x:+.2f}%"
            )
        display_df = display_df.rename(columns={
            "date":            "Date",
            "spot_return_pct": "Spot Return",
            "portfolio_pnl":   "Portfolio PnL",
        })
        st.dataframe(display_df, use_container_width=True, height=300)

    st.divider()

    st.subheader("Export")
    ecol1, ecol2 = st.columns(2)

    with ecol1:
        if scenarios:
            st.download_button(
                label="Download Scenarios CSV",
                data=pd.DataFrame(scenarios).to_csv(index=False).encode("utf-8"),
                file_name=f"var_scenarios_{meta.get('symbol', '')}_{meta.get('trade_date', '')}.csv",
                mime="text/csv",
            )

    with ecol2:
        manifest = {
            "symbol":         meta.get("symbol"),
            "trade_date":     meta.get("trade_date"),
            "lookback_days":  meta.get("lookback_days"),
            "method":         "historical_simulation",
            "return_type":    "arithmetic",
            "vol_treatment":  "constant",
            "rate_treatment": "constant",
            "var_summary":    var_summary,
        }
        st.download_button(
            label="Download Run Manifest JSON",
            data=json.dumps(manifest, indent=2),
            file_name=f"var_manifest_{meta.get('symbol', '')}_{meta.get('trade_date', '')}.json",
            mime="application/json",
        )


# ── Sidebar ──
with st.sidebar:
    st.header("Controls")

    symbol = st.selectbox(
        "Index",
        VALID_SYMBOLS,
        help="Index whose historical spot returns are used to stress the portfolio.",
    )
    trade_date = st.date_input(
        "Trade Date",
        value=get_latest_trade_date(),
        help="The EOD date used to price your portfolio positions before applying historical scenarios.",
    )
    trade_date_str = str(trade_date)

    lookback_days = st.slider(
        "Lookback (trading days)",
        min_value=21, max_value=252, value=252, step=21,
        help=(
            "Number of historical trading days to use. "
            "21 ≈ 1 month. 63 ≈ 1 quarter. 252 ≈ 1 full year. "
            "Longer lookback captures tail events like market crashes."
        ),
    )

    st.subheader("Portfolio Upload")
    uploaded_file = st.file_uploader(
        "Upload positions CSV",
        type=["csv"],
        help="Same format as Portfolio Risk page. Each row is one position.",
    )

    run = st.button("Compute VaR / CVaR", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Audit Panel")
    st.caption("Method: Historical simulation (non-parametric)")
    st.caption("Return type: Arithmetic spot returns")
    st.caption("Vol and rate: Held constant across scenarios")
    st.caption(f"Symbol: {symbol}")
    st.caption(f"Trade date: {trade_date_str}")
    st.caption(f"Lookback: {lookback_days} trading days")

    st.divider()
    st.subheader("CSV Format")
    st.code(
        "symbol,expiry_date,strike,option_type,quantity,entry_date,entry_price\n"
        "NIFTY,2026-03-24,22500,CE,2,2026-03-10,120.50\n"
        "NIFTY,2026-03-24,22000,PE,-1,2026-03-10,95.25\n"
        "NIFTY,2026-03-30,0,XX,1,2026-03-10,22150.00",
        language="text",
    )
    st.caption("quantity: positive = long, negative = short. option_type: CE / PE / XX (futures).")


# ── Run on button click ──
if run:
    if uploaded_file is None:
        st.warning("Upload a portfolio CSV first.")
        st.stop()

    csv_bytes = uploaded_file.read()
    with st.spinner(f"Running {lookback_days}-scenario historical simulation..."):
        result = call_var_api(symbol, trade_date_str, lookback_days, csv_bytes)

    if result is not None:
        st.session_state["var_result"] = result
        st.session_state["var_meta"]   = {
            "symbol":        symbol,
            "trade_date":    trade_date_str,
            "lookback_days": lookback_days,
        }


# ── Render from session state ──
render_results()
