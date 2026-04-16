import os
import requests
import streamlit as st
from datetime import date, datetime
import time

API_BASE = os.environ.get("QRL_API_BASE", "http://127.0.0.1:8000")

VALID_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

SHOCK_DEFAULTS = {
    "spot_shock_pct": 0.0,
    "vol_shock_abs":  0.0,
    "rate_shock_bps": 0.0,
}


def fetch_latest_trade_date() -> date:
    """
    Fetch the latest available trade date from the API.
    Falls back to today's date on failure.
    """
    try:
        r = requests.get(f"{API_BASE}/chain/latest-date", timeout=5)
        if r.status_code == 200:
            date_str = r.json().get("latest_date")
            return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        pass
    return date.today()


def fetch_market_summary(trade_date: str) -> dict:
    """
    Fetch market summary from API for a given trade date.

    Args:
        trade_date: date string in YYYY-MM-DD format

    Returns:
        dict with keys: indices, vix, yields, chart_data
        yields keys: rate_3m, rate_6m, rate_1y
        Returns None on failure after 3 attempts.
    """
    url = f"{API_BASE}/market/summary/{trade_date}"

    for attempt in range(3):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(3)
                continue
            st.error(f"Failed to fetch market summary: {str(e)}")
            return None
