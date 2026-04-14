import os
import requests
from datetime import date, datetime

API_BASE = os.environ.get("QRL_API_BASE", "http://127.0.0.1:8000")

VALID_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

SHOCK_DEFAULTS = {
    "spot_shock_pct": 0.0,
    "vol_shock_abs":  0.0,
    "rate_shock_bps": 0.0,
}

def fetch_latest_trade_date() -> date:
    try:
        r = requests.get(f"{API_BASE}/chain/latest-date", timeout=5)
        if r.status_code == 200:
            date_str = r.json().get("latest_date")
            return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        pass
    return date.today()
