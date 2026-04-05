import os

API_BASE = os.environ.get("QRL_API_BASE", "http://127.0.0.1:8000")

VALID_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

SHOCK_DEFAULTS = {
    "spot_shock_pct": 0.0,
    "vol_shock_abs":  0.0,
    "rate_shock_bps": 0.0,
}
