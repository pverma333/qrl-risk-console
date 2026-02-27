import logging
from datetime import datetime
import requests
import pandas as pd
from src.core.fetch_config import FetchConfig


class GbondDailyFetch:

    URL = "https://scanner.tradingview.com/global/scan"

    TICKER_MAP = {
        "IN03MY": "3m",
        "IN06MY": "6m",
        "IN01Y": "1y",
    }

    def __init__(self, config: FetchConfig):
        self.config = config
        self.ingest_dir = self.config.get_year_ingest_dir("gbond")
        self.output_path = self.ingest_dir / "gbond_combined.parquet"
        self.trade_calendar_path = self.config.ingest_dir / "TradeCalendar" / "trade_calendar.parquet"

        logging.basicConfig(
            filename=self.config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        )

        self.logger = logging.getLogger("GbondDailyFetcher")

    def _is_trading_day(self):
        #today = pd.Timestamp("2026-02-26") --> date input
        today = pd.Timestamp(datetime.today().date())
        if not self.trade_calendar_path.exists():
            self.logger.warning("Trade calendar not found. Skipping gbond fetch.")
            return False,today
        df_calendar = pd.read_parquet(
            self.trade_calendar_path,
            columns=["trade_date"]
        )
        is_trading = (df_calendar["trade_date"] == today).any()
        return is_trading, today

    def run(self):
        is_trading, today = self._is_trading_day()
        if not is_trading:
            #self.logger.info(f"{today} is not a trading day. Skipping gbond fetch.") -- single day fetch
            self.logger.info(f"{today.date()} is not a trading day. Skipping gbond fetch.")
            return None
        self.ingest_dir.mkdir(parents=True, exist_ok=True)
        new_df = self._fetch()
        if new_df.empty:
            self.logger.warning("No gbond data fetched. Skipping update.")
            return None
        if self.output_path.exists():
            existing = pd.read_parquet(self.output_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["date", "tenor"], keep="last")
        else:
            combined = new_df
        combined = self._recalculate_change(combined)
        combined = combined.sort_values(["date", "tenor"]).reset_index(drop=True)
        combined.to_parquet(self.output_path, index=False)
        self.logger.info("gbond_combined.parquet updated successfully")
        return combined

    def _fetch(self):
        payload = {
            "symbols": {
                "tickers": ["TVC:IN03MY", "TVC:IN06MY", "TVC:IN01Y"]
            },
            "columns": ["name", "open", "high", "low", "close"],
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
        }
        response = requests.post(self.URL, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        rows = []
        #today = pd.Timestamp("2026-02-26") -- date input
        today = pd.Timestamp(datetime.today().date())
        for item in data.get("data", []):
            d = item.get("d", [])
            if len(d) < 5:
                continue
            raw_name = d[0]
            tenor = self.TICKER_MAP.get(raw_name)
            if tenor:
                rows.append({
                    "date": today,
                    "price": float(d[4]),
                    "open": float(d[1]),
                    "high": float(d[2]),
                    "low": float(d[3]),
                    "change %": None,
                    "tenor": tenor,
                })
        return pd.DataFrame(rows)

    def _recalculate_change(self, df):
        df = df.sort_values(["tenor", "date"])
        prev_price = df.groupby("tenor")["price"].shift(1)
        pct = (df["price"] - prev_price) / prev_price
        df["change %"] = (pct * 100).round(2)
        df.loc[prev_price.isna(), "change %"] = None
        return df
