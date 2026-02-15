import pandas as pd
import time
import logging
from datetime import date, timedelta
from pathlib import Path
from nsepython import index_pe_pb_div

class MasterIndexYieldFetcher:

    INDICES = {
        "Nifty_50": "NIFTY 50",
        "Nifty_Bank": "NIFTY BANK",
        "Nifty_Fin_Services": "NIFTY FIN SERVICE",
        "Nifty_Midcap_Select": "NIFTY MID SELECT"
    }

    def __init__(self,base_dir: Path,max_retries: int = 3,save_interval: int = 20,delay: float = 0.15):
        self.base_dir = base_dir
        self.raw_path = base_dir / "data" / "raw"
        self.log_path = base_dir / "logs"

        self.max_retries = max_retries
        self.save_interval = save_interval
        self.delay = delay

        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.log_path.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            filename=self.log_path / "master_index_yield_fetch.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    # Fetch Index Dividend Yield
    def fetch_snapshot(self, symbol: str, target_date: date):
        start_str = target_date.strftime("%d-%b-%Y")
        end_str = start_str

        for attempt in range(1, self.max_retries + 1):
            try:
                df = index_pe_pb_div(symbol, start_str, end_str)
                if df is not None and not df.empty:
                    df.columns = [c.strip().upper() for c in df.columns]
                    if "DIV YIELD" not in df.columns:
                        logging.warning(f"DIV YIELD missing for {symbol} {target_date}")
                        return None
                    result = df[["DIV YIELD"]].copy()
                    result["DATE"] = target_date.strftime("%Y-%m-%d")
                    return result
                return None
            except Exception as e:
                logging.error(f"Attempt {attempt} failed for {symbol} on {target_date}: {e}")
                time.sleep(2 ** attempt)

        logging.error(f"Failed after retries: {symbol} on {target_date}")
        return None

    # Runner
    def run(self, start_date: date, end_date: date):
        if start_date > end_date:
            raise ValueError("Start date must be before end date.")
        yield_data = []
        curr = start_date
        processed_count = 0
        logging.info(f"Index Yield Fetch started: {start_date} to {end_date}")
        while curr <= end_date:
            if curr.weekday() >= 5:
                curr += timedelta(days=1)
                continue
            for label, symbol in self.INDICES.items():
                df = self.fetch_snapshot(symbol, curr)
                if df is not None:
                    df["INDEX"] = label
                    yield_data.append(df)
                    processed_count += 1
                    if processed_count % self.save_interval == 0:
                        self._save_partial(yield_data)
                time.sleep(self.delay)
            curr += timedelta(days=1)
        self.save_final(yield_data)
        logging.info("Index yield fetch completed successfully.")
    # Save
    def _save_partial(self, yield_data):
        if yield_data:
            pd.concat(yield_data, ignore_index=True).to_csv(
                self.raw_path / "Index_Dividend_Yield_partial.csv",
                index=False
            )
    def save_final(self, yield_data):
        final_path = self.raw_path / "Index_Dividend_Yield_Historical.csv"
        partial_path = self.raw_path / "Index_Dividend_Yield_partial.csv"
        try:
            if yield_data:
                pd.concat(yield_data, ignore_index=True).to_csv(
                    final_path,
                    index=False
                )
                if not final_path.exists():
                    raise Exception("Final file not created.")
            if partial_path.exists():
                partial_path.unlink()
            logging.info("Final save successful. Partial file removed.")
        except Exception as e:
            logging.error(f"Final save failed. Partial retained. Error: {e}")
            raise
