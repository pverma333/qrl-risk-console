import pandas as pd
import time
import logging
from datetime import date, timedelta
from nsepython import index_pe_pb_div
from src.core.fetch_config import FetchConfig

class MasterIndexYieldFetcher:

    def __init__(self,config: FetchConfig,max_retries: int = 3,save_interval: int = 20,delay: float = 0.15):
        self.config = config
        self.raw_path = config.raw_dir
        self.processed_path = config.processed_dir
        self.log_path = config.logs_dir

        self.indices = config.yield_names
        self.max_retries = max_retries
        self.save_interval = save_interval
        self.delay = delay

        logging.basicConfig(
            filename=self.log_path / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("IndexYieldFetcher")

        self.final_file = self.processed_path / "Index_Dividend_Yield_Historical.csv"
        self.partial_file = self.raw_path / "Index_Dividend_Yield_partial.csv"

    # Fetch single snapshot
    def fetch_snapshot(self, index_name: str, target_date: date):
        start_str = target_date.strftime("%d-%b-%Y")
        end_str = start_str
        for attempt in range(1, self.max_retries + 1):
            try:
                df = index_pe_pb_div(index_name, start_str, end_str)
                if df is None:
                    self.logger.warning(
                        f"No response for {index_name} on {target_date}"
                    )
                    return None
                if df.empty:
                    self.logger.warning(
                        f"Empty data returned for {index_name} on {target_date}"
                    )
                    return None
                df.columns = [c.strip().upper() for c in df.columns]
                if "DIVYIELD" not in df.columns:
                    self.logger.warning(
                        f"DIVYIELD column missing for {index_name} on {target_date}"
                    )
                    return None
                result = df[["DIVYIELD"]].copy()
                result["DATE"] = target_date.strftime("%Y-%m-%d")
                result["INDEX"] = index_name
                self.logger.info(f"Fetched yield for {index_name} on {target_date}")
                return result
            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt} failed for {index_name} on {target_date}: {e}"
                )
                time.sleep(2 ** attempt)
        self.logger.error(f"Failed after retries: {index_name} on {target_date}")
        return None
    # Runner
    def run(self, start_date: date, end_date: date):
        if start_date > end_date:
            raise ValueError("Start date must be before end date.")
        # rebuild
        if self.final_file.exists():
            self.final_file.unlink()
        self.logger.info(
            "Running in rebuild mode. Existing processed file will be overwritten."
        )
        self.logger.info(f"Index Yield Fetch started: {start_date} to {end_date}")
        yield_data = []
        curr = start_date
        processed_count = 0
        while curr <= end_date:
            if curr.weekday() >= 5:
                curr += timedelta(days=1)
                continue
            for index_name in self.indices:
                df = self.fetch_snapshot(index_name, curr)
                if df is not None:
                    yield_data.append(df)
                    processed_count += 1
                    if processed_count % self.save_interval == 0:
                        self._save_partial(yield_data)
                time.sleep(self.delay)
            curr += timedelta(days=1)
        self._save_final(yield_data)
        self.logger.info("Index yield fetch completed successfully.")
    # Partial Save
    def _save_partial(self, yield_data):
        if yield_data:
            pd.concat(yield_data, ignore_index=True).to_csv(
                self.partial_file,
                index=False
            )
    # Final Save
    def _save_final(self, yield_data):
        try:
            if yield_data:
                pd.concat(yield_data, ignore_index=True).to_csv(
                    self.final_file,
                    index=False
                )
                if not self.final_file.exists():
                    raise Exception("Final file not created.")
            if self.partial_file.exists():
                self.partial_file.unlink()
            self.logger.info("Final save successful. Partial file removed.")
        except Exception as e:
            self.logger.error(
                f"Final save failed. Partial retained. Error: {e}"
            )
            raise
