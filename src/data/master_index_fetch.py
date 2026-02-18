import pandas as pd
import requests
import io
import time
import logging
from datetime import date, timedelta
from src.core.fetch_config import FetchConfig

class MasterIndexFetcher:

    DEFAULT_HEADERS ={
        "User-Agent": "Mozilla/5.0"
    }

    def __init__(self, config: FetchConfig, max_retries: int = 3, save_interval: int = 20,delay: float = 0.15):
        self.config = config
        self.spot_namespace = "index_spot"
        self.vix_namespace = "vix"
        self.log_path = config.logs_dir
        self.max_retries = max_retries
        self.save_interval = save_interval
        self.delay = delay

        logging.basicConfig(
            filename=self.log_path / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("IndexFetcher")

    # Fetch Index archives
    def fetch_daily_archive(self, target_date: date):

        date_str = target_date.strftime("%d%m%Y")
        url = f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{date_str}.csv"

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, headers=self.DEFAULT_HEADERS, timeout=10)
                if response.status_code == 200:
                    df = pd.read_csv(io.StringIO(response.text))
                    df.columns = [c.strip() for c in df.columns]
                    return df
                elif response.status_code == 404:
                    self.logger.info(f"Holiday / No data: {target_date}")
                    return None
                else:
                    self.logger.warning(f"Status {response.status_code} for {target_date}")
            except Exception as e:
                self.logger.error(f"Attempt {attempt} failed for {target_date}: {e}")
            time.sleep(2 ** attempt)
        self.logger.error(f"Failed after retries: {target_date}")
        return None

    # Core Runner
    def run(self, start_date: date, end_date: date):

        if start_date > end_date:
            raise ValueError("Start date must be before end date.")
        self.logger.info("Running in rebuild mode. Existing ingested spot and VIX parquet files will be replaced.")

        spot_data = []
        vix_data = []
        curr = start_date
        processed_count = 0

        self.logger.info(f"Fetch started: {start_date} to {end_date}")

        while curr <= end_date:

            if curr.weekday() >= 5:
                curr += timedelta(days=1)
                continue

            df = self.fetch_daily_archive(curr)

            if df is not None:

                # Spot
                spot_filter = df[df["Index Name"].isin(self.config.index_names)].copy()

                if not spot_filter.empty:
                    spot_filter['Date'] = curr.strftime('%Y-%m-%d')
                    temp_spot = spot_filter[
                        ['Date', 'Index Name',
                         'Open Index Value',
                         'High Index Value',
                         'Low Index Value',
                         'Closing Index Value']
                    ]
                    temp_spot.columns = ['Date', 'Index', 'Open', 'High', 'Low', 'Close']
                    spot_data.append(temp_spot)

                # VIX
                vix_filter = df[df['Index Name'] == 'India VIX'].copy()

                if not vix_filter.empty:
                    vix_filter['Date'] = curr.strftime('%Y-%m-%d')
                    temp_vix = vix_filter[['Date', 'Closing Index Value']]
                    temp_vix.columns = ['Date', 'VIX_Close']
                    vix_data.append(temp_vix)

                processed_count += 1

                if processed_count % self.save_interval == 0:
                    self._save_partial(spot_data, vix_data)

            curr += timedelta(days=1)
            time.sleep(self.delay)

        self.save_final(spot_data, vix_data)

        self.logger.info("Fetch completed successfully.")

    # Save Helpers
    def _save_partial(self, spot_data, vix_data):

        if spot_data:
            spot_folder = self.config.get_year_ingest_dir(self.spot_namespace)
            pd.concat(spot_data, ignore_index=True).to_parquet(
                spot_folder / "Index_Spot_Prices_partial.parquet",
                index=False
            )

        if vix_data:
            vix_folder = self.config.get_year_ingest_dir(self.vix_namespace)
            pd.concat(vix_data, ignore_index=True).to_parquet(
                vix_folder / "India_VIX_Historical_partial.parquet",
                index=False
            )

    def save_final(self, spot_data, vix_data):

        spot_folder = self.config.get_year_ingest_dir(self.spot_namespace)
        vix_folder = self.config.get_year_ingest_dir(self.vix_namespace)

        spot_final_path = spot_folder / "Index_Spot_Prices.parquet"
        vix_final_path = vix_folder / "India_VIX_Historical.parquet"

        spot_partial_path = spot_folder / "Index_Spot_Prices_partial.parquet"
        vix_partial_path = vix_folder / "India_VIX_Historical_partial.parquet"

        try:
            # Save Spot Final
            if spot_data:
                new_spot = pd.concat(spot_data,ignore_index = True)
                if spot_final_path.exists():
                    existing_spot = pd.read_parquet(spot_final_path)
                    combined_spot = pd.concat([existing_spot,new_spot], ignore_index=True)
                    combined_spot.drop_duplicates(subset=["Date","Index"],inplace = True)
                else:
                    combined_spot = new_spot
                combined_spot.to_parquet(spot_final_path,index=False)
                if not spot_final_path.exists():
                    raise Exception("Spot final file not created.")

            # Save VIX Final
            if vix_data:
                new_vix = pd.concat(vix_data,ignore_index = True)
                if vix_final_path.exists():
                    existing_vix = pd.read_parquet(vix_final_path)
                    combined_vix = pd.concat([existing_vix,new_vix], ignore_index=True)
                    combined_vix.drop_duplicates(subset=["Date"],inplace = True)
                else:
                    combined_vix = new_vix
                combined_vix.to_parquet(vix_final_path,index=False)
                if not vix_final_path.exists():
                    raise Exception("VIX final file not created.")

            # Delete partial csv after successful final save
            if spot_partial_path.exists():
                spot_partial_path.unlink()

            if vix_partial_path.exists():
                vix_partial_path.unlink()

            self.logger.info("Final save successful. Partial files removed.")

        except Exception as e:
            self.logger.error(f"Final save failed. Partial files retained. Error: {e}")
            raise
