import time
import requests
import zipfile
import io
import logging
import pandas as pd
from datetime import date, timedelta
from jugaad_data.nse import bhavcopy_fo_save
from src.core.fetch_config import FetchConfig


class DerivativesFetcher:

    SWITCH_DATE = date(2024, 6, 30)

    HEADERS = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }

    COLS = [
        'INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
        'OPEN', 'HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS',
        'OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP'
    ]

    def __init__(self, config: FetchConfig, batch_size: int = 30, rebuild: bool = False):

        self.config = config
        self.namespace = "derivatives"
        self.batch_size = batch_size
        self.rebuild = rebuild
        self._yearly_unknown_symbols = {}

        self.log_path = config.logs_dir

        logging.basicConfig(
            filename=self.log_path / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("DerivativesFetcher")

    # Public Runner
    def run(self, start_date: date, end_date: date):

        if self.rebuild:
            base_folder = self.config.get_year_ingest_dir(self.namespace)

            if base_folder.exists():
                for item in base_folder.rglob("*"):
                    if item.is_file():
                        item.unlink()

            self.logger.info("Rebuild mode: ingest yearly files and master deleted.")

        curr = start_date
        batch_data = []

        while curr <= end_date:
            if curr.weekday() >= 5:
                curr += timedelta(days=1)
                continue

            try:
                df = self._fetch_by_date(curr)

                if df is not None and not df.empty:
                    batch_data.append(df)
                    self.logger.info(f"Processed: {curr}")

                if len(batch_data) >= self.batch_size:
                    self._save(batch_data)
                    batch_data = []

            except Exception as e:
                self.logger.info(f"Error on {curr}: {e}")

            curr += timedelta(days=1)
            time.sleep(1.0)

        if batch_data:
            self._save(batch_data)

    # Fetch Router
    def _fetch_by_date(self, target_date: date):
        if target_date <= self.SWITCH_DATE:
            return self._fetch_jugaad(target_date)
        else:
            return self._fetch_archive(target_date)

    # Pre July 2024
    def _fetch_jugaad(self, target_date: date):
        year_folder = self.config.get_year_ingest_dir(self.namespace,target_date.year)
        file_path = bhavcopy_fo_save(target_date, str(year_folder))

        df = pd.read_csv(file_path)

        mapping = {
            'TradDt': 'TIMESTAMP', 'BizDt': 'TIMESTAMP',
            'InstrmntType': 'INSTRUMENT',
            'Symbl': 'SYMBOL', 'XpryDt': 'EXPIRY_DT',
            'StrkPric': 'STRIKE_PR', 'OptnTyp': 'OPTION_TYP',
            'OpnPric': 'OPEN', 'HghPric': 'HIGH',
            'LwPric': 'LOW', 'ClsPric': 'CLOSE',
            'SttlmPric': 'SETTLE_PR',
            'TtlTradgVol': 'CONTRACTS',
            'OpnIntrst': 'OPEN_INT',
            'ChngInOpnIntrst': 'CHG_IN_OI'
        }

        df = df.rename(columns=mapping)
        return self._standardize(df, target_date)

    # Post July 2024
    def _fetch_archive(self, target_date: date):
        year_folder = self.config.get_year_ingest_dir(self.namespace,target_date.year)

        url = (
            f"https://nsearchives.nseindia.com/content/fo/"
            f"BhavCopy_NSE_FO_0_0_0_{target_date.strftime('%Y%m%d')}_F_0000.csv.zip"
        )

        response = requests.get(url, headers=self.HEADERS, timeout=15)
        if response.status_code == 404:
            return None

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f)

        mapping = {
            'FinInstrmId': 'INSTRUMENT',
            'TckrSymb': 'SYMBOL',
            'XpryDt': 'EXPIRY_DT',
            'StrkPric': 'STRIKE_PR',
            'OptnTp': 'OPTION_TYP',
            'OpnPric': 'OPEN',
            'HghPric': 'HIGH',
            'LwPric': 'LOW',
            'ClsPric': 'CLOSE',
            'SttlmPric': 'SETTLE_PR',
            'TtlTradgVol': 'CONTRACTS',
            'OpnIntrst': 'OPEN_INT',
            'ChngInOpnIntrst': 'CHG_IN_OI'
        }

        df = df.rename(columns=mapping)
        return self._standardize(df, target_date)

    # Standardization
    def _standardize(self, df, target_date):

        df.columns = [c.strip() for c in df.columns]

        # Detect unknown symbols BEFORE filtering
        all_symbols = set(df["SYMBOL"].unique())
        allowed = set(self.config.derivatives_symbols)
        unknown_symbols = all_symbols - allowed

        if unknown_symbols:
            year = target_date.year
            if year not in self._yearly_unknown_symbols:
                self._yearly_unknown_symbols[year] = set()
            self._yearly_unknown_symbols[year].update(unknown_symbols)

        # Filter universe
        df = df[df["SYMBOL"].isin(allowed)].copy()

        df["STRIKE_PR"] = pd.to_numeric(df["STRIKE_PR"], errors="coerce").fillna(0)

        # Vectorized instrument classification
        df["INSTRUMENT"] = "OPTIDX"
        df.loc[df["STRIKE_PR"] == 0, "INSTRUMENT"] = "FUTIDX"

        df["TIMESTAMP"] = target_date.strftime("%Y-%m-%d")

        df = df.reindex(columns=self.COLS)

        return df

    # Save
    def _save(self, data_list):

        final_df = pd.concat(data_list, ignore_index=True)
        final_df["YEAR"] = pd.to_datetime(final_df["TIMESTAMP"]).dt.year

        for year, year_df in final_df.groupby("YEAR"):

            year_folder = self.config.get_year_ingest_dir(self.namespace,year)
            year_file = year_folder / f"Derivatives_{year}.parquet"

            new_data = year_df.drop(columns=["YEAR"])

            if year_file.exists():
                existing = pd.read_parquet(year_file)
                combined = pd.concat([existing, new_data], ignore_index=True)
                combined.drop_duplicates(subset=["TIMESTAMP", "SYMBOL", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP"],inplace=True)
            else:
                combined = new_data

            combined.to_parquet(year_file, index=False)

            self.logger.info(f"Year {year} saved (parquet overwrite)")

            if year in self._yearly_unknown_symbols:
                self.logger.info(
                    f"Year {year} - Unknown symbols encountered: "
                    f"{sorted(self._yearly_unknown_symbols[year])}"
                )
                del self._yearly_unknown_symbols[year]

            # Clean daily ingested files
            for file in year_folder.iterdir():
                if file.suffix == ".csv":
                    file.unlink()

            self.logger.info(f"Cleaned ingested daily files for {year}")

