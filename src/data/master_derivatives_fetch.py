import time
import requests
import zipfile
import io
import logging
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
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
        self.raw_path = config.raw_dir
        self.processed_path = config.processed_dir
        self.master_file = self.processed_path / "Nifty_Historical_Derivatives.csv"
        self.batch_size = batch_size
        self.rebuild = rebuild

        self.log_path = config.logs_dir

        logging.basicConfig(
            filename=self.log_path / "master_derivatives_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("DerivativesFetcher")

    # Public Runner
    def run(self, start_date: date, end_date: date):

        if self.rebuild:
            for folder in self.raw_path.iterdir():
                if folder.is_dir():
                    for file in folder.iterdir():
                        file.unlink()

            if self.master_file.exists():
                self.master_file.unlink()

            self.logger.info("Rebuild mode: raw yearly files and master deleted.")

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

    # Jugaad/bhaavcopy Fetch (Pre July 2024)
    def _fetch_jugaad(self, target_date: date):
        year_folder = self.config.get_year_raw_dir(target_date.year)

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

    # NSE Archive Fetch (Post July 2024)
    def _fetch_archive(self, target_date: date):
        year_folder = self.config.get_year_raw_dir(target_date.year)
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
        df = df[df['SYMBOL'].isin(['NIFTY', 'BANKNIFTY','FINNIFTY','MIDCPNIFTY'])].copy()
        df['STRIKE_PR'] = pd.to_numeric(df['STRIKE_PR'], errors='coerce').fillna(0)
        df['INSTRUMENT'] = df.apply(
            lambda x: 'FUTIDX' if x['STRIKE_PR'] == 0 else 'OPTIDX',
            axis=1
        )
        df['TIMESTAMP'] = target_date.strftime('%Y-%m-%d')
        df = df.reindex(columns=self.COLS)
        return df

    # Save
    def _save(self, data_list):
        final_df = pd.concat(data_list, ignore_index=True)
        final_df['YEAR'] = pd.to_datetime(final_df['TIMESTAMP']).dt.year

        for year, year_df in final_df.groupby('YEAR'):
            year_folder = self.config.get_year_raw_dir(year)
            year_file = year_folder / f"Nifty_Derivatives_{year}.csv"
            year_header = not year_file.exists()
            year_df.drop(columns=['YEAR']).to_csv(
                year_file,
                mode='a',
                index=False,
                header=year_header
            )

            # create new file not append
            if self.rebuild:
                mode = 'w'
                header = True
                self.rebuild = False   # only recreate once
            else:
                mode = 'a'
                header = not self.master_file.exists()

            year_df.drop(columns=['YEAR']).to_csv(
                self.master_file,
                mode=mode,
                index=False,
                header=header
            )

            self.logger.info(f"Year {year} saved and appended to master")

            # Delete daily raw files except yearly file
            for file in year_folder.iterdir():
                if file.name != year_file.name:
                    file.unlink()

            self.logger.info(f"Cleaned raw daily files for {year}")

