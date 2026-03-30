import logging
import pandas as pd
from src.core.fetch_config import FetchConfig

COLUMN_RENAME_MAP = {
    "TIMESTAMP":   "trade_date",
    "SYMBOL":      "symbol",
    "EXPIRY_DT":   "expiry_date",
    "STRIKE_PR":   "strike",
    "OPTION_TYP":  "option_type",
    "INSTRUMENT":  "instrument",
    "OPEN":        "open",
    "HIGH":        "high",
    "LOW":         "low",
    "CLOSE":       "close",
    "SETTLE_PR":   "settle",
    "CONTRACTS":   "contracts",
    "OPEN_INT":    "open_interest",
    "CHG_IN_OI":   "chg_in_oi",
}


class ProcessedDerivativesBuilder:

    def __init__(self, config: FetchConfig):
        self.ingest_root = config.ingest_dir / "derivatives"
        self.options_root = config.processed_dir / "options"
        self.futures_root = config.processed_dir / "futures"

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("Processed_Derivatives")

    def _get_available_years(self) -> list[int]:
        if not self.ingest_root.exists():
            raise FileNotFoundError(f"Ingest root not found: {self.ingest_root}")
        years = sorted([
            int(p.name)
            for p in self.ingest_root.iterdir()
            if p.is_dir() and p.name.isdigit()
        ])
        self.logger.info("Years found in ingest: %s", years)
        return years

    def _read_ingest_year(self, year: int) -> pd.DataFrame:
        path = self.ingest_root / str(year) / f"Derivatives_{year}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"Ingest file not found: {path}")
        df = pd.read_parquet(path)
        self.logger.info("Year %d: read %d rows", year, len(df))
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=COLUMN_RENAME_MAP)

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["expiry_date"] = pd.to_datetime(df["expiry_date"], format="mixed").dt.date
        df["contracts"] = df["contracts"].astype("Int64")
        df["open_interest"] = df["open_interest"].astype("Int64")
        df["chg_in_oi"] = df["chg_in_oi"].astype("Int64")
        return df

    def _compute_dte(self, df: pd.DataFrame) -> pd.DataFrame:
        df["dte"] = (
            pd.to_datetime(df["expiry_date"]) - pd.to_datetime(df["trade_date"])
        ).dt.days
        return df

    def _split_instruments(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        options_df = df[df["instrument"] == "OPTIDX"].copy()
        futures_df = df[df["instrument"] == "FUTIDX"].copy()
        self.logger.info(
            "Split: %d options rows, %d futures rows",
            len(options_df), len(futures_df)
        )
        return options_df, futures_df

    def _drop_columns(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        cols_to_drop = ["instrument"]
        if table == "futures":
            cols_to_drop += ["strike", "option_type"]
        return df.drop(columns=cols_to_drop)

    def _deduplicate(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        if table == "options":
            key = ["trade_date", "symbol", "expiry_date", "strike", "option_type"]
        else:
            key = ["trade_date", "symbol", "expiry_date"]
        before = len(df)
        df = df.drop_duplicates(subset=key)
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Table %s: deduplicated %d rows", table, dropped)
        return df

    def _validate_schema(self, df: pd.DataFrame, table: str):
        if table == "options":
            required = {
                "trade_date", "symbol", "expiry_date", "option_type",
                "strike", "open", "high", "low", "close", "settle",
                "contracts", "open_interest", "chg_in_oi", "dte"
            }
        else:
            required = {
                "trade_date", "symbol", "expiry_date",
                "open", "high", "low", "close", "settle",
                "contracts", "open_interest", "chg_in_oi", "dte"
            }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Table {table}: missing columns {missing}")
        for col in ["trade_date", "symbol", "expiry_date"]:
            if df[col].isnull().any():
                raise ValueError(f"Table {table}: nulls found in {col}")

    def _write_partitioned(
        self, df: pd.DataFrame, year: int, table: str, mode: str
    ):
        if table == "options":
            out_path = self.options_root / str(year) / f"processed_options_{year}.parquet"
        else:
            out_path = self.futures_root / str(year) / f"processed_futures_{year}.parquet"

        out_path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "incremental" and out_path.exists():
            existing = pd.read_parquet(out_path)
            existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
            existing["expiry_date"] = pd.to_datetime(existing["expiry_date"]).dt.date
            combined = pd.concat([existing, df], ignore_index=True)
            df = self._deduplicate(combined, table)

        df = df.sort_values(["trade_date", "symbol", "expiry_date"]).reset_index(drop=True)
        df.to_parquet(out_path, index=False)
        self.logger.info("Table %s year %d: written %d rows to %s", table, year, len(df), out_path)

    def _get_latest_trade_date(self, year: int, table: str):
        if table == "options":
            path = self.options_root / str(year) / f"processed_options_{year}.parquet"
        else:
            path = self.futures_root / str(year) / f"processed_futures_{year}.parquet"

        if not path.exists():
            return None
        df = pd.read_parquet(path, columns=["trade_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df["trade_date"].max()

    def _process_year(self, year: int, mode: str):
        df = self._read_ingest_year(year)
        df = self._rename_columns(df)
        df = self._cast_types(df)
        df = self._compute_dte(df)

        if mode == "incremental":
            latest_options = self._get_latest_trade_date(year, "options")
            latest_futures = self._get_latest_trade_date(year, "futures")
            latest = min(
                d for d in [latest_options, latest_futures] if d is not None
            ) if any([latest_options, latest_futures]) else None

            if latest is not None:
                df = df[df["trade_date"] > latest].copy()
                if not df.empty:
                    self.logger.info(
                        "Year %d: incremental delta %d rows after %s",
                        year, len(df), latest
                    )

            if df.empty:
                return

        options_df, futures_df = self._split_instruments(df)
        options_df = self._drop_columns(options_df, "options")
        futures_df = self._drop_columns(futures_df, "futures")
        options_df = self._deduplicate(options_df, "options")
        futures_df = self._deduplicate(futures_df, "futures")
        self._validate_schema(options_df, "options")
        self._validate_schema(futures_df, "futures")
        self._write_partitioned(options_df, year, "options", mode)
        self._write_partitioned(futures_df, year, "futures", mode)

    def build_all(self):
        years = self._get_available_years()
        for year in years:
            self._process_year(year, "full")
        self.logger.info("Full build complete.")

    def build_incremental(self):
        years = self._get_available_years()
        for year in years:
            self._process_year(year, "incremental")
        self.logger.info("Incremental build complete.")

    def run(self, mode: str):
        if mode == "full":
            self.build_all()
        elif mode == "incremental":
            self.build_incremental()
        else:
            raise ValueError(f"Invalid mode: '{mode}'. Expected 'full' or 'incremental'.")
