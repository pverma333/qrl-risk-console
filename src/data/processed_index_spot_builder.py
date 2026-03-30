import logging
import pandas as pd
from src.core.fetch_config import FetchConfig


class ProcessedIndexSpotBuilder:

    def __init__(self, config: FetchConfig):
        self.ingest_file = config.ingest_dir / "index_spot" / "Index_Spot_Prices.parquet"
        self.output_root = config.processed_dir / "index_spot"

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("Processed_IndexSpot")

    def _read_ingest(self) -> pd.DataFrame:
        if not self.ingest_file.exists():
            raise FileNotFoundError(f"Ingest file not found: {self.ingest_file}")
        df = pd.read_parquet(self.ingest_file)
        self.logger.info("Ingest read: %d rows", len(df))
        return df

    def _get_latest_trade_date(self, year: int):
        path = self.output_root / str(year) / f"processed_index_spot_{year}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path, columns=["trade_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df["trade_date"].max()

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "Date": "trade_date",
            "Index": "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
        })

    def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df

    def _normalize_symbols(self, df: pd.DataFrame) -> pd.DataFrame:
        df["symbol"] = df["symbol"].map(FetchConfig.SYMBOL_NORMALISATION_MAP)
        unrecognized = df["symbol"].isnull().sum()
        if unrecognized:
            self.logger.warning("Dropping %d rows with unrecognized symbols", unrecognized)
            df = df[df["symbol"].notna()].copy()
        return df

    def _cast_price_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ["open", "high", "low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["trade_date", "symbol"])
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Deduplicated %d rows", dropped)
        return df

    def _validate_schema(self, df: pd.DataFrame):
        required = {"trade_date", "symbol", "open", "high", "low", "close"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Schema validation failed. Missing columns: {missing}")
        for col in ["trade_date", "symbol", "close"]:
            if df[col].isnull().any():
                raise ValueError(f"Null values found in {col}")

    def _write_partitioned(self, df: pd.DataFrame, year: int, mode: str):
        out_path = self.output_root / str(year) / f"processed_index_spot_{year}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "incremental" and out_path.exists():
            existing = pd.read_parquet(out_path)
            existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
            combined = pd.concat([existing, df], ignore_index=True)
            df = self._deduplicate(combined)

        df = df.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
        df.to_parquet(out_path, index=False)
        self.logger.info("Year %d: written %d rows to %s", year, len(df), out_path)

    def _run_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._rename_columns(df)
        df = self._parse_dates(df)
        df = self._normalize_symbols(df)
        df = self._cast_price_columns(df)
        df = self._deduplicate(df)
        self._validate_schema(df)
        return df

    def build_all(self):
        df = self._read_ingest()
        df = self._run_pipeline(df)
        df["_year"] = pd.to_datetime(df["trade_date"]).dt.year

        for year, group in df.groupby("_year"):
            group = group.drop(columns=["_year"])
            self._write_partitioned(group, year, "full")

        self.logger.info("Full build complete.")

    def build_incremental(self):
        df = self._read_ingest()
        df = self._run_pipeline(df)
        df["_year"] = pd.to_datetime(df["trade_date"]).dt.year

        for year, group in df.groupby("_year"):
            group = group.drop(columns=["_year"])
            latest = self._get_latest_trade_date(year)

            if latest is not None:
                group = group[group["trade_date"] > latest].copy()
                if not group.empty:
                    self.logger.info(
                        "Year %d: incremental delta %d rows after %s",
                        year, len(group), latest
                    )

            if group.empty:
                continue

            self._write_partitioned(group, year, "incremental")

        self.logger.info("Incremental build complete.")

    def run(self, mode: str):
        if mode == "full":
            self.build_all()
        elif mode == "incremental":
            self.build_incremental()
        else:
            raise ValueError(f"Invalid mode: '{mode}'. Expected 'full' or 'incremental'.")
