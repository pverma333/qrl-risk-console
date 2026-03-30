import logging
import pandas as pd
from src.core.fetch_config import FetchConfig


class ProcessedTradeCalendarBuilder:

    def __init__(self, config: FetchConfig):
        self.ingest_path = config.ingest_dir / "TradeCalendar" / "trade_calendar.parquet"
        self.output_path = config.processed_dir / "trade_calendar" / "trade_calendar.parquet"
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("Processed_TradeCalendar")

    def _read_ingest(self) -> pd.DataFrame:
        if not self.ingest_path.exists():
            raise FileNotFoundError(f"Ingest file not found: {self.ingest_path}")
        df = pd.read_parquet(self.ingest_path)
        self.logger.info("Ingest read: %d rows", len(df))
        return df

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df

    def _normalize_symbols(self, df: pd.DataFrame) -> pd.DataFrame:
        df["symbol"] = df["symbol"].map(FetchConfig.SYMBOL_NORMALISATION_MAP)
        unrecognized = df["symbol"].isnull().sum()
        if unrecognized:
            self.logger.warning("Dropping %d rows with unrecognized symbols", unrecognized)
            df = df[df["symbol"].notna()].copy()
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["trade_date", "symbol"])
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Deduplicated %d rows", dropped)
        return df

    def _validate_schema(self, df: pd.DataFrame):
        required = {"trade_date", "symbol"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Schema validation failed. Missing columns: {missing}")
        if df["trade_date"].isnull().any():
            raise ValueError("Null values found in trade_date.")
        if df["symbol"].isnull().any():
            raise ValueError("Null values found in symbol.")

    def _read_existing_processed(self) -> pd.DataFrame:
        if not self.output_path.exists():
            return pd.DataFrame(columns=["trade_date", "symbol"])
        df = pd.read_parquet(self.output_path)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df

    def _get_latest_trade_date(self, df: pd.DataFrame):
        if df.empty:
            return None
        return df["trade_date"].max()

    def _write(self, df: pd.DataFrame):
        df = df.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
        df.to_parquet(self.output_path, index=False)
        self.logger.info("Written %d rows to %s", len(df), self.output_path)

    def build_all(self):
        df = self._read_ingest()
        df = self._cast_types(df)
        df = self._normalize_symbols(df)
        df = self._deduplicate(df)
        self._validate_schema(df)
        self._write(df)
        self.logger.info("Full build complete.")

    def build_incremental(self):
        existing = self._read_existing_processed()
        latest = self._get_latest_trade_date(existing)

        df = self._read_ingest()
        df = self._cast_types(df)
        df = self._normalize_symbols(df)

        if latest is not None:
            df = df[df["trade_date"] > latest].copy()
            self.logger.info("Incremental delta: %d new rows after %s", len(df), latest)

        if df.empty:
            self.logger.info("No new rows to append. Already up to date.")
            return

        combined = pd.concat([existing, df], ignore_index=True)
        combined = self._deduplicate(combined)
        self._validate_schema(combined)
        self._write(combined)
        self.logger.info("Incremental build complete.")

    def run(self, mode: str):
        if mode == "full":
            self.build_all()
        elif mode == "incremental":
            self.build_incremental()
        else:
            raise ValueError(f"Invalid mode: '{mode}'. Expected 'full' or 'incremental'.")
