import logging
import pandas as pd
from datetime import timedelta
from src.core.fetch_config import FetchConfig


class ProcessedLotSizeBuilder:

    def __init__(self, config: FetchConfig):
        self.ingest_path = config.ingest_dir / "LotSize" / "lot_size_map.parquet"
        self.output_path = config.processed_dir / "lot_size" / "lot_size.parquet"
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("Processed_LotSize")

    def _read_ingest(self) -> pd.DataFrame:
        if not self.ingest_path.exists():
            raise FileNotFoundError(f"Ingest file not found: {self.ingest_path}")
        df = pd.read_parquet(self.ingest_path)
        self.logger.info("Ingest read: %d rows", len(df))
        return df

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
        df["end_date"] = df["end_date"].apply(
            lambda x: x.date() if isinstance(x, pd.Timestamp) else x
        )
        return df

    def _normalize_symbols(self, df: pd.DataFrame) -> pd.DataFrame:
        df["symbol"] = df["symbol"].map(FetchConfig.SYMBOL_NORMALISATION_MAP)
        unrecognized = df["symbol"].isnull().sum()
        if unrecognized:
            self.logger.warning("Dropping %d rows with unrecognized symbols", unrecognized)
            df = df[df["symbol"].notna()].copy()
        return df

    def _validate_no_gaps(self, df: pd.DataFrame):
        for symbol, group in df.groupby("symbol"):
            group = group.sort_values("start_date").reset_index(drop=True)
            for i in range(len(group) - 1):
                current_end = group.loc[i, "end_date"]
                next_start = group.loc[i + 1, "start_date"]
                if current_end is None:
                    self.logger.warning(
                        "Symbol %s: row %d has no end_date but is not the last row",
                        symbol, i
                    )
                    continue
                expected_next = current_end + timedelta(days=1)
                if next_start != expected_next:
                    self.logger.warning(
                        "Symbol %s: gap or overlap between %s and %s",
                        symbol, current_end, next_start
                    )

    def _validate_no_nulls(self, df: pd.DataFrame):
        for col in ["symbol", "start_date", "lot_size"]:
            if df[col].isnull().any():
                raise ValueError(f"Null values found in required column: {col}")

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["symbol", "start_date"])
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Deduplicated %d rows", dropped)
        return df

    def _read_existing_processed(self) -> pd.DataFrame:
        if not self.output_path.exists():
            return pd.DataFrame(columns=["symbol", "start_date", "end_date", "lot_size"])
        df = pd.read_parquet(self.output_path)
        df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
        df["end_date"] = df["end_date"].apply(
            lambda x: x.date() if isinstance(x, pd.Timestamp) else x
        )
        return df

    def _detect_new_events(
        self, ingest_df: pd.DataFrame, existing_df: pd.DataFrame
    ) -> pd.DataFrame:
        if existing_df.empty:
            return ingest_df
        existing_keys = set(
            zip(existing_df["symbol"], existing_df["start_date"])
        )
        mask = ingest_df.apply(
            lambda row: (row["symbol"], row["start_date"]) not in existing_keys,
            axis=1
        )
        new_events = ingest_df[mask].copy()
        self.logger.info("New lot size events detected: %d", len(new_events))
        return new_events

    def _write(self, df: pd.DataFrame):
        df = df.sort_values(["symbol", "start_date"]).reset_index(drop=True)
        df.to_parquet(self.output_path, index=False)
        self.logger.info("Written %d rows to %s", len(df), self.output_path)

    def build_all(self):
        df = self._read_ingest()
        df = self._cast_types(df)
        df = self._normalize_symbols(df)
        df = self._deduplicate(df)
        self._validate_no_gaps(df)
        self._validate_no_nulls(df)
        self._write(df)
        self.logger.info("Full build complete.")

    def build_incremental(self):
        existing = self._read_existing_processed()
        df = self._read_ingest()
        df = self._cast_types(df)
        df = self._normalize_symbols(df)
        new_events = self._detect_new_events(df, existing)

        if new_events.empty:
            self.logger.info("No new lot size events. Already up to date.")
            return

        combined = pd.concat([existing, new_events], ignore_index=True)
        combined = self._deduplicate(combined)
        self._validate_no_gaps(combined)
        self._validate_no_nulls(combined)
        self._write(combined)
        self.logger.info("Incremental build complete.")

    def run(self, mode: str):
        if mode == "full":
            self.build_all()
        elif mode == "incremental":
            self.build_incremental()
        else:
            raise ValueError(f"Invalid mode: '{mode}'. Expected 'full' or 'incremental'.")
