import logging
import pandas as pd
from src.core.fetch_config import FetchConfig

TENOR_DAYS = {"3m": 91, "6m": 182, "1y": 365}
YIELD_THRESHOLD = 15.0


class ProcessedGBondBuilder:

    def __init__(self, config: FetchConfig):
        self.ingest_file = config.ingest_dir / "gbond" / "gbond_combined.parquet"
        self.output_root = config.processed_dir / "gbond"

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("Processed_GBond")

    def _read_ingest(self) -> pd.DataFrame:
        if not self.ingest_file.exists():
            raise FileNotFoundError(f"Ingest file not found: {self.ingest_file}")
        df = pd.read_parquet(self.ingest_file)
        self.logger.info("Ingest read: %d rows", len(df))
        return df

    def _get_latest_trade_date(self, year: int):
        path = self.output_root / str(year) / f"processed_gbond_{year}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path, columns=["trade_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df["trade_date"].max()

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "date": "trade_date",
            "price": "yield_pct",
            "open": "open",
            "high": "high",
            "low": "low",
            "change %": "change_pct",
            "tenor": "tenor",
        })

    def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df

    def _correct_par_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        bad_mask = df["yield_pct"] > YIELD_THRESHOLD
        bad_count = bad_mask.sum()

        if bad_count == 0:
            return df

        affected = df[bad_mask][["trade_date", "tenor"]].copy()
        self.logger.warning(
            "Par price detected: %d rows | tenor(s): %s | period: %s to %s",
            bad_count,
            sorted(df[bad_mask]["tenor"].unique().tolist()),
            affected["trade_date"].min(),
            affected["trade_date"].max(),
        )
        self.logger.warning(
            "Action: converting par price to annualised yield pct using "
            "formula: ((100 - price) / 100) * (365 / tenor_days) * 100"
        )

        df = df.copy()
        bad_idx = df[bad_mask].index

        df.loc[bad_idx, "yield_pct"] = df.loc[bad_idx].apply(
            lambda r: ((100 - r["yield_pct"]) / 100) * (365 / TENOR_DAYS[r["tenor"]]) * 100,
            axis=1
        )
        df.loc[bad_idx, "open"] = df.loc[bad_idx].apply(
            lambda r: ((100 - r["open"]) / 100) * (365 / TENOR_DAYS[r["tenor"]]) * 100,
            axis=1
        )
        df.loc[bad_idx, "high"] = df.loc[bad_idx].apply(
            lambda r: ((100 - r["high"]) / 100) * (365 / TENOR_DAYS[r["tenor"]]) * 100,
            axis=1
        )
        df.loc[bad_idx, "low"] = df.loc[bad_idx].apply(
            lambda r: ((100 - r["low"]) / 100) * (365 / TENOR_DAYS[r["tenor"]]) * 100,
            axis=1
        )

        df = df.sort_values(["tenor", "trade_date"]).reset_index(drop=True)

        for tenor in df["tenor"].unique():
            tenor_mask = df["tenor"] == tenor
            df.loc[tenor_mask, "change_pct"] = (
                df.loc[tenor_mask, "yield_pct"].pct_change() * 100
            )

        self.logger.info("Par price correction complete. %d rows corrected.", bad_count)
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["trade_date", "tenor"])
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Deduplicated %d rows", dropped)
        return df

    def _validate_schema(self, df: pd.DataFrame):
        required = {"trade_date", "tenor", "yield_pct", "open", "high", "low", "change_pct"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Schema validation failed. Missing columns: {missing}")
        for col in ["trade_date", "tenor", "yield_pct"]:
            if df[col].isnull().any():
                raise ValueError(f"Null values found in {col}")
        invalid_tenors = set(df["tenor"].unique()) - set(TENOR_DAYS.keys())
        if invalid_tenors:
            raise ValueError(f"Unknown tenor values found: {invalid_tenors}")
        if (df["yield_pct"] > YIELD_THRESHOLD).any():
            raise ValueError("yield_pct still contains values above threshold after correction.")

    def _write_partitioned(self, df: pd.DataFrame, year: int, mode: str):
        out_path = self.output_root / str(year) / f"processed_gbond_{year}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "incremental" and out_path.exists():
            existing = pd.read_parquet(out_path)
            existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
            combined = pd.concat([existing, df], ignore_index=True)
            df = self._deduplicate(combined)

        df = df.sort_values(["trade_date", "tenor"]).reset_index(drop=True)
        df.to_parquet(out_path, index=False)
        self.logger.info("Year %d: written %d rows to %s", year, len(df), out_path)

    def _run_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._rename_columns(df)
        df = self._parse_dates(df)
        df = self._correct_par_prices(df)
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
