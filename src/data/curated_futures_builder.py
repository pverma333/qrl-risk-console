import logging
import numpy as np
import pandas as pd
from pathlib import Path
from src.core.fetch_config import FetchConfig
from src.db.connection import DuckDBConnection
from src.db.processed_registry import ProcessedRegistry


QUERY = """
    SELECT
        f.trade_date,
        f.symbol,
        f.expiry_date,
        f.dte,
        f.open,
        f.high,
        f.low,
        f.close,
        f.settle,
        f.contracts,
        f.open_interest,
        f.chg_in_oi,
        s.close      AS spot,
        y.div_yield,
        g3.yield_pct AS rate_3m,
        g6.yield_pct AS rate_6m,
        g1.yield_pct AS rate_1y
    FROM v_processed_futures f
    JOIN v_processed_index_spot s
        ON f.trade_date = s.trade_date AND f.symbol = s.symbol
    JOIN v_processed_index_yield y
        ON f.trade_date = y.trade_date AND f.symbol = y.symbol
    JOIN v_processed_gbond g3
        ON f.trade_date = g3.trade_date AND g3.tenor = '3m'
    JOIN v_processed_gbond g6
        ON f.trade_date = g6.trade_date AND g6.tenor = '6m'
    JOIN v_processed_gbond g1
        ON f.trade_date = g1.trade_date AND g1.tenor = '1y'
"""


class CuratedFuturesBuilder:

    def __init__(self, config: FetchConfig):
        self.config = config
        self.output_root = config.curated_dir / "futures"

        conn = DuckDBConnection(config.duckdb_path)
        reg = ProcessedRegistry(conn, config)
        reg.register_all()
        self.con = conn.get()

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        )
        self.logger = logging.getLogger("CuratedFuturesBuilder")

    def _get_available_years(self) -> list[int]:
        result = self.con.execute("""
            SELECT DISTINCT YEAR(trade_date) AS yr
            FROM v_processed_futures
            ORDER BY yr
        """).df()
        return result["yr"].tolist()

    def _get_latest_trade_date(self, year: int):
        path = self.output_root / str(year) / f"curated_futures_{year}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path, columns=["trade_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df["trade_date"].max()

    def _query_year(self, year: int, since_date=None) -> pd.DataFrame:
        where = f"WHERE YEAR(f.trade_date) = {year}"
        if since_date is not None:
            where += f" AND f.trade_date > '{since_date}'"
        df = self.con.execute(QUERY + " " + where).df()
        self.logger.info("Year %d: queried %d rows", year, len(df))
        return df

    def _interpolate_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        dte  = df["dte"].to_numpy(dtype=np.float64)
        r3m  = df["rate_3m"].to_numpy(dtype=np.float64) / 100
        r6m  = df["rate_6m"].to_numpy(dtype=np.float64) / 100
        r1y  = df["rate_1y"].to_numpy(dtype=np.float64) / 100

        d3m = 91.0
        d6m = 182.0
        d1y = 365.0

        rate = np.where(
            dte < d3m,
            r3m,
            np.where(
                dte < d6m,
                r3m + (dte - d3m) / (d6m - d3m) * (r6m - r3m),
                np.where(
                    dte < d1y,
                    r6m + (dte - d6m) / (d1y - d6m) * (r1y - r6m),
                    r1y,
                ),
            ),
        )

        df = df.copy()
        df["rate"] = rate
        return df

    def _compute_quant(self, df: pd.DataFrame) -> pd.DataFrame:
        T         = df["dte"].to_numpy(dtype=np.float64) / 365.0
        spot      = df["spot"].to_numpy(dtype=np.float64)
        rate      = df["rate"].to_numpy(dtype=np.float64)
        div_yield = df["div_yield"].to_numpy(dtype=np.float64) / 100.0
        settle    = df["settle"].to_numpy(dtype=np.float64)

        df = df.copy()
        df["theoretical_price"] = spot * np.exp((rate - div_yield) * T)
        df["basis"]             = settle - df["theoretical_price"]
        df["delta"]             = 1.0
        return df

    def _drop_rate_tenor_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=["rate_3m", "rate_6m", "rate_1y"])

    def _log_summary(self, df: pd.DataFrame, year: int):
        total       = len(df)
        null_theo   = df["theoretical_price"].isna().sum()
        self.logger.info(
            "Year %d: total=%d | null_theoretical_price=%d",
            year, total, null_theo,
        )

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        key     = ["trade_date", "symbol", "expiry_date"]
        before  = len(df)
        df      = df.drop_duplicates(subset=key)
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Deduplicated %d rows", dropped)
        return df

    def _validate_schema(self, df: pd.DataFrame):
        required = {
            "trade_date", "symbol", "expiry_date", "dte",
            "open", "high", "low", "close", "settle",
            "contracts", "open_interest", "chg_in_oi",
            "spot", "div_yield", "rate",
            "theoretical_price", "basis", "delta",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Schema validation failed. Missing columns: {missing}")
        for col in ["trade_date", "symbol", "expiry_date"]:
            if df[col].isnull().any():
                raise ValueError(f"Null values found in required column: {col}")

    def _write_partitioned(self, df: pd.DataFrame, year: int, mode: str):
        out_path = self.output_root / str(year) / f"curated_futures_{year}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "incremental" and out_path.exists():
            existing = pd.read_parquet(out_path)
            existing["trade_date"]  = pd.to_datetime(existing["trade_date"]).dt.date
            existing["expiry_date"] = pd.to_datetime(existing["expiry_date"]).dt.date
            df = df.copy()
            df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.date
            df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
            combined = pd.concat([existing, df], ignore_index=True)
            df = self._deduplicate(combined)

        df = df.copy()
        df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.date
        df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date

        df = df.sort_values(
            ["trade_date", "symbol", "expiry_date"]
        ).reset_index(drop=True)

        df.to_parquet(out_path, index=False)
        self.logger.info("Year %d: written %d rows to %s", year, len(df), out_path)

    def _process_year(self, year: int, mode: str):
        self.logger.info("Processing year %d | mode=%s", year, mode)
        since = self._get_latest_trade_date(year) if mode == "incremental" else None
        df    = self._query_year(year, since_date=since)

        if df.empty:
            self.logger.info("Year %d: no new rows. Skipping.", year)
            return

        df = self._interpolate_rates(df)
        df = self._compute_quant(df)
        df = self._drop_rate_tenor_cols(df)
        self._log_summary(df, year)
        df = self._deduplicate(df)
        self._validate_schema(df)
        self._write_partitioned(df, year, mode)

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
