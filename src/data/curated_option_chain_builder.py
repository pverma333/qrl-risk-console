import logging
import numpy as np
import pandas as pd
from src.core.fetch_config import FetchConfig
from src.db.connection import DuckDBConnection
from src.db.processed_registry import ProcessedRegistry
from src.quant.bs_vectorized import compute_batch

QUERY = """
    SELECT
        o.trade_date,
        o.symbol,
        o.expiry_date,
        o.strike,
        o.option_type,
        o.open,
        o.high,
        o.low,
        o.close,
        o.settle,
        o.contracts,
        o.open_interest,
        o.chg_in_oi,
        o.dte,
        s.close      AS spot,
        y.div_yield,
        g3.yield_pct AS rate_3m,
        g6.yield_pct AS rate_6m,
        g1.yield_pct AS rate_1y,
        COALESCE(g3.rate_imputed, FALSE) AS rate_imputed
    FROM v_processed_options o
    LEFT JOIN v_processed_index_spot s
        ON o.trade_date = s.trade_date AND o.symbol = s.symbol
    LEFT JOIN v_processed_index_yield y
        ON o.trade_date = y.trade_date AND o.symbol = y.symbol
    LEFT JOIN v_processed_gbond g3
        ON o.trade_date = g3.trade_date AND g3.tenor = '3m'
    LEFT JOIN v_processed_gbond g6
        ON o.trade_date = g6.trade_date AND g6.tenor = '6m'
    LEFT JOIN v_processed_gbond g1
        ON o.trade_date = g1.trade_date AND g1.tenor = '1y'
"""

# Fallback for processed G-bond parquets that pre-date the rate_imputed column.
_QUERY_COMPAT = """
    SELECT
        o.trade_date,
        o.symbol,
        o.expiry_date,
        o.strike,
        o.option_type,
        o.open,
        o.high,
        o.low,
        o.close,
        o.settle,
        o.contracts,
        o.open_interest,
        o.chg_in_oi,
        o.dte,
        s.close      AS spot,
        y.div_yield,
        g3.yield_pct AS rate_3m,
        g6.yield_pct AS rate_6m,
        g1.yield_pct AS rate_1y
    FROM v_processed_options o
    LEFT JOIN v_processed_index_spot s
        ON o.trade_date = s.trade_date AND o.symbol = s.symbol
    LEFT JOIN v_processed_index_yield y
        ON o.trade_date = y.trade_date AND o.symbol = y.symbol
    LEFT JOIN v_processed_gbond g3
        ON o.trade_date = g3.trade_date AND g3.tenor = '3m'
    LEFT JOIN v_processed_gbond g6
        ON o.trade_date = g6.trade_date AND g6.tenor = '6m'
    LEFT JOIN v_processed_gbond g1
        ON o.trade_date = g1.trade_date AND g1.tenor = '1y'
"""


class CuratedOptionChainBuilder:

    def __init__(self, config: FetchConfig):
        self.config = config
        self.output_root = config.curated_dir / "option_chain"

        conn = DuckDBConnection(config.duckdb_path)
        reg = ProcessedRegistry(conn, config)
        reg.register_all()
        self.con = conn.get()

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("Curated_OptionChain")

    def _get_available_years(self) -> list[int]:
        result = self.con.execute("""
            SELECT DISTINCT YEAR(trade_date) AS yr
            FROM v_processed_options
            ORDER BY yr
        """).df()
        return result["yr"].tolist()

    def _get_latest_trade_date(self, year: int):
        path = self.output_root / str(year) / f"curated_options_{year}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path, columns=["trade_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df["trade_date"].max()

    def _get_existing_trade_dates(self, year: int) -> set:
        path = self.output_root / str(year) / f"curated_options_{year}.parquet"
        if not path.exists():
            return set()
        df = pd.read_parquet(path, columns=["trade_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return set(df["trade_date"])

    def _get_confirmed_dates(self, year: int) -> set:
        """Curated dates with rate_imputed=False — no need to re-process."""
        path = self.output_root / str(year) / f"curated_options_{year}.parquet"
        if not path.exists():
            return set()
        cols = ["trade_date"] + (["rate_imputed"] if "rate_imputed" in
                pd.read_parquet(path, columns=["trade_date"]).columns else [])
        df = pd.read_parquet(path)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        if "rate_imputed" not in df.columns:
            return set(df["trade_date"])
        return set(df.loc[~df["rate_imputed"], "trade_date"])

    def _get_imputed_dates(self, year: int) -> set:
        """Curated dates with rate_imputed=True — candidates for re-processing."""
        path = self.output_root / str(year) / f"curated_options_{year}.parquet"
        if not path.exists():
            return set()
        df = pd.read_parquet(path)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        if "rate_imputed" not in df.columns:
            return set()
        return set(df.loc[df["rate_imputed"], "trade_date"])

    def _dates_now_real_in_gbond(self, imputed_dates: set, year: int) -> set:
        """Return which imputed curated dates now have rate_imputed=False in processed G-bond."""
        if not imputed_dates:
            return set()
        try:
            date_list = ", ".join(f"'{d}'" for d in sorted(imputed_dates))
            result = self.con.execute(f"""
                SELECT DISTINCT trade_date
                FROM v_processed_gbond
                WHERE trade_date IN ({date_list})
                  AND YEAR(trade_date) = {year}
                  AND rate_imputed = FALSE
            """).df()
            result["trade_date"] = pd.to_datetime(result["trade_date"]).dt.date
            return set(result["trade_date"])
        except Exception:
            return set()

    def _query_year(self, year: int, existing_dates: set = None) -> pd.DataFrame:
        where = f"WHERE YEAR(o.trade_date) = {year}"
        if existing_dates:
            date_list = ", ".join(f"'{d}'" for d in sorted(existing_dates))
            where += f" AND o.trade_date NOT IN ({date_list})"
        try:
            df = self.con.execute(QUERY + " " + where).df()
        except Exception:
            # Processed G-bond parquets pre-date rate_imputed column; treat as False.
            df = self.con.execute(_QUERY_COMPAT + " " + where).df()
            df["rate_imputed"] = False
        self.logger.info("Year %d: queried %d rows", year, len(df))
        return df

    _JOIN_COLS = ["spot", "div_yield", "rate_3m", "rate_6m", "rate_1y"]

    def _check_join_completeness(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        missing_mask = df[self._JOIN_COLS].isnull().any(axis=1)
        rejected = df[missing_mask].copy()
        complete = df[~missing_mask].copy()
        if not rejected.empty:
            for _, row in rejected.iterrows():
                missing = [c for c in self._JOIN_COLS if pd.isna(row[c])]
                self.logger.warning(
                    "REJECTION: trade_date=%s symbol=%s missing=%s",
                    row["trade_date"], row["symbol"], missing,
                )
            self.logger.warning(
                "REJECTION SUMMARY: %d rows dropped (missing join inputs)", len(rejected)
            )
        return complete, rejected

    def _interpolate_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        dte = df["dte"].to_numpy(dtype=np.float64)
        r3m = df["rate_3m"].to_numpy(dtype=np.float64) / 100
        r6m = df["rate_6m"].to_numpy(dtype=np.float64) / 100
        r1y = df["rate_1y"].to_numpy(dtype=np.float64) / 100

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
                    r1y
                )
            )
        )

        df = df.copy()
        df["rate"] = rate
        return df

    def _compute_quant(self, df: pd.DataFrame) -> pd.DataFrame:
        results = compute_batch(df)
        df = df.copy()
        df["iv"]    = results["iv"]
        df["delta"] = results["delta"]
        df["gamma"] = results["gamma"]
        df["vega"]  = results["vega"]
        df["theta"] = results["theta"]
        df["rho"]   = results["rho"]
        return df

    def _drop_rate_tenor_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=["rate_3m", "rate_6m", "rate_1y"])

    def _log_null_summary(self, df: pd.DataFrame, year: int):
        total   = len(df)
        null_iv = df["iv"].isna().sum()
        pct     = null_iv / total * 100 if total > 0 else 0
        self.logger.info(
            "Year %d: total=%d | null_iv=%d (%.1f%%) | computed=%d",
            year, total, null_iv, pct, total - null_iv
        )

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        key    = ["trade_date", "symbol", "expiry_date", "strike", "option_type"]
        before = len(df)
        df     = df.drop_duplicates(subset=key)
        dropped = before - len(df)
        if dropped:
            self.logger.warning("Deduplicated %d rows", dropped)
        return df

    def _validate_schema(self, df: pd.DataFrame):
        required = {
            "trade_date", "symbol", "expiry_date", "strike", "option_type",
            "open", "high", "low", "close", "settle",
            "contracts", "open_interest", "chg_in_oi", "dte",
            "spot", "div_yield", "rate",
            "iv", "delta", "gamma", "vega", "theta", "rho",
            "rate_imputed",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Schema validation failed. Missing columns: {missing}")
        for col in ["trade_date", "symbol", "expiry_date", "strike", "option_type"]:
            if df[col].isnull().any():
                raise ValueError(f"Null values found in required column: {col}")

    def _write_partitioned(self, df: pd.DataFrame, year: int, mode: str,
                           invalidate_dates: set = None):
        out_path = self.output_root / str(year) / f"curated_options_{year}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "incremental" and out_path.exists():
            existing = pd.read_parquet(out_path)
            existing = existing.copy()
            existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
            existing["expiry_date"] = pd.to_datetime(existing["expiry_date"]).dt.date
            if "rate_imputed" not in existing.columns:
                existing["rate_imputed"] = False
            if invalidate_dates:
                existing = existing[~existing["trade_date"].isin(invalidate_dates)]
            df = df.copy()
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
            combined = pd.concat([existing, df], ignore_index=True)
            df = self._deduplicate(combined)

        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date

        df = df.sort_values(
            ["trade_date", "symbol", "expiry_date", "strike", "option_type"]
        ).reset_index(drop=True)

        df.to_parquet(out_path, index=False)
        self.logger.info(
            "Year %d: written %d rows to %s", year, len(df), out_path
        )

    def _process_year(self, year: int, mode: str):
        self.logger.info("Processing year %d | mode=%s", year, mode)

        if mode == "incremental":
            confirmed_dates = self._get_confirmed_dates(year)
            imputed_dates   = self._get_imputed_dates(year)
            now_real        = self._dates_now_real_in_gbond(imputed_dates, year)
            # Skip confirmed (real) dates; re-process imputed dates whose G-bond is now real
            skip_dates      = confirmed_dates - now_real
            invalidate_dates = now_real
        else:
            skip_dates       = set()
            invalidate_dates = set()

        df = self._query_year(year, existing_dates=skip_dates)

        if df.empty:
            self.logger.info("Year %d: no new rows. Skipping.", year)
            return

        df, rejected = self._check_join_completeness(df)
        if not rejected.empty:
            self.logger.warning(
                "Year %d: %d rows dropped due to missing join inputs — "
                "check spot/yield/gbond sources for gaps",
                year, len(rejected),
            )
        if df.empty:
            self.logger.warning("Year %d: all rows rejected — skipping write.", year)
            return

        df = self._interpolate_rates(df)
        df = self._compute_quant(df)
        df = self._drop_rate_tenor_cols(df)
        self._log_null_summary(df, year)
        df = self._deduplicate(df)
        self._validate_schema(df)
        self._write_partitioned(df, year, mode, invalidate_dates=invalidate_dates)

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
            raise ValueError(
                f"Invalid mode: '{mode}'. Expected 'full' or 'incremental'."
            )
