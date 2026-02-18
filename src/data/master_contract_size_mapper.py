import logging
import pandas as pd
from src.core.fetch_config import FetchConfig

class LotSizeMapStore:

    REQUIRED_COLS = ["symbol", "start_date", "end_date", "lot_size"]

    def __init__(self, config: FetchConfig):
        self.config = config
        self.raw_path = config.raw_dir
        self.map_file = self.raw_path / config.lot_size_map_csv_name
        self._cache = None

    def get_map(self, force_reload=False):
        if self._cache is not None and not force_reload:
            return self._cache

        if not self.map_file.exists():
            self._write_default_map()

        df = pd.read_csv(self.map_file)
        df = self._normalize(df)

        self._cache = df
        return df

    def rebuild_from_defaults(self):
        self._write_default_map()
        self._cache = None
        return self.map_file

    def _write_default_map(self):
        rows = ["symbol,start_date,end_date,lot_size"]

        for sym in self.config.derivatives_symbols:
            if sym not in FetchConfig.DEFAULT_LOT_PERIODS:
                raise ValueError(f"DEFAULT_LOT_PERIODS missing for symbol: {sym}")

            for start, end, lot in FetchConfig.DEFAULT_LOT_PERIODS[sym]:
                end_str = "" if end is None else end
                rows.append(f"{sym},{start},{end_str},{lot}")

        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.map_file.write_text("\n".join(rows) + "\n", encoding="utf-8")

    def _normalize(self, df):
        missing = [c for c in self.REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"lot_size_map.csv missing columns: {missing}")

        out = df.copy()
        out["symbol"] = out["symbol"].astype(str).str.upper()
        out["start_date"] = pd.to_datetime(out["start_date"], errors="raise")
        out["end_date"] = pd.to_datetime(out["end_date"], errors="coerce")  # blank -> NaT
        out["lot_size"] = out["lot_size"].astype(int)

        return out.sort_values(["symbol", "start_date"]).reset_index(drop=True)


class LotSizeMapValidator:
    """
    Responsibility:
    - Validate mapping integrity so you don't silently compute wrong P&L
    """

    def validate(self, df_map):
        self._validate_date_order(df_map)
        self._validate_no_overlaps(df_map)

    def _validate_date_order(self, df_map):
        bad = df_map[df_map["end_date"].notna() & (df_map["end_date"] < df_map["start_date"])]
        if not bad.empty:
            raise ValueError(f"Invalid rows: end_date < start_date\n{bad}")

    def _validate_no_overlaps(self, df_map):
        for sym, g in df_map.groupby("symbol", sort=False):
            g = g.sort_values("start_date").copy()
            prev_end = g["end_date"].shift(1)
            overlap = prev_end.notna() & (g["start_date"] <= prev_end)
            if overlap.any():
                overlaps = g.loc[overlap, ["symbol", "start_date", "end_date", "lot_size"]]
                raise ValueError(f"Overlapping lot-size periods for {sym}\n{overlaps}")


class ContractSizeMapper:
    """
    Public API:
    - attach_lot_size(df) -> adds LOT_SIZE to all rows (fast + validated)
    - get_lot_size(date, symbol) -> single lookup
    - rebuild_map() -> regenerate lot_size_map.csv from FetchConfig.DEFAULT_LOT_PERIODS
    """

    def __init__(self, config: FetchConfig, date_col="TIMESTAMP", symbol_col="SYMBOL", out_col="LOT_SIZE"):
        self.config = config
        self.date_col = date_col
        self.symbol_col = symbol_col
        self.out_col = out_col

        self.store = LotSizeMapStore(config)
        self.validator = LotSizeMapValidator()

        self.processed_path = config.processed_dir
        self.calendar_file = self.processed_path / "lot_size_calendar.csv"

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_contract_size.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        )
        self.logger = logging.getLogger("ContractSizeMapper")

        self._df_map = None

    def rebuild_map(self):
        path = self.store.rebuild_from_defaults()
        self._df_map = None
        self.logger.info(f"Rebuilt lot size map at: {path}")
        return path

    def get_lot_size(self, target_date, symbol):
        df_map = self._get_validated_map()

        td = pd.Timestamp(target_date)
        sym = str(symbol).upper()

        sub = df_map[df_map["symbol"] == sym]
        if sub.empty:
            raise ValueError(f"No mapping rows for symbol: {sym}")

        mask = (sub["start_date"] <= td) & ((sub["end_date"] >= td) | (sub["end_date"].isna()))
        match = sub[mask]

        if match.empty:
            raise ValueError(f"No lot size defined for {sym} on {td.date()}")

        # If config overlaps (shouldn't after validation), pick latest start_date
        if len(match) > 1:
            match = match.sort_values("start_date", ascending=False)

        return int(match.iloc[0]["lot_size"])

    def attach_lot_size(self, df_deriv):
        df = df_deriv.copy()

        if self.date_col not in df.columns:
            raise ValueError(f"Missing date column: {self.date_col}")
        if self.symbol_col not in df.columns:
            raise ValueError(f"Missing symbol column: {self.symbol_col}")

        df[self.symbol_col] = df[self.symbol_col].astype(str).str.upper()
        df["__trade_date__"] = pd.to_datetime(df[self.date_col], errors="raise").dt.normalize()

        df_map = self._get_validated_map()

        out_parts = []

        # Process per symbol for speed + correctness with merge_asof
        for sym, df_sym in df.sort_values("__trade_date__").groupby(self.symbol_col, sort=False):
            lot_sym = df_map[df_map["symbol"] == sym].sort_values("start_date")
            if lot_sym.empty:
                raise ValueError(f"No lot-size mapping rows found for symbol: {sym}")

            merged = pd.merge_asof(
                df_sym.sort_values("__trade_date__"),
                lot_sym[["start_date", "end_date", "lot_size"]].sort_values("start_date"),
                left_on="__trade_date__",
                right_on="start_date",
                direction="backward",
            )

            # Enforce end_date
            valid = merged["end_date"].isna() | (merged["__trade_date__"] <= merged["end_date"])
            merged.loc[~valid, "lot_size"] = pd.NA

            out_parts.append(merged.drop(columns=["start_date", "end_date"]))

        result = pd.concat(out_parts, ignore_index=True)
        result = result.rename(columns={"lot_size": self.out_col})
        result = result.drop(columns=["__trade_date__"])

        if result[self.out_col].isna().any():
            missing = result[result[self.out_col].isna()][self.symbol_col].value_counts().to_dict()
            raise ValueError(f"Lot size missing for some rows. Missing counts by symbol: {missing}")

        self._write_trade_calendar(result)
        self.logger.info(f"Lot size attached. Rows: {len(result)}")

        return result

    def _get_validated_map(self):
        if self._df_map is not None:
            return self._df_map

        df_map = self.store.get_map()
        self.validator.validate(df_map)

        self._df_map = df_map
        return df_map

    def _write_trade_calendar(self, df_with_lot):
        self.processed_path.mkdir(parents=True, exist_ok=True)

        cal = df_with_lot.copy()
        cal["trade_date"] = pd.to_datetime(cal[self.date_col], errors="raise").dt.normalize()
        cal = cal[["trade_date", self.symbol_col, self.out_col]].drop_duplicates()
        cal = cal.sort_values(["trade_date", self.symbol_col]).reset_index(drop=True)
        cal = cal.rename(columns={self.symbol_col: "symbol", self.out_col: "lot_size"})

        cal.to_csv(self.calendar_file, index=False)
        self.logger.info(f"Lot size calendar written: {self.calendar_file}")
