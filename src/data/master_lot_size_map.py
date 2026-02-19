import logging
import pandas as pd
from src.core.fetch_config import FetchConfig

class LotSizeMapStore:

    def __init__(self, config: FetchConfig):
        self.config = config
        self.namespace = "LotSize"

        self.base_path = config.ingest_dir / self.namespace
        self.map_path = self.base_path / "lot_size_map.parquet"

        self.base_path.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("LotSizeMapStore")

    def build(self):
        rows = []
        for sym in self.config.derivatives_symbols:
            if sym not in FetchConfig.DEFAULT_LOT_PERIODS:
                raise ValueError(f"DEFAULT_LOT_PERIODS missing for {sym}")
            for start, end, lot in FetchConfig.DEFAULT_LOT_PERIODS[sym]:
                rows.append({
                    "symbol": sym.upper(),
                    "start_date": pd.Timestamp(start),
                    "end_date": pd.Timestamp(end) if end else pd.NaT,
                    "lot_size": int(lot)
                })
        df = (
            pd.DataFrame(rows)
            .sort_values(["symbol", "start_date"])
            .reset_index(drop=True)
        )
        df.to_parquet(self.map_path, index=False)
        self.logger.info(f"Lot size map rebuilt: {self.map_path}")
        return self.map_path

class TradeCalendarWriter:

    def __init__(self, config: FetchConfig, rebuild: bool = False):
        self.config = config
        self.namespace = "TradeCalendar"
        self.rebuild = rebuild

        self.base_path = config.ingest_dir / self.namespace
        self.calendar_path = self.base_path / "trade_calendar.parquet"

        self.log_path = config.logs_dir

        logging.basicConfig(
            filename=self.log_path / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("TradeCalendarWriter")

    def build_from_derivatives(self, df_derivatives):

        df = df_derivatives.copy()

        df["trade_date"] = pd.to_datetime(df["TIMESTAMP"]).dt.normalize()

        cal = (
            df[["trade_date", "SYMBOL"]]
            .drop_duplicates()
            .sort_values(["trade_date", "SYMBOL"])
            .reset_index(drop=True)
        )

        self.base_path.mkdir(parents=True, exist_ok=True)
        cal.to_parquet(self.calendar_path, index=False)

        self.logger.info(f"Trade calendar written: {self.calendar_path}")

        return self.calendar_path

