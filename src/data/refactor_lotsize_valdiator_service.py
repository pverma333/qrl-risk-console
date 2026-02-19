import pandas as pd
class LotSizeMapValidator:

    REQUIRED_COLS = ["symbol", "start_date", "end_date", "lot_size"]

    def validate(self, df_map):

        missing = [c for c in self.REQUIRED_COLS if c not in df_map.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        self._validate_date_order(df_map)
        self._validate_no_overlaps(df_map)

    def _validate_date_order(self, df_map):

        bad = df_map[
            df_map["end_date"].notna() &
            (df_map["end_date"] < df_map["start_date"])
        ]

        if not bad.empty:
            raise ValueError("Invalid end_date < start_date detected.")

    def _validate_no_overlaps(self, df_map):

        for sym, g in df_map.groupby("symbol"):

            g = g.sort_values("start_date")
            prev_end = g["end_date"].shift(1)

            overlap = (
                prev_end.notna() &
                (g["start_date"] <= prev_end)
            )

            if overlap.any():
                raise ValueError(f"Overlapping periods for {sym}")

class LotSizeService:

    def __init__(self, df_map):
        self.df_map = df_map

    def get_lot_size(self, trade_date, symbol):

        td = pd.Timestamp(trade_date)
        sym = symbol.upper()

        sub = self.df_map[self.df_map["symbol"] == sym]

        mask = (
            (sub["start_date"] <= td) &
            ((sub["end_date"] >= td) | sub["end_date"].isna())
        )

        match = sub[mask]

        if match.empty:
            raise ValueError(f"No lot size for {sym} on {td}")

        return int(match.iloc[0]["lot_size"])

