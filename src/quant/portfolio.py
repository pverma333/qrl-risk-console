import math
import pandas as pd
from dataclasses import dataclass
from typing import Optional

from src.quant.scenario_engine import (
    MarketSnapshot, Shock, OptionContract, FuturesContract,
    ScenarioPnL, scenario_option, scenario_futures,
)
from src.quant.black_scholes import _bs_price, _time_to_expiry


VALID_SYMBOLS   = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
VALID_OPT_TYPES = {"CE", "PE", "XX"}
REQUIRED_COLS   = {
    "symbol", "expiry_date", "strike", "option_type",
    "quantity", "entry_date", "entry_price",
}


@dataclass
class PositionResult:
    symbol:         str
    expiry_date:    str
    strike:         float
    option_type:    str
    quantity:       int
    lot_size:       int
    entry_date:     str
    entry_price:    float
    current_price:  Optional[float]
    mtm_pnl:        float
    scenario_pnl:   float
    total_pnl:      float
    method:         str
    delta:          Optional[float]
    gamma:          Optional[float]
    vega:           Optional[float]
    theta:          Optional[float]
    rho:            Optional[float]


@dataclass
class PortfolioSummary:
    total_mtm_pnl:      float
    total_scenario_pnl: float
    total_pnl:          float
    net_delta:          float
    net_gamma:          float
    net_vega:           float
    net_theta:          float
    net_rho:            float


@dataclass
class PortfolioResult:
    trade_date:  str
    positions:   list[PositionResult]
    summary:     PortfolioSummary


def _validate_csv(df: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    df["symbol"]      = df["symbol"].str.strip().str.upper()
    df["option_type"] = df["option_type"].str.strip().str.upper()

    bad_symbols = set(df["symbol"].unique()) - VALID_SYMBOLS
    if bad_symbols:
        raise ValueError(f"Unknown symbols in CSV: {bad_symbols}")

    bad_types = set(df["option_type"].unique()) - VALID_OPT_TYPES
    if bad_types:
        raise ValueError(f"Unknown option types in CSV: {bad_types}")

    df["quantity"]    = df["quantity"].astype(int)
    df["strike"]      = df["strike"].astype(float)
    df["entry_price"] = df["entry_price"].astype(float)
    df["entry_date"]  = pd.to_datetime(df["entry_date"]).dt.date
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date

    if (df["quantity"] == 0).any():
        raise ValueError("quantity cannot be zero.")
    if (df["entry_price"] <= 0).any():
        raise ValueError("entry_price must be positive.")

    return df


def _compute_mtm_option(
    snapshot: MarketSnapshot,
    contract: OptionContract,
    entry_price: float,
) -> tuple[Optional[float], float]:
    if snapshot.dte <= 0 or snapshot.iv is None or snapshot.iv <= 0:
        return None, 0.0

    T = _time_to_expiry(snapshot.dte)
    current_price = _bs_price(
        snapshot.spot, contract.strike, T,
        snapshot.rate, snapshot.div_yield,
        snapshot.iv, contract.option_type,
    )
    multiplier = contract.quantity * contract.lot_size
    mtm_pnl    = (current_price - entry_price) * multiplier
    return current_price, mtm_pnl


def _compute_mtm_futures(
    snapshot: MarketSnapshot,
    contract: FuturesContract,
    entry_price: float,
) -> tuple[float, float]:
    multiplier    = contract.quantity * contract.lot_size
    mtm_pnl       = (snapshot.spot - entry_price) * multiplier
    return snapshot.spot, mtm_pnl


def _get_lot_size(curated_df: pd.DataFrame, symbol: str) -> int:
    rows = curated_df[curated_df["symbol"] == symbol]
    if rows.empty:
        raise ValueError(f"No curated data found for symbol: {symbol}")
    return int(rows.iloc[0]["lot_size"])


def _build_snapshot(row: pd.Series) -> MarketSnapshot:
    return MarketSnapshot(
        spot=float(row["spot"]),
        iv=float(row["iv"])       if pd.notna(row["iv"])    else None,
        rate=float(row["rate"]),
        div_yield=float(row["div_yield"]),
        dte=int(row["dte"]),
        delta=float(row["delta"]) if pd.notna(row["delta"]) else None,
        gamma=float(row["gamma"]) if pd.notna(row["gamma"]) else None,
        vega=float(row["vega"])   if pd.notna(row["vega"])  else None,
        theta=float(row["theta"]) if pd.notna(row["theta"]) else None,
        rho=float(row["rho"])     if pd.notna(row["rho"])   else None,
    )


def run_portfolio(
    positions_df: pd.DataFrame,
    curated_options: pd.DataFrame,
    curated_futures: pd.DataFrame,
    lot_size_df: pd.DataFrame,
    shock: Shock,
    trade_date: str,
) -> PortfolioResult:
    positions_df = _validate_csv(positions_df)
    results: list[PositionResult] = []

    for _, pos in positions_df.iterrows():
        symbol      = pos["symbol"]
        option_type = pos["option_type"]
        expiry_date = pos["expiry_date"]
        strike      = pos["strike"]
        quantity    = int(pos["quantity"])
        entry_price = float(pos["entry_price"])
        entry_date  = str(pos["entry_date"])

        trade_date_as_date = pd.Timestamp(trade_date).date()
        lot_size_filtered = lot_size_df[lot_size_df["symbol"] == symbol].copy()
        lot_size_filtered["start_date"] = pd.to_datetime(lot_size_filtered["start_date"], errors="coerce").dt.date
        lot_size_filtered["end_date"]   = pd.to_datetime(lot_size_filtered["end_date"],   errors="coerce").dt.date
        lot_rows = lot_size_filtered[
            (lot_size_filtered["start_date"] <= trade_date_as_date) &
            (
                lot_size_filtered["end_date"].isna() |
                (lot_size_filtered["end_date"] >= trade_date_as_date)
            )
        ]
        lot_size = int(lot_rows.iloc[0]["lot_size"]) if not lot_rows.empty else 1

        if option_type == "XX":
            fut_rows = curated_futures[
                (curated_futures["symbol"]      == symbol) &
                (curated_futures["expiry_date"] == expiry_date)
            ]
            if fut_rows.empty:
                results.append(PositionResult(
                    symbol=symbol, expiry_date=str(expiry_date),
                    strike=strike, option_type=option_type,
                    quantity=quantity, lot_size=lot_size,
                    entry_date=entry_date, entry_price=entry_price,
                    current_price=None, mtm_pnl=0.0,
                    scenario_pnl=0.0, total_pnl=0.0,
                    method="no_data",
                    delta=None, gamma=None, vega=None, theta=None, rho=None,
                ))
                continue

            fut_row  = fut_rows.iloc[0]
            snapshot = MarketSnapshot(
                spot=float(fut_row["spot"]),
                iv=None, rate=float(fut_row["rate"]),
                div_yield=float(fut_row["div_yield"]),
                dte=int(fut_row["dte"]),
                delta=1.0, gamma=0.0, vega=0.0, theta=0.0, rho=0.0,
            )
            contract      = FuturesContract(quantity=quantity, lot_size=lot_size)
            current_price, mtm_pnl = _compute_mtm_futures(snapshot, contract, entry_price)
            scenario      = scenario_futures(snapshot, contract, shock)
            scenario_pnl  = scenario.pnl_total

            results.append(PositionResult(
                symbol=symbol, expiry_date=str(expiry_date),
                strike=strike, option_type=option_type,
                quantity=quantity, lot_size=lot_size,
                entry_date=entry_date, entry_price=entry_price,
                current_price=current_price,
                mtm_pnl=mtm_pnl, scenario_pnl=scenario_pnl,
                total_pnl=mtm_pnl + scenario_pnl,
                method="futures_linear",
                delta=1.0, gamma=0.0, vega=0.0, theta=0.0, rho=0.0,
            ))

        else:
            opt_rows = curated_options[
                (curated_options["symbol"]      == symbol) &
                (curated_options["expiry_date"] == expiry_date) &
                (curated_options["strike"]      == strike) &
                (curated_options["option_type"] == option_type)
            ]
            if opt_rows.empty:
                results.append(PositionResult(
                    symbol=symbol, expiry_date=str(expiry_date),
                    strike=strike, option_type=option_type,
                    quantity=quantity, lot_size=lot_size,
                    entry_date=entry_date, entry_price=entry_price,
                    current_price=None, mtm_pnl=0.0,
                    scenario_pnl=0.0, total_pnl=0.0,
                    method="no_data",
                    delta=None, gamma=None, vega=None, theta=None, rho=None,
                ))
                continue

            opt_row  = opt_rows.iloc[0]
            snapshot = _build_snapshot(opt_row)
            contract = OptionContract(
                strike=strike, option_type=option_type,
                quantity=quantity, lot_size=lot_size,
            )
            current_price, mtm_pnl = _compute_mtm_option(snapshot, contract, entry_price)
            scenario     = scenario_option(snapshot, contract, shock)
            scenario_pnl = scenario.pnl_total

            results.append(PositionResult(
                symbol=symbol, expiry_date=str(expiry_date),
                strike=strike, option_type=option_type,
                quantity=quantity, lot_size=lot_size,
                entry_date=entry_date, entry_price=entry_price,
                current_price=current_price,
                mtm_pnl=mtm_pnl, scenario_pnl=scenario_pnl,
                total_pnl=mtm_pnl + scenario_pnl,
                method=scenario.method,
                delta=snapshot.delta, gamma=snapshot.gamma,
                vega=snapshot.vega,   theta=snapshot.theta,
                rho=snapshot.rho,
            ))

    def _safe_sum(attr: str) -> float:
        return sum(
            (getattr(r, attr) or 0.0) * r.quantity * r.lot_size
            for r in results
        )

    summary = PortfolioSummary(
        total_mtm_pnl=sum(r.mtm_pnl      for r in results),
        total_scenario_pnl=sum(r.scenario_pnl for r in results),
        total_pnl=sum(r.total_pnl        for r in results),
        net_delta=_safe_sum("delta"),
        net_gamma=_safe_sum("gamma"),
        net_vega=_safe_sum("vega"),
        net_theta=_safe_sum("theta"),
        net_rho=_safe_sum("rho"),
    )

    return PortfolioResult(
        trade_date=trade_date,
        positions=results,
        summary=summary,
    )
