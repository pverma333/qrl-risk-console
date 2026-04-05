import math


class FuturesPricer:

    @staticmethod
    def annualized_dte(dte: int, day_count: int = 365) -> float:
        if dte < 0:
            raise ValueError(f"DTE cannot be negative. Got: {dte}")
        return dte / day_count

    @staticmethod
    def theoretical_price(spot: float, rate: float, div_yield: float, T: float) -> float:
        if spot <= 0:
            raise ValueError(f"Spot must be positive. Got: {spot}")
        if T < 0:
            raise ValueError(f"T cannot be negative. Got: {T}")
        return spot * math.exp((rate - div_yield) * T)

    @staticmethod
    def basis(actual_price: float, theoretical_price: float) -> float:
        return actual_price - theoretical_price

    @staticmethod
    def delta() -> float:
        return 1.0
