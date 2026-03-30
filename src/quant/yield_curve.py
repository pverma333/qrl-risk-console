from dataclasses import dataclass

TENOR_DAYS = {"3m": 91,"6m": 182,"1y": 365,}

@dataclass
class TenorRates:
    rate_3m: float
    rate_6m: float
    rate_1y: float


def interpolate_rate(tenor_rates: TenorRates, dte: int) -> float:
    if dte < 0:
        raise ValueError(f"DTE cannot be negative. Got: {dte}")

    d3m = TENOR_DAYS["3m"]
    d6m = TENOR_DAYS["6m"]
    d1y = TENOR_DAYS["1y"]

    r3m = tenor_rates.rate_3m / 100
    r6m = tenor_rates.rate_6m / 100
    r1y = tenor_rates.rate_1y / 100

    if dte < d3m:
        return r3m
    if dte < d6m:
        return r3m + (dte - d3m) / (d6m - d3m) * (r6m - r3m)
    if dte < d1y:
        return r6m + (dte - d6m) / (d1y - d6m) * (r1y - r6m)
    return r1y
