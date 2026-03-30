from src.quant.yield_curve import TenorRates, interpolate_rate

rates = TenorRates(rate_3m=6.440, rate_6m=6.560, rate_1y=6.566)

test_cases = [
    (0,   "below 3m — flat at 3m"),
    (45,  "below 3m — flat at 3m"),
    (91,  "exactly 3m"),
    (120, "between 3m and 6m"),
    (182, "exactly 6m"),
    (270, "between 6m and 1y"),
    (365, "exactly 1y"),
    (400, "above 1y — flat at 1y"),
]

for dte, label in test_cases:
    r = interpolate_rate(rates, dte)
    print(f"DTE={dte:4d} | {label:<30} | rate={r:.6f} ({r*100:.4f}%)")
