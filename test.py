import pandas as pd

df = pd.read_parquet("data/processed/gbond/2025/processed_gbond_2025.parquet")
print(df.dtypes)
print(df[["trade_date", "tenor", "yield_pct", "change_pct"]].tail(10).to_string())
