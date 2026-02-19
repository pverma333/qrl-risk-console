### For QRL:
1. Storage: Parquet (partitioned by year + symbol)
2. Query: DuckDB
3. Processing: Year-wise batch jobs
4. Feature layer: Separate curated folder
5. Memory: Use categorical + float32

### Recuriter guide - If they open your repo and see:
1. Parquet partitioning
2. Schema versioning
3. Config-driven ingestion
4. SQL query layer
5. Dtype optimization
6. Batch processing

### Raw Layer (Immutable)
Source files
No transformations
Stored partitioned by date
Format: CSV or Parquet

Example -
data/raw/fo/2019/
data/raw/fo/2020/
data/raw/spot/
data/raw/yield/

### Processed Layer
- Cleaned
- Normalized schema
- Dtypes optimized
- No analytics yet
- Partitioned by year/month
- Format: Parquet only

Why Parquet?
- Columnar (fast)
- Compressed
- Reads only needed columns
- Industry standard
- Perfect for 8M rows

Example:
data/processed/derivatives/year=2024/
data/processed/spot/
data/processed/yield/

### Curated Layer
- Feature engineered
- IV computed
- Greeks computed
- DTE
- Moneyness
- Risk buckets
- Regime labels
- Also stored as partitioned Parquet

### Best Data Structure and DB - Parquet + Partioning + DuckDB
Partition by:
- year
- optionally symbol

Why?
Because in production:
You never query 2019 if you want 2025
You never query all symbols if you want NIFTY
You never query all columns

Parquet allows:
- Column pruning
- Predicate pushdown
- Huge performance gain.

Advantages:
- No server
- SQL support
- Blazing fast
- Used by hedge funds
- Recruiter impressive

### Convert Datatypes - reduces storage by 40-60%
df['symbol'] = df['symbol'].astype('category')
df['instrument'] = df['instrument'].astype('category')
df['option_typ'] = df['option_typ'].astype('category')

df['open'] = df['open'].astype('float32')
df['strike_pr'] = df['strike_pr'].astype('float32')
df['open_int'] = df['open_int'].astype('int32')

### Processing
Pattern 1: Year-wise Processing
- Process 1 year at a time.
- Append to Parquet partition.

Pattern 2: Symbol-wise Processing
- Process NIFTY separately from BANKNIFTY.

Pattern 3: Feature Computation as Batch Jobs
- Compute IV in batches
- Compute Greeks in batches
- Save results incrementally
- Production systems never recalc entire history


## Design Principle
Raw = Data Lake
Processed = Warehouse
Curated = Risk Engine Ready



""""
assumption - yes right now it only contains one parquet file but it depends on how the daily fetch will work -

1. have a script that fetches all the details daily at eod or start of the day
2. there will be different paruet files files for daily fetch and after rows are appended in the respective raw data files i will delete these files -
- derivatives
- index spot and india vix
- gov bond 1 year/ 6months/3months
- index yeild

it appends respective rows from adjacent daily detch file in their respective parquet so for isntance
daily fetch of 19th feb 2026
- index spot goes in index_spot_prices.parquet
- india vix does into india_VIX_historical.parquet
- bond file goes into gbond_combined.parquet
- index yeild goes into index_dividend_yeild.parquet
- derivaties goes and appends into ingest/derivatives/2026/derivatives_2026.parquet file


the values should be deduplicated and appended once it is successful the daily fetch will be deleted

now based on this trade calendar should run
- the moment derivatives folder any file is updated (only append new values)
- when we run the pipeline in rebuild mode (delete and recreate )
- when we run indivdual tradeclenaderwriter (delete adn recreate)


Daily Fetch → temporary daily files →
Append into consolidated historical parquet →
Deduplicate →
Delete daily temp file →
Repeat next day.

Derivatives:
data/ingest/derivatives/2026/derivatives_2026.parquet
gets appended.

Then TradeCalendar:

• If derivatives updated → append only new trade dates
• If rebuild → delete + full regenerate
• If standalone → delete + full regenerate

This is correct layering.
"""
