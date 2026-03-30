# Devlog: Master Index Backfill Fetcher
Date: 2025-02-13
Module: src/data/master_index_fetch.py

---

## Implemented

- Class-based NSE EOD backfill engine (`MasterIndexFetcher`)
- Retry with exponential backoff
- Weekend skip + holiday detection (HTTP 404)
- Periodic checkpoint save
- Final write with validation
- CLI entrypoint (`python -m scripts.run_index_fetch_cli`)
- Logging enabled

---

## Data Extracted

- Nifty 50
- Nifty Bank
- Nifty Financial Services
- Nifty Midcap Select
- India VIX

Output written to:

## fetch_config.py
- Symbols for derivatives, Index, Yield are defined in config file
- Lot size and respective periods defined
- folder base directory, folders are mandatory but yearly partiona is optional

## master_derivatives_fetch.py
- Headers - accepting all type of contents, connection is always alive as fetching for long duration
- Introduced yealy unknown symbols variable for tracking and auditing purposes
- logging is at INFO level; only debug entries are not logged
- Switch Date variable is present as NSE moved away from jugaad data to archives style zip file mid 2024
- Inrebuild mode: Wipes out ingest storage for the namespace and start clean
- Temprory storing data and upoloading data in batches of 30
- Fetch only works for weekdays and _fetch_by_date() function decides whether to use _fetch_jugaad() or _fetch_archive()
- _fetch_archive() does the transaction in memory like file but does not save data to disk for each read
- in _standardize() - capturing extra symbol for later audit, filtering for required symbols
- strike price is non-relevant values are coerced to NaN and then to 0 as futures have 0 strike price;
- re-validate OPTIDX, FUTIDX row classification based on strike price being zero or non-zero using vectorized instrument classification
- save uses partion by year as quantum of data is high
- incremental append and deduplication of data is done in save block - making it rerun safe

## master_trade_calendar_writer.py
- always rebuild -> in case a historical derivatives csv is uploaded or updated.
-
