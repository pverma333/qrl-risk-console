import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.db.connection import DuckDBConnection
from src.db.ingest_registry import IngestRegistry
from src.db.processed_registry import ProcessedRegistry

config = FetchConfig(BASE_DIR)
conn = DuckDBConnection(config.duckdb_path)

ingest_reg = IngestRegistry(conn, config)
ingest_reg.register_all()
print("Ingest views:", ingest_reg.list_registered())

processed_reg = ProcessedRegistry(conn, config)
processed_reg.register_all()
print("Processed views:", processed_reg.list_registered())

# quick query test on processed options
result = conn.get().execute("""
    SELECT trade_date, symbol, COUNT(*) as rows
    FROM v_processed_options
    WHERE symbol = 'NIFTY'
    GROUP BY trade_date, symbol
    ORDER BY trade_date DESC
    LIMIT 5
""").df()
print(result)

conn.close()

#run

"""
python scripts/audit_registry.py
"""
