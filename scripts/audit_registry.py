import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.db.connection import DuckDBConnection
from src.db.ingest_registry import IngestRegistry
from src.db.processed_registry import ProcessedRegistry
from src.db.curated_registry import CuratedRegistry

config = FetchConfig(BASE_DIR)
conn = DuckDBConnection(config.duckdb_path)

ingest_reg = IngestRegistry(conn, config)
ingest_reg.register_all()
print("Ingest views   :", ingest_reg.list_registered())

processed_reg = ProcessedRegistry(conn, config)
processed_reg.register_all()
print("Processed views:", processed_reg.list_registered())

curated_reg = CuratedRegistry(conn, config)
curated_reg.register_all()
print("Curated views  :", curated_reg.list_registered())

# query test
result = conn.get().execute("""
    SELECT
        symbol,
        COUNT(*)                                    AS total_rows,
        SUM(CASE WHEN iv IS NOT NULL THEN 1 END)    AS rows_with_iv,
        ROUND(AVG(iv) * 100, 2)                     AS avg_iv_pct,
        ROUND(AVG(delta), 4)                        AS avg_delta
    FROM v_curated_option_chain
    WHERE option_type = 'CE'
    AND iv IS NOT NULL
    GROUP BY symbol
    ORDER BY symbol
""").df()

print()
print(result.to_string())
conn.close()

#run

"""
python scripts/audit_registry.py
"""
