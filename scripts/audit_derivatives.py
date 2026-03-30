import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.fetch_config import FetchConfig
from src.db.connection import DuckDBConnection
from src.db.ingest_registry import IngestRegistry

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")


def run_audit():
    config   = FetchConfig(base_dir=Path("."))
    db_conn  = DuckDBConnection(db_path=config.duckdb_path)
    registry = IngestRegistry(db_conn=db_conn, ingest_root=config.ingest_dir)
    registry.register_all()

    con = db_conn.get()

    print("\n--- 1. SCHEMA ---")
    print(con.execute("DESCRIBE SELECT * FROM v_derivatives LIMIT 1").df().to_string())

    print("\n--- 2. NULL COUNTS ---")
    print(con.execute("""
        SELECT
            COUNT(*)                                              AS total_rows,
            COUNT(*) FILTER (WHERE CLOSE IS NULL)                AS null_close,
            COUNT(*) FILTER (WHERE SETTLE_PR IS NULL)            AS null_settle,
            COUNT(*) FILTER (WHERE EXPIRY_DT IS NULL)            AS null_expiry,
            COUNT(*) FILTER (WHERE STRIKE_PR IS NULL)            AS null_strike,
            COUNT(*) FILTER (WHERE OPTION_TYP IS NULL)           AS null_option_type
        FROM v_derivatives
    """).df().to_string())

    print("\n--- 3. ZERO VALUE COUNTS ---")
    print(con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE CLOSE = 0)         AS zero_close,
            COUNT(*) FILTER (WHERE SETTLE_PR = 0)     AS zero_settle,
            COUNT(*) FILTER (WHERE STRIKE_PR = 0)     AS zero_strike
        FROM v_derivatives
    """).df().to_string())

    print("\n--- 4. SYMBOL DISTRIBUTION ---")
    print(con.execute("""
        SELECT SYMBOL, COUNT(*) AS row_count
        FROM v_derivatives
        GROUP BY SYMBOL
        ORDER BY row_count DESC
    """).df().to_string())

    print("\n--- 5. OPTION TYPE DISTRIBUTION ---")
    print(con.execute("""
        SELECT OPTION_TYP, COUNT(*) AS row_count
        FROM v_derivatives
        GROUP BY OPTION_TYP
    """).df().to_string())

    db_conn.close()


if __name__ == "__main__":
    run_audit()
