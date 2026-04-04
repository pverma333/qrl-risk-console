import logging
from functools import lru_cache
from pathlib import Path
import duckdb
from src.core.fetch_config import FetchConfig
from src.db.connection import DuckDBConnection
from src.db.ingest_registry import IngestRegistry
from src.db.processed_registry import ProcessedRegistry
from src.db.curated_registry import CuratedRegistry

logger = logging.getLogger("app.dependencies")

@lru_cache(maxsize=1)
def get_db() -> duckdb.DuckDBPyConnection:
    base_dir = Path(__file__).resolve().parent.parent
    config = FetchConfig(base_dir=base_dir)

    db_conn = DuckDBConnection(db_path=config.duckdb_path)

    IngestRegistry(db_conn=db_conn, config=config).register_all()
    ProcessedRegistry(db_conn=db_conn, config=config).register_all()
    CuratedRegistry(db_conn=db_conn, config=config).register_all()

    logger.info("DuckDB connection established. All views registered.")
    return db_conn.get()
