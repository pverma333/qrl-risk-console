import logging
from pathlib import Path
from src.db.connection import DuckDBConnection

logger = logging.getLogger(__name__)


class IngestRegistry:

    def __init__(self, db_conn: DuckDBConnection, ingest_root: Path):
        self.con = db_conn.get()
        self.ingest_root = ingest_root
        self._registered: list[str] = []

    def _discover_views(self) -> dict[str, Path]:
        view_map = {}
        for folder in sorted(self.ingest_root.iterdir()):
            if not folder.is_dir():
                continue
            has_parquet = any(folder.rglob("*.parquet"))
            if not has_parquet:
                logger.warning("No parquet found, skipping: %s", folder.name)
                continue
            view_name = f"v_{folder.name.lower()}"
            view_map[view_name] = folder
        return view_map

    def register_all(self):
        if not self.ingest_root.exists():
            raise FileNotFoundError(f"Ingest root not found: {self.ingest_root}")

        view_map = self._discover_views()

        if not view_map:
            raise RuntimeError("No valid ingest folders discovered. Cannot proceed.")

        for view_name, folder_path in view_map.items():
            glob_pattern = str(folder_path / "**" / "*.parquet")
            self.con.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)
            """)
            self._registered.append(view_name)
            logger.info("Registered view: %s → %s", view_name, folder_path)

        logger.info("Total views registered: %d", len(self._registered))

    def list_registered(self) -> list[str]:
        return list(self._registered)
