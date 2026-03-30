import logging
from pathlib import Path
from src.db.connection import DuckDBConnection


class ProcessedRegistry:

    def __init__(self, db_conn: DuckDBConnection, config):
        self.con = db_conn.get()
        self.processed_root = config.processed_dir
        self._registered: list[str] = []

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("ProcessedRegistry")

    def _discover_views(self) -> dict[str, Path]:
        view_map = {}
        for folder in sorted(self.processed_root.iterdir()):
            if not folder.is_dir():
                continue
            has_parquet = any(folder.rglob("*.parquet"))
            if not has_parquet:
                self.logger.warning("No parquet found, skipping: %s", folder.name)
                continue
            view_name = f"v_processed_{folder.name.lower()}"
            view_map[view_name] = folder
        return view_map

    def register_all(self):
        if not self.processed_root.exists():
            raise FileNotFoundError(f"Processed root not found: {self.processed_root}")

        view_map = self._discover_views()

        if not view_map:
            raise RuntimeError("No valid processed folders discovered. Cannot proceed.")

        for view_name, folder_path in view_map.items():
            glob_pattern = str(folder_path / "**" / "*.parquet")
            self.con.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=false)
            """)
            self._registered.append(view_name)
            self.logger.info("Registered processed view: %s → %s", view_name, folder_path)

        self.logger.info("Processed views registered: %d", len(self._registered))

    def list_registered(self) -> list[str]:
        return list(self._registered)
