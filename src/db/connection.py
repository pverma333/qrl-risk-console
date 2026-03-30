import duckdb
from pathlib import Path

class DuckDBConnection:

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))

    def get(self) -> duckdb.DuckDBPyConnection:
        return self.con

    def close(self):
        self.con.close()
