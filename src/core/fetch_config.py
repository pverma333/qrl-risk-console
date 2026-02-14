from dataclasses import dataclass
from pathlib import Path

@dataclass
class FetchConfig:
    base_dir: Path
    use_year_partition: bool = False

    def __post_init__(self):
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.logs_dir = self.base_dir / "logs"

        self._create_base_dirs()

    def _create_base_dirs(self):
        for d in [
            self.data_dir,
            self.raw_dir,
            self.processed_dir,
            self.logs_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)

    # Optional yearly partition
    def get_year_raw_dir(self, year: int) -> Path:
        if not self.use_year_partition:
            return self.raw_dir

        year_dir = self.raw_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        return year_dir
