from dataclasses import dataclass, field
from pathlib import Path
from typing import List

@dataclass
class FetchConfig:
    base_dir: Path
    use_year_partition: bool = False
    master_derivatives_filename: str = "Historical_Derivatives.csv"

    # Derivatives symbols
    derivatives_symbols: List[str] = field(
        default_factory=lambda: [
            "NIFTY",
            "BANKNIFTY",
            "FINNIFTY",
            "MIDCPNIFTY",
        ]
    )

    # Index symbols
    index_names: List[str] = field(
        default_factory=lambda: [
            "Nifty 50",
            "Nifty Bank",
            "Nifty Financial Services",
            "Nifty Midcap Select",
        ]
    )

    # Yield symbols
    yield_names: List[str] = field(
        default_factory=lambda: [
            "Nifty 50",
            "NIFTY BANK",
            "NIFTY FIN SERVICE",
            "NIFTY MID SELECT",
        ]
    )

    def __post_init__(self):
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.logs_dir = self.base_dir / "logs"
        self._create_base_dirs()

    def _create_base_dirs(self):
        for d in [self.data_dir, self.raw_dir, self.processed_dir, self.logs_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def get_year_raw_dir(self, year: int) -> Path:
        if not self.use_year_partition:
            return self.raw_dir
        year_dir = self.raw_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        return year_dir
