from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Tuple, ClassVar

@dataclass
class FetchConfig:
    base_dir: Path
    use_year_partition: bool = False

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

    #Lot size
    DEFAULT_LOT_PERIODS: ClassVar [Dict[str, List[Tuple[str, Optional[str], int]]]] = {
        "NIFTY": [
            ("2019-01-01", "2021-04-29", 75),
            ("2021-04-30", "2024-04-25", 50),
            ("2024-04-26", "2024-11-19", 25),
            ("2024-11-20", "2025-10-27", 75),
            ("2025-10-28", None, 65),
        ],
        "BANKNIFTY": [
            ("2019-01-01", "2020-05-03", 20),
            ("2020-05-04", "2023-04-27", 25),
            ("2023-04-28", "2024-11-19", 15),
            ("2024-11-20", "2025-04-24", 30),
            ("2025-04-25", "2025-10-27", 35),
            ("2025-10-28", None, 30),
        ],
        "FINNIFTY": [
            ("2021-01-11", "2024-04-25", 40),
            ("2024-04-26", "2024-11-19", 25),
            ("2024-11-20", "2025-10-27", 65),
            ("2025-10-28", None, 60),
        ],
        "MIDCPNIFTY": [
            ("2022-01-24", "2024-04-25", 75),
            ("2024-04-26", "2024-11-19", 50),
            ("2024-11-20", "2025-04-24", 120),
            ("2025-04-25", "2025-10-27", 140),
            ("2025-10-28", None, 120),
        ],
    }

    def __post_init__(self):
        self.data_dir = self.base_dir / "data"
        self.ingest_dir = self.data_dir / "ingest"
        self.processed_dir = self.data_dir / "processed"
        self.logs_dir = self.base_dir / "logs"
        self._create_base_dirs()

    def _create_base_dirs(self):
        for d in [self.data_dir, self.ingest_dir, self.logs_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def get_year_ingest_dir(self, namespace: str ,year: Optional[int] = None) -> Path:
        base_path = self.ingest_dir/namespace
        base_path.mkdir(parents=True,exist_ok=True)

        if self.use_year_partition and year is not None:
            year_path = base_path/str(year)
            year_path.mkdir(parents=True,exist_ok=True)
            return year_path
        return base_path
