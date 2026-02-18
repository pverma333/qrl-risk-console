import pandas as pd
import logging
from src.core.fetch_config import FetchConfig

class GbondProcessor:

    FILES = {
        "3m": "3monthbond.csv",
        "6m": "6monthbond.csv",
        "1y": "1yearbond.csv",
    }

    def __init__(self, config: FetchConfig, rebuild: bool = False):
        self.config = config
        self.namespace = "gbond"
        self.rebuild = rebuild
        self.ingest_dir = self.config.get_year_ingest_dir(self.namespace)
        self.log_path = self.config.logs_dir

        logging.basicConfig(
            filename=self.log_path / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )

        self.logger = logging.getLogger("GbondProcessor")

    def build_combined_gbond(self):

        output_path = self.ingest_dir / "gbond_combined.parquet"

        if self.rebuild and output_path.exists():
            output_path.unlink()
            self.logger.info("Rebuild mode: Existing gbond_combined.parquet deleted.")

        self.logger.info("Starting gbond combine process")

        dfs = []

        for tenor, file_name in self.FILES.items():

            file_path = self.ingest_dir / file_name

            if not file_path.exists():
                self.logger.error(f"Missing file: {file_path}")
                raise FileNotFoundError(f"{file_path} not found")

            self.logger.info(f"Reading file: {file_name}")

            df = pd.read_csv(file_path)

            df.columns = df.columns.str.strip().str.lower()

            df["date"] = pd.to_datetime(
                df["date"],
                format="%d-%m-%Y",
                errors="raise"
            )

            df["tenor"] = tenor

            dfs.append(df)

        new_data = (
            pd.concat(dfs, ignore_index=True)
            .drop_duplicates()
            .sort_values(["date", "tenor"])
            .reset_index(drop=True)
        )

        if output_path.exists() and not self.rebuild:
            self.logger.info("Existing combined file found. Appending and deduplicating.")
            existing = pd.read_parquet(output_path)

            combined = pd.concat([existing, new_data], ignore_index=True)

            combined.drop_duplicates(
                subset=["date", "tenor"],
                inplace=True
            )

            combined.sort_values(["date", "tenor"], inplace=True)

        else:
            combined = new_data

        combined.to_parquet(output_path, index=False)

        self.logger.info(
            f"Gbond combined parquet saved successfully at {output_path}"
        )
