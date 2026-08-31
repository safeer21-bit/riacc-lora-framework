"""
===============================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
-------------------------------------------------------------------------------
Dataset Loader

Loads raw datasets and converts each record into DatasetEvent objects.

Responsibilities
----------------
• Load CSV dataset
• Validate required columns
• Create DatasetEvent objects
• No feature engineering
• No scheduling logic

Research Project:
    A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)

Author:
    Safeer Shah

Version:
    1.0
===============================================================================
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from config import PAYLOAD_SIZE_COLUMN
from logger import logger
from models import DatasetEvent


class DatasetLoader:
    """
    Generic CSV dataset loader.
    """

    def __init__(self, dataset_path: str | Path):

        self.dataset_path = Path(dataset_path)

        self.dataframe = pd.DataFrame()

        self.events: List[DatasetEvent] = []

    def load_csv(self, nrows: Optional[int] = None) -> pd.DataFrame:
        """
        Load dataset from CSV file safely without RAM spikes.
        """

        logger.info(f"Loading dataset: {self.dataset_path}")

        self.dataframe = pd.read_csv(
            self.dataset_path,
            nrows=nrows,
            engine="c",
            on_bad_lines="skip"
        )

        logger.info(
            f"Dataset loaded successfully "
            f"({len(self.dataframe)} rows)"
        )

        return self.dataframe

    @classmethod
    def load_all_directory(cls, directory_path: str | Path) -> List[Tuple[Path, int]]:
        """
        Automatically discover all CSV dataset files in numerical natural order
        (Network_dataset_1.csv -> Network_dataset_23.csv), count events/packets, and display full summary.
        Returns a list of tuples: [(file_path, packet_count), ...]
        """
        import re

        dir_path = Path(directory_path)
        if not dir_path.exists():
            raise FileNotFoundError(f"Dataset directory not found: {dir_path}")

        def natural_sort_key(path: Path) -> int:
            numbers = re.findall(r"\d+", path.name)
            return int(numbers[0]) if numbers else 0

        raw_csv_files = sorted(list(dir_path.glob("*.csv")), key=natural_sort_key)

        file_info: List[Tuple[Path, int]] = []
        total_packets = 0

        logger.info("=" * 70)
        logger.info(f"AUTOMATIC DATASET DIRECTORY SCANNER: {dir_path}")
        logger.info("=" * 70)

        for idx, file_path in enumerate(raw_csv_files, start=1):
            try:
                # Fast row count without loading full memory
                with open(file_path, "r", encoding="utf-8") as f:
                    row_count = max(0, sum(1 for _ in f) - 1)  # Subtract header row
            except Exception:
                row_count = 0

            file_info.append((file_path, row_count))
            total_packets += row_count

            logger.info(
                f"  Dataset [{idx:>2}/{len(raw_csv_files)}]: {file_path.name:<25} -> {row_count:>10,} Packets/Events"
            )

        logger.info("=" * 70)
        logger.info(
            f"TOTAL AUTOMATICALLY LOADED: {len(file_info)} Files | {total_packets:,} Cumulative Packets/Events"
        )
        logger.info("=" * 70)

        return file_info

    def validate_columns(
        self,
        required_columns: List[str],
    ) -> bool:
        """
        Validate that all required columns exist.
        """

        if self.dataframe.empty:
            raise ValueError("Dataset has not been loaded.")

        missing_columns = [
            column
            for column in required_columns
            if column not in self.dataframe.columns
        ]

        if missing_columns:
            logger.error(
                f"Missing required columns: {missing_columns}"
            )
            raise ValueError(
                f"Dataset validation failed. Missing columns: "
                f"{missing_columns}"
            )

        logger.info("Dataset column validation successful.")

        return True


    def clean_dataset(self) -> pd.DataFrame:
        """
        Perform generic dataset cleaning.

        This method intentionally avoids dataset-specific
        preprocessing.
        """

        if self.dataframe.empty:
            raise ValueError("Dataset has not been loaded.")

        initial_rows = len(self.dataframe)

        # Remove duplicate rows
        self.dataframe.drop_duplicates(inplace=True)

        # Reset row indices
        self.dataframe.reset_index(
            drop=True,
            inplace=True,
        )

        removed_rows = initial_rows - len(self.dataframe)

        logger.info(
            f"Dataset cleaned. "
            f"Removed {removed_rows} duplicate rows."
        )

        return self.dataframe


    def shuffle(
        self,
        random_state: int = 42,
    ) -> pd.DataFrame:
        """
        Shuffle dataset rows.
        """

        if self.dataframe.empty:
            raise ValueError("Dataset has not been loaded.")

        self.dataframe = self.dataframe.sample(
            frac=1.0,
            random_state=random_state,
        ).reset_index(drop=True)

        logger.info("Dataset shuffled.")

        return self.dataframe


    def limit_events(
        self,
        limit: int,
    ) -> pd.DataFrame:
        """
        Keep only the first N dataset records.
        """

        if self.dataframe.empty:
            raise ValueError("Dataset has not been loaded.")

        self.dataframe = self.dataframe.head(limit).copy()

        logger.info(
            f"Dataset limited to {len(self.dataframe)} events."
        )

        return self.dataframe

    def create_dataset_events(self) -> List[DatasetEvent]:
        """
        Convert each dataset row into a DatasetEvent object (Optimized Dict Iteration).
        """

        if self.dataframe.empty:
            raise ValueError("Dataset has not been loaded.")

        self.events.clear()

        records = self.dataframe.to_dict("records")

        for row in records:

            event = DatasetEvent(

                timestamp=float(row.get("ts", 0.0) if pd.notnull(row.get("ts")) else 0.0),

                source_ip=str(row.get("src_ip", "") if pd.notnull(row.get("src_ip")) else ""),

                destination_ip=str(row.get("dst_ip", "") if pd.notnull(row.get("dst_ip")) else ""),

                source_port=row.get("src_port") if pd.notnull(row.get("src_port")) else None,

                destination_port=row.get("dst_port") if pd.notnull(row.get("dst_port")) else None,

                protocol=str(row.get("proto", "") if pd.notnull(row.get("proto")) else ""),

                service=str(row.get("service", "") if pd.notnull(row.get("service")) else ""),

                duration=float(row.get("dur", 0.0) if pd.notnull(row.get("dur")) else 0.0),

                payload_size=row.get(PAYLOAD_SIZE_COLUMN) if pd.notnull(row.get(PAYLOAD_SIZE_COLUMN)) else 20,

                response_size=row.get("dbytes") if pd.notnull(row.get("dbytes")) else 0,

                attack_type=str(row.get("type", "normal") if pd.notnull(row.get("type")) else "normal"),

                label=int(row.get("label", 0) if pd.notnull(row.get("label")) else 0),

                raw_record=row,

            )

            self.events.append(event)

        logger.info(
            f"Created {len(self.events):,} DatasetEvent objects."
        )

        return self.events


    def summary(self) -> None:
        """
        Log a summary of the loaded dataset.
        """

        if self.dataframe.empty:
            logger.warning("Dataset is empty.")
            return

        logger.info("=" * 60)
        logger.info("DATASET SUMMARY")
        logger.info("=" * 60)

        logger.info(f"Rows      : {len(self.dataframe)}")
        logger.info(f"Columns   : {len(self.dataframe.columns)}")
        logger.info(f"Events    : {len(self.events)}")

        if "type" in self.dataframe.columns:

            logger.info("Attack Distribution:")

            distribution = (
                self.dataframe["type"]
                .value_counts()
                .to_dict()
            )

            for attack, count in distribution.items():
                logger.info(f"  {attack:<20} {count}")

        logger.info("=" * 60)


    def get_events(self) -> List[DatasetEvent]:
        """
        Return all loaded DatasetEvent objects.
        """

        return self.events.copy()