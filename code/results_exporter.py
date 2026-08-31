# =============================================================================
# results_exporter.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Results Exporter
#
# Exports simulation results for research and publication.
# =============================================================================

from pathlib import Path
from typing import Optional
import json
import csv

from comparison_engine import ComparisonEngine
from logger import logger


# =============================================================================
# RESULTS EXPORTER
# =============================================================================

class ResultsExporter:
    """
    Exports comparison results into multiple research-friendly formats.

    Supported Formats
    -----------------
    • CSV
    • JSON
    • Excel (future extension)

    The exporter only writes data produced by ComparisonEngine.
    """

    def __init__(
        self,
        comparison_engine: ComparisonEngine,
        output_directory: Optional[str] = None
    ):

        self.engine = comparison_engine

        if output_directory is None:
            self.output_directory = Path(__file__).resolve().parent.parent / "results" / "files"
        else:
            self.output_directory = Path(output_directory)

        self.output_directory.mkdir(

            parents=True,

            exist_ok=True

        )

    # ------------------------------------------------------------------

    def output_path(self):

        return self.output_directory

    # ------------------------------------------------------------------

    def comparison_table(self):

        """
        Returns the complete comparison table.
        """

        return self.engine.generate_comparison_table()

    # ------------------------------------------------------------------

    def improvement_table(self):

        """
        Returns improvement percentages.
        """

        return self.engine.generate_improvement_matrix()

    # ------------------------------------------------------------------

    def summary(self):

        """
        Returns research summary.
        """

        return self.engine.research_summary()

    # ------------------------------------------------------------------

    def clear_directory(self):
        """
        Removes previously exported files.
        """

        for file in self.output_directory.glob("*"):

            if file.is_file():

                file.unlink()

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"ResultsExporter("

            f"output='{self.output_directory}')"

        )


        # ------------------------------------------------------------------
    # EXPORT COMPARISON TABLE (CSV)
    # ------------------------------------------------------------------

    def export_comparison_csv(
        self,
        filename: str = "comparison_results.csv"
    ):
        """
        Exports the comparison table to CSV.
        """

        table = self.comparison_table()

        filepath = self.output_directory / filename

        modes = []

        if table:

            first_metric = next(iter(table.values()))

            modes = list(first_metric.keys())

        with open(
            filepath,
            "w",
            newline=""
        ) as csvfile:

            writer = csv.writer(csvfile)

            writer.writerow(

                ["Metric"] + modes

            )

            for metric, values in table.items():

                row = [

                    metric

                ]

                for mode in modes:

                    row.append(

                        values.get(mode)

                    )

                writer.writerow(row)

        return filepath

    # ------------------------------------------------------------------
    # EXPORT IMPROVEMENT MATRIX (CSV)
    # ------------------------------------------------------------------

    def export_improvement_csv(
        self,
        filename: str = "improvement_matrix.csv"
    ):
        """
        Exports percentage improvements to CSV.
        """

        matrix = self.improvement_table()

        filepath = self.output_directory / filename

        with open(
            filepath,
            "w",
            newline=""
        ) as csvfile:

            writer = csv.writer(csvfile)

            first_mode = next(iter(matrix.values()), {})

            metrics = list(first_mode.keys())

            writer.writerow(

                ["Communication Mode"] +

                metrics

            )

            for mode, values in matrix.items():

                row = [

                    mode

                ]

                for metric in metrics:

                    row.append(

                        values.get(metric)

                    )

                writer.writerow(row)

        return filepath

    # ------------------------------------------------------------------
    # EXPORT RESEARCH SUMMARY (JSON)
    # ------------------------------------------------------------------

    def export_summary_json(
        self,
        filename: str = "research_summary.json"
    ):
        """
        Exports the research summary.
        """

        summary = self.summary()

        filepath = self.output_directory / filename

        with open(
            filepath,
            "w"
        ) as file:

            json.dump(

                summary,

                file,

                indent=4,

                default=str

            )

        return filepath



        # ------------------------------------------------------------------
    # EXPORT COMPARISON TABLE (JSON)
    # ------------------------------------------------------------------

    def export_comparison_json(
        self,
        filename: str = "comparison_results.json"
    ):
        """
        Exports the comparison table as JSON.
        """

        filepath = self.output_directory / filename

        with open(
            filepath,
            "w"
        ) as file:

            json.dump(

                self.comparison_table(),

                file,

                indent=4

            )

        return filepath

    # ------------------------------------------------------------------
    # EXPORT IMPROVEMENT MATRIX (JSON)
    # ------------------------------------------------------------------

    def export_improvement_json(
        self,
        filename: str = "improvement_matrix.json"
    ):
        """
        Exports the improvement matrix as JSON.
        """

        filepath = self.output_directory / filename

        with open(
            filepath,
            "w"
        ) as file:

            json.dump(

                self.improvement_table(),

                file,

                indent=4

            )

        return filepath

    # ------------------------------------------------------------------
    # EXPORT TIME-SERIES TRAJECTORIES (JSON & CSV)
    # ------------------------------------------------------------------

    def export_timeseries_json(
        self,
        filename: str = "simulation_timeseries.json"
    ):
        """
        Exports the raw empirical time-series recorded during simulation for all modes.
        """
        ts_data = {}
        for mode, res in self.engine.results.items():
            mode_name = mode.value if hasattr(mode, "value") else str(mode)
            if res.raw_stats and "time_series" in res.raw_stats:
                ts_data[mode_name] = res.raw_stats["time_series"]

        filepath = self.output_directory / filename
        with open(filepath, "w") as file:
            json.dump(ts_data, file, indent=2)

        return filepath

    def export_timeseries_csv(
        self,
        filename: str = "simulation_timeseries.csv"
    ):
        """
        Exports the complete time-series snapshot logs into CSV format for research auditing.
        """
        filepath = self.output_directory / filename
        fieldnames = [
            "mode", "time", "nodes", "generated", "normal_generated", "emergency_generated",
            "delivered", "normal_delivered", "emergency_delivered",
            "dropped", "emergency_dropped", "collisions", "collisions_prevented",
            "pdr", "normal_pdr", "emergency_pdr", "retries",
            "channel_freq_mhz", "spreading_factor", "avg_ati",
            "hold_decisions", "fastpath_acks",
            "energy_kj", "energy_per_delivered_mj", "avg_delay_ms", "goodput_bps", "queued"
        ]

        with open(filepath, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()

            for mode, res in self.engine.results.items():
                mode_name = mode.value if hasattr(mode, "value") else str(mode)
                if res.raw_stats and "time_series" in res.raw_stats:
                    for row in res.raw_stats["time_series"]:
                        row_copy = dict(row)
                        row_copy["mode"] = mode_name
                        writer.writerow(row_copy)

        return filepath

    # ------------------------------------------------------------------
    # EXPORT EVERYTHING
    # ------------------------------------------------------------------

    def export_all(self):
        """
        Exports every available result.
        """

        exported_files = []

        exported_files.append(
            self.export_comparison_csv()
        )

        exported_files.append(
            self.export_improvement_csv()
        )

        exported_files.append(
            self.export_summary_json()
        )

        exported_files.append(
            self.export_comparison_json()
        )

        exported_files.append(
            self.export_improvement_json()
        )

        exported_files.append(
            self.export_timeseries_json()
        )

        exported_files.append(
            self.export_timeseries_csv()
        )

        logger.info("==================================================")
        logger.info("        RESULTS EXPORTED SUCCESSFULLY")
        logger.info("==================================================")
        for file in exported_files:
            logger.info(f"Exported file: {file}")
        logger.info("==================================================")

        return exported_files

    # ------------------------------------------------------------------

    def file_count(self):
        """
        Returns the number of exported files.
        """

        return len(

            list(

                self.output_directory.glob("*")

            )

        )

    # ------------------------------------------------------------------

    def exported_files(self):
        """
        Returns a list of exported result files.
        """

        return sorted(

            list(

                self.output_directory.glob("*")

            )

        )