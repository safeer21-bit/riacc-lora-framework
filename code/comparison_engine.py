# =============================================================================
# comparison_engine.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Comparison Engine
#
# Compares different LoRa communication strategies.
# =============================================================================

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Any

from performance_metrics import PerformanceMetrics


# =============================================================================
# COMMUNICATION MODES
# =============================================================================

class CommunicationMode(Enum):
    """
    Communication modes evaluated in this research.
    """

    PURE_ALOHA = "Pure ALOHA"

    PURE_ALOHA_RIACC = "Pure ALOHA + RIACC"

    CLASS_A_ADR = "LoRaWAN Class A"

    CLASS_A_ADR_RIACC = "LoRaWAN Class A + RIACC"




# =============================================================================
# COMPARISON RESULT
# =============================================================================

@dataclass
class ComparisonResult:

    mode: CommunicationMode

    metrics: PerformanceMetrics

    raw_stats: Optional[Dict[str, Any]] = None


# =============================================================================
# COMPARISON ENGINE
# =============================================================================

class ComparisonEngine:
    """
    Compares the performance of different LoRa communication modes.

    Primary Evaluated Scenarios (4 Modes)
    --------------------------------------
    • Unslotted ALOHA (Baseline)
    • Unslotted ALOHA + RIACC (Proposed)
    • LoRaWAN Class A with ADR (Baseline)
    • LoRaWAN Class A + RIACC (Proposed)

    Note: Class B support is in the theoretical RIACC specification but is not
    wired into the 4-mode comparison pipeline for this research paper.
    """

    def __init__(self):

        self.results: Dict[
            CommunicationMode,
            ComparisonResult
        ] = {}

    # ------------------------------------------------------------------

    def add_result(
        self,
        mode: CommunicationMode,
        metrics: PerformanceMetrics,
        raw_stats: Optional[Dict[str, Any]] = None
    ):

        self.results[mode] = ComparisonResult(

            mode=mode,

            metrics=metrics,

            raw_stats=raw_stats

        )

    # ------------------------------------------------------------------

    def has_result(
        self,
        mode: CommunicationMode
    ) -> bool:

        return mode in self.results

    # ------------------------------------------------------------------

    def get_result(
        self,
        mode: CommunicationMode
    ) -> Optional[ComparisonResult]:

        return self.results.get(mode)

    # ------------------------------------------------------------------

    def available_modes(self):

        return list(self.results.keys())

    # ------------------------------------------------------------------

    def clear(self):

        self.results.clear()

    # ------------------------------------------------------------------

    def count(self):

        return len(self.results)

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"ComparisonEngine("
            f"Modes={len(self.results)})"

        )


        # ------------------------------------------------------------------
    # METRIC COMPARISON
    # ------------------------------------------------------------------

    def compare_metric(
        self,
        metric_name: str
    ) -> Dict[str, float]:
        """
        Returns the value of one metric for every communication mode.
        """

        comparison = {}

        for mode, result in self.results.items():

            if hasattr(result.metrics, metric_name):

                comparison[mode.value] = getattr(
                    result.metrics,
                    metric_name
                )

        return comparison

    # ------------------------------------------------------------------

    def generate_comparison_table(self):
        """
        Generates the complete comparison table including computed metrics
        and raw empirical event counts from raw_stats for full data traceability.
        """

        metrics = [
            "packet_delivery_ratio",
            "packet_loss_ratio",
            "collision_rate",
            "throughput",
            "average_waiting_time",
            "average_queue_length",
            "average_rssi",
            "average_snr",
            "average_ati",
            "energy_per_packet",
            "channel_utilization",
            "emergency_delivery_ratio",
            "scheduler_efficiency"
        ]

        table = {}

        for metric in metrics:
            table[metric] = self.compare_metric(metric)

        # ------------------------------------------------------------------
        # Append raw event counts extracted directly from raw_stats.
        # These feed graph generators and paper tables without hardcoding.
        # ------------------------------------------------------------------
        raw_count_keys = {
            "total_generated":       lambda rs: rs.get("generated", rs.get("total_generated", 0)),
            "total_delivered":       lambda rs: rs.get("channel", {}).get("delivered", 0),
            "total_collisions":      lambda rs: rs.get("channel", {}).get("collisions", 0),
            "total_dropped":         lambda rs: rs.get("channel", {}).get("dropped", 0),
            "emergency_generated":   lambda rs: rs.get("emergency_generated", 0),
            "emergency_delivered":   lambda rs: rs.get("emergency_delivered", 0),
            "emergency_dropped":     lambda rs: max(0, rs.get("emergency_generated", 0) - rs.get("emergency_delivered", 0)),
            "normal_delivered":      lambda rs: max(0, rs.get("channel", {}).get("delivered", 0) - rs.get("emergency_delivered", 0)),
            "collisions_prevented":  lambda rs: rs.get("collisions_prevented", 0),
            "simulation_time_s":     lambda rs: rs.get("simulation_time", 0.0),
            "energy_total_mj":       lambda rs: rs.get("energy_sum", 0.0),
            "avg_delay_ms":          lambda rs: (rs.get("wait_sum", 0.0) / max(1, rs.get("channel", {}).get("delivered", 1))) * 1000.0,
            "total_retransmissions": lambda rs: rs.get("total_retransmissions", 0),
            "hold_decisions":        lambda rs: rs.get("master_gateway", {}).get("hold_decisions_issued", 0),
            "fastpath_acks":         lambda rs: rs.get("master_gateway", {}).get("emergency_fastpath_acks", 0),
        }

        for key, extractor in raw_count_keys.items():
            row = {}
            for mode, result in self.results.items():
                rs = result.raw_stats or {}
                try:
                    row[mode.value] = extractor(rs)
                except Exception:
                    row[mode.value] = 0
            table[key] = row

        return table

    # ------------------------------------------------------------------

    def get_best_mode(
        self,
        metric_name: str,
        higher_is_better: bool = True
    ):
        """
        Returns the communication mode with the best value.
        """

        values = self.compare_metric(metric_name)

        if len(values) == 0:

            return None

        if higher_is_better:

            return max(
                values.items(),
                key=lambda x: x[1]
            )

        return min(
            values.items(),
            key=lambda x: x[1]
        )

    # ------------------------------------------------------------------

    def improvement(
        self,
        baseline: CommunicationMode,
        proposed: CommunicationMode,
        metric_name: str
    ) -> float:
        """
        Percentage improvement of proposed over baseline.
        """

        if baseline not in self.results:
            # Return NaN so callers can distinguish "genuinely not computed"
            # from a real 0.00% improvement result.
            # research_validation.py guards against NaN via its own base_val/prop_val checks.
            return float("nan")

        if proposed not in self.results:
            return float("nan")

        base = getattr(

            self.results[baseline].metrics,

            metric_name

        )

        prop = getattr(

            self.results[proposed].metrics,

            metric_name

        )

        if base == 0:
            # Genuine zero-baseline: return NaN, not 0.0, so callers know
            # this comparison cannot be normalized.
            return float("nan")

        return ((prop - base) / base) * 100.0


        # ------------------------------------------------------------------
    # IMPROVEMENT MATRIX
    # ------------------------------------------------------------------

    def generate_improvement_matrix(self):
        """
        Generates percentage improvements of every communication mode
        relative to the Unslotted ALOHA baseline.
        """

        baseline = CommunicationMode.PURE_ALOHA

        if baseline not in self.results:

            return {}

        metrics = [

            "packet_delivery_ratio",

            "packet_loss_ratio",

            "collision_rate",

            "throughput",

            "average_waiting_time",

            "average_queue_length",

            "energy_per_packet",

            "channel_utilization",

            "emergency_delivery_ratio"

        ]

        # Direction map: metrics where lower value = improvement.
        # Sign is flipped ONLY in the exported improvement_matrix CSV/JSON so that
        # positive percentages uniformly represent improvement across all metrics.
        # NOTE: improvement() itself is NOT sign-flipped here because
        # research_validation.py already applies its own -improvement negation
        # at line 203 for lower-is-better metrics. Double-negating would invert
        # all 8 research hypothesis pass/fail results.
        LOWER_IS_BETTER = {
            "packet_loss_ratio", "collision_rate",
            "average_waiting_time", "energy_per_packet"
        }

        matrix = {}

        for mode in self.results:

            if mode == baseline:

                continue

            matrix[mode.value] = {}

            for metric in metrics:

                raw = self.improvement(baseline, mode, metric)
                if raw != raw:  # NaN check (float("nan") != float("nan"))
                    display_val = float("nan")
                elif metric in LOWER_IS_BETTER:
                    display_val = -raw  # Flip: a reduction now shows as positive
                else:
                    display_val = raw

                matrix[mode.value][metric] = round(display_val, 2) if display_val == display_val else float("nan")

        return matrix

    # ------------------------------------------------------------------
    # RESEARCH SUMMARY
    # ------------------------------------------------------------------

    def research_summary(self):
        """
        Generates a concise summary suitable for research reporting.
        """

        if len(self.results) == 0:

            return {}

        summary = {

            "best_packet_delivery_ratio":

                self.get_best_mode(

                    "packet_delivery_ratio",

                    True

                ),

            "lowest_collision_rate":

                self.get_best_mode(

                    "collision_rate",

                    False

                ),

            "highest_throughput":

                self.get_best_mode(

                    "throughput",

                    True

                ),

            "lowest_waiting_time":

                self.get_best_mode(

                    "average_waiting_time",

                    False

                ),

            "lowest_energy_per_packet":

                self.get_best_mode(

                    "energy_per_packet",

                    False

                ),

            "highest_channel_utilization":

                self.get_best_mode(

                    "channel_utilization",

                    True

                ),

            "highest_emergency_delivery":

                self.get_best_mode(

                    "emergency_delivery_ratio",

                    True

                )

        }

        return summary

    # ------------------------------------------------------------------
    # PRINT COMPARISON
    # ------------------------------------------------------------------

    def print_summary(self):
        """
        Prints a formatted comparison report.
        """

        print("\n===================================================")
        print("        RIACC COMMUNICATION MODE COMPARISON")
        print("===================================================\n")

        table = self.generate_comparison_table()

        for metric, values in table.items():

            print(metric.replace("_", " ").title())

            for mode, value in values.items():

                if isinstance(value, float):

                    print(f"   {mode:30} : {value:.3f}")

                else:

                    print(f"   {mode:30} : {value}")

            print()

        print("===================================================")

    # ------------------------------------------------------------------
    # PRINT IMPROVEMENTS
    # ------------------------------------------------------------------

    def print_improvements(self):
        """
        Prints improvement percentages relative to Unslotted ALOHA.
        """

        matrix = self.generate_improvement_matrix()

        print("\n===================================================")
        print("        IMPROVEMENT OVER PURE ALOHA")
        print("===================================================\n")

        for mode, metrics in matrix.items():

            print(mode)

            for metric, value in metrics.items():

                print(

                    f"   {metric:30}"

                    f": {value:.2f}%"

                )

            print()

        print("===================================================")

    # ------------------------------------------------------------------
    # PRINT COMPARATIVE METRICS TABLE
    # ------------------------------------------------------------------

    def print_comparative_table(self):
        """
        Prints a single unified comparative table comparing all 4 communication
        modes across all core research metrics.
        """
        if not self.results:
            print("\n  [No comparative results available]\n")
            return

        print("\n" + "=" * 145)
        print("  EXPERIMENTAL SCENARIO COMPARATIVE PERFORMANCE TABLE  (All 4 Communication Modes)")
        print("=" * 145)

        header = (
            f"{'Communication Mode':<28} | {'PDR (%)':>8} | {'Collision(%)':>12} | "
            f"{'Loss (%)':>8} | {'Emg PDR(%)':>10} | {'Wait Delay(s)':>13} | "
            f"{'Energy(mJ)':>10} | {'Avg ATI':>8} | {'Throughput':>10}"
        )
        print(header)
        print("-" * 145)

        for mode, result in self.results.items():
            m = result.metrics
            mode_name = mode.value

            pdr = getattr(m, "packet_delivery_ratio", 0.0)
            col = getattr(m, "collision_rate", 0.0)
            loss = getattr(m, "packet_loss_ratio", 0.0)
            emg_pdr = getattr(m, "emergency_delivery_ratio", 0.0)
            wait_delay = getattr(m, "average_waiting_time", 0.0)
            energy = getattr(m, "energy_per_packet", 0.0)
            ati = getattr(m, "average_ati", 0.0)
            tp = getattr(m, "throughput", 0.0)

            row = (
                f"{mode_name:<28} | {pdr:>8.2f} | {col:>12.2f} | "
                f"{loss:>8.2f} | {emg_pdr:>10.2f} | {wait_delay:>13.4f} | "
                f"{energy:>10.2f} | {ati:>8.2f} | {tp:>10.2f}"
            )
            print(row)

        print("=" * 145 + "\n")



        