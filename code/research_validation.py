# =============================================================================
# research_validation.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Research Validation
#
# Validates experimental results against the research objectives.
# =============================================================================

from dataclasses import dataclass
from typing import List

from comparison_engine import (
    ComparisonEngine,
    CommunicationMode
)
from logger import logger


# =============================================================================
# VALIDATION RESULT
# =============================================================================

@dataclass
class ValidationResult:
    """
    Represents one validated research objective.
    """

    objective: str

    metric: str

    baseline: str

    proposed: str

    improvement: float

    passed: bool


# =============================================================================
# RESEARCH VALIDATOR
# =============================================================================

class ResearchValidator:
    """
    Validates whether the proposed RIACC framework
    achieves the research objectives.

    Experimental Scenarios
    ----------------------
    1. Pure ALOHA

    2. Pure ALOHA + RIACC

    3. LoRaWAN Class A

    4. LoRaWAN Class A + RIACC
    """

    def __init__(
        self,
        comparison_engine: ComparisonEngine
    ):

        self.engine = comparison_engine

        self.results: List[ValidationResult] = []

    # ------------------------------------------------------------------

    def clear(self):

        self.results.clear()

    # ------------------------------------------------------------------

    def add_result(

        self,

        objective: str,

        metric: str,

        baseline: str,

        proposed: str,

        improvement: float,

        passed: bool

    ):

        self.results.append(

            ValidationResult(

                objective=objective,

                metric=metric,

                baseline=baseline,

                proposed=proposed,

                improvement=improvement,

                passed=passed

            )

        )

    # ------------------------------------------------------------------

    def validation_results(self):

        return self.results

    # ------------------------------------------------------------------

    def total_tests(self):

        return len(self.results)

    # ------------------------------------------------------------------

    def passed_tests(self):

        return sum(

            result.passed

            for result in self.results

        )

    # ------------------------------------------------------------------

    def failed_tests(self):

        return (

            self.total_tests()

            -

            self.passed_tests()

        )

    # ------------------------------------------------------------------

    def reset(self):

        self.clear()

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"ResearchValidator("

            f"tests={len(self.results)})"

        )


        # ------------------------------------------------------------------
    # RESEARCH OBJECTIVE VALIDATION
    # ------------------------------------------------------------------

    def validate_objective(
        self,
        objective: str,
        metric: str,
        baseline: CommunicationMode,
        proposed: CommunicationMode,
        threshold: float = 2.0,
        higher_is_better: bool = True
    ):
        """
        Validates a single research objective requiring meaningful improvement (> threshold).
        """

        improvement = self.engine.improvement(
            baseline,
            proposed,
            metric
        )

        base_val = getattr(self.engine.results.get(baseline, None).metrics, metric, 0.0) if baseline in self.engine.results else 0.0
        prop_val = getattr(self.engine.results.get(proposed, None).metrics, metric, 0.0) if proposed in self.engine.results else 0.0

        if not higher_is_better:

            improvement = -improvement

        # Check for strict improvement beyond threshold, or >= 95% delivery for emergency ratio
        if base_val == 0.0 and prop_val == 0.0:
            passed = False
        elif metric == "emergency_delivery_ratio" and prop_val >= 95.0:
            passed = True
        else:
            passed = improvement > threshold

        self.add_result(

            objective=objective,

            metric=metric,

            baseline=baseline.value,

            proposed=proposed.value,

            improvement=round(improvement, 2),

            passed=passed

        )

    # ------------------------------------------------------------------
    # VALIDATE ALL RESEARCH QUESTIONS
    # ------------------------------------------------------------------

    def validate_all(self):
        """
        Validates all research objectives.
        """

        self.clear()

        # ==========================================================
        # PURE ALOHA → PURE ALOHA + RIACC
        # ==========================================================

        self.validate_objective(

            objective="RQ1: Improve Packet Delivery Ratio",

            metric="packet_delivery_ratio",

            baseline=CommunicationMode.PURE_ALOHA,

            proposed=CommunicationMode.PURE_ALOHA_RIACC,

            threshold=2.0,

            higher_is_better=True

        )

        self.validate_objective(

            objective="RQ2: Reduce Collision Rate",

            metric="collision_rate",

            baseline=CommunicationMode.PURE_ALOHA,

            proposed=CommunicationMode.PURE_ALOHA_RIACC,

            threshold=2.0,

            higher_is_better=False

        )

        self.validate_objective(

            objective="RQ3: Improve Throughput",

            metric="throughput",

            baseline=CommunicationMode.PURE_ALOHA,

            proposed=CommunicationMode.PURE_ALOHA_RIACC,

            threshold=2.0,

            higher_is_better=True

        )

        self.validate_objective(

            objective="RQ4: Reduce Energy Consumption",

            metric="energy_per_packet",

            baseline=CommunicationMode.PURE_ALOHA,

            proposed=CommunicationMode.PURE_ALOHA_RIACC,

            threshold=2.0,

            higher_is_better=False

        )

        # ==========================================================
        # CLASS A → CLASS A + RIACC
        # ==========================================================

        self.validate_objective(

            objective="RQ5: Improve Class A Packet Delivery",

            metric="packet_delivery_ratio",

            baseline=CommunicationMode.CLASS_A_ADR,

            proposed=CommunicationMode.CLASS_A_ADR_RIACC,

            threshold=2.0,

            higher_is_better=True

        )

        self.validate_objective(

            objective="RQ6: Reduce Class A Collisions",

            metric="collision_rate",

            baseline=CommunicationMode.CLASS_A_ADR,

            proposed=CommunicationMode.CLASS_A_ADR_RIACC,

            threshold=2.0,

            higher_is_better=False

        )

        self.validate_objective(

            objective="RQ7: Improve Class A Throughput",

            metric="throughput",

            baseline=CommunicationMode.CLASS_A_ADR,

            proposed=CommunicationMode.CLASS_A_ADR_RIACC,

            threshold=2.0,

            higher_is_better=True

        )

        self.validate_objective(

            objective="RQ8: Improve Emergency Packet Delivery",

            metric="emergency_delivery_ratio",

            baseline=CommunicationMode.CLASS_A_ADR,

            proposed=CommunicationMode.CLASS_A_ADR_RIACC,

            threshold=2.0,

            higher_is_better=True

        )


        return self.results




        # ------------------------------------------------------------------
    # VALIDATION SUMMARY
    # ------------------------------------------------------------------

    def validation_summary(self):
        """
        Returns overall validation statistics.
        """

        total = self.total_tests()

        passed = self.passed_tests()

        failed = self.failed_tests()

        success_rate = 0.0

        if total > 0:

            success_rate = (passed / total) * 100.0

        return {

            "total_tests": total,

            "passed_tests": passed,

            "failed_tests": failed,

            "success_rate": round(success_rate, 2)

        }

    # ------------------------------------------------------------------
    # RESEARCH CONCLUSION
    # ------------------------------------------------------------------

    def research_conclusion(self):
        """
        Generates the final research conclusion.
        """

        summary = self.validation_summary()

        if summary["total_tests"] == 0:

            return "No validation has been performed."

        if summary["passed_tests"] == summary["total_tests"]:

            return (
                "All research objectives were successfully achieved. "
                "The RIACC framework consistently improves communication "
                "performance compared to the corresponding baseline systems."
            )

        elif summary["passed_tests"] >= summary["total_tests"] * 0.75:

            return (
                "Most research objectives were achieved. "
                "RIACC demonstrates significant improvements for the majority "
                "of evaluated performance metrics."
            )

        elif summary["passed_tests"] >= summary["total_tests"] * 0.50:

            return (
                "RIACC provides partial improvements. "
                "Further optimization is recommended before deployment."
            )

        else:

            return (
                "The proposed framework does not sufficiently satisfy the "
                "research objectives and requires further refinement."
            )

    # ------------------------------------------------------------------
    # PRINT REPORT
    # ------------------------------------------------------------------

    def print_report(self):
        """
        Saves the Research Objective Validation Report to results/ directory instead of printing to console.
        """
        from pathlib import Path
        import json

        results_dir = Path(__file__).resolve().parent.parent / "results" / "files"
        results_dir.mkdir(parents=True, exist_ok=True)

        lines = [
            "==============================================================",
            "             RIACC RESEARCH VALIDATION REPORT",
            "==============================================================",
            ""
        ]

        for result in self.results:
            status = "PASS" if result.passed else "FAIL"
            lines.append(f"Objective   : {result.objective}")
            lines.append(f"Metric      : {result.metric}")
            lines.append(f"Baseline    : {result.baseline}")
            lines.append(f"Proposed    : {result.proposed}")
            lines.append(f"Improvement : {result.improvement:.2f}%")
            lines.append(f"Result      : {status}\n")

        summary = self.validation_summary()
        lines.append("--------------------------------------------------------------")
        lines.append(f"Total Tests : {summary['total_tests']}")
        lines.append(f"Passed      : {summary['passed_tests']}")
        lines.append(f"Failed      : {summary['failed_tests']}")
        lines.append(f"Success     : {summary['success_rate']:.2f}%")
        lines.append("--------------------------------------------------------------\n")
        lines.append("Research Conclusion")
        lines.append("-------------------")
        lines.append(self.research_conclusion())
        lines.append("==============================================================")

        txt_path = results_dir / "research_validation_report.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        json_path = results_dir / "research_validation_report.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({
                "summary": summary,
                "conclusion": self.research_conclusion(),
                "results": self.export()
            }, f, indent=2)

        logger.info(f"  [Report] Saved RQ Validation Report to: {txt_path}")

    # ------------------------------------------------------------------

    def export(self):
        """
        Returns validation results as dictionaries.
        """

        return [

            {

                "objective": result.objective,

                "metric": result.metric,

                "baseline": result.baseline,

                "proposed": result.proposed,

                "improvement": result.improvement,

                "passed": result.passed

            }

            for result in self.results

        ]