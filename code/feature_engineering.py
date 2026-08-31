"""
===============================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
-------------------------------------------------------------------------------
Feature Engineering

Converts raw DatasetEvent objects into intelligent NodeEvent objects.

Responsibilities
----------------
• Attack mapping
• Threat level assignment
• Priority assignment
• Burst score calculation
• Adaptive Threat Index (ATI)
• Create NodeEvent objects

Research Project:
    A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)

Author:
    Safeer Shah

Version:
    1.0
===============================================================================
"""

from __future__ import annotations

from collections import deque
from typing import List

from config import (
    ATTACK_MAPPING,
    Priority,
    ThreatLevel,
    ATI_THREAT_WEIGHT,
    ATI_BURST_WEIGHT,
    ATI_ENERGY_WEIGHT,
    ATI_EVENT_WEIGHT,
)

from logger import logger
from models import DatasetEvent, NodeEvent


class FeatureEngineering:
    """
    Performs feature engineering for the RIACC simulator.

    PDF Chapter 4 Alignment:
    - Burst Score uses temporal event density (events per sliding window)
    - ATI uses multi-parameter weighted fusion (severity, burst, battery,
      waiting, retry, operational context)
    """

    # Sliding window duration for burst detection (seconds)
    BURST_WINDOW_SECONDS = 10.0

    # ATI component weights imported from config.py (sum to 1.0)
    W_SEVERITY  = ATI_THREAT_WEIGHT   # 0.40
    W_BURST     = ATI_BURST_WEIGHT    # 0.30
    W_BATTERY   = ATI_ENERGY_WEIGHT   # 0.20
    W_CONTEXT   = ATI_EVENT_WEIGHT    # 0.10
    W_WAITING   = 0.0
    W_RETRY     = 0.0

    def __init__(self):

        self.processed_events: List[NodeEvent] = []

        # Per-node event history buffer for temporal burst detection
        self._node_event_history: dict = {}  # node_id -> list of timestamps

    def process_batch(
        self,
        events: List[DatasetEvent],
    ) -> List[NodeEvent]:
        """
        Process multiple DatasetEvent objects.
        """

        self.processed_events.clear()

        for event in events:
            self.processed_events.append(
                self.process(event)
            )

        logger.info(
            f"Processed {len(self.processed_events)} events."
        )

        return self.processed_events

    


    # =========================================================================
    # THREAT LEVEL
    # =========================================================================

    def assign_threat_level(
        self,
        attack_type: str,
    ) -> ThreatLevel:
        """
        Assign threat level based on attack type.
        """

        attack = self.map_attack_type(attack_type).lower()

        if attack == "critical":
            return ThreatLevel.CRITICAL

        if attack == "high":
            return ThreatLevel.HIGH

        if attack == "medium":
            return ThreatLevel.MEDIUM

        if attack == "low":
            return ThreatLevel.LOW

        return ThreatLevel.NORMAL


    # =========================================================================
    # PRIORITY
    # =========================================================================

    def assign_priority(
        self,
        threat_level: ThreatLevel,
    ) -> Priority:
        """
        Assign transmission priority.
        """

        priority_map = {
            ThreatLevel.CRITICAL: Priority.EMERGENCY,
            ThreatLevel.HIGH: Priority.HIGH,
            ThreatLevel.MEDIUM: Priority.MEDIUM,
            ThreatLevel.LOW: Priority.LOW,
            ThreatLevel.NORMAL: Priority.LOW,
        }

        return priority_map[threat_level]

    # =========================================================================
    # BURST SCORE (PDF 4.1 — Temporal Event Density)
    # =========================================================================

    def calculate_burst_score(
        self,
        event: DatasetEvent,
    ) -> float:
        """
        Calculate burst score using temporal event density.

        Per PDF Chapter 4.1: The Burst Detection Engine continuously
        compares recent timestamps using a sliding observation window.
        Burst Score = (events_in_window / max_expected_events) * 100
        """

        node_id = event.source_ip or "UNKNOWN"
        ts = event.timestamp or 0.0

        # Update per-node event history buffer using deque for O(1) sliding window
        if node_id not in self._node_event_history:
            self._node_event_history[node_id] = deque()

        history = self._node_event_history[node_id]
        history.append(ts)

        # Prune events outside the sliding window in O(1) amortized
        cutoff = ts - self.BURST_WINDOW_SECONDS
        while history and history[0] < cutoff:
            history.popleft()

        events_in_window = len(history)

        # Normalize: 1 event = 0 burst, 10+ events in window = 100 burst
        max_expected = 10.0
        burst_score = min(100.0, ((events_in_window - 1) / max(1.0, max_expected - 1)) * 100.0)

        return max(0.0, burst_score)

    # =========================================================================
    # ADAPTIVE THREAT INDEX (PDF 4.1 — Multi-Parameter Weighted Fusion)
    # =========================================================================

    def calculate_adaptive_threat_index(
        self,
        threat_level: ThreatLevel,
        burst_score: float,
        battery_pct: float = 100.0,
        waiting_duration: float = 0.0,
        retry_count: int = 0,
        infrastructure_importance: float = 50.0,
    ) -> float:
        """
        Calculate the Adaptive Threat Index using multi-parameter fusion.

        Per PDF Chapter 4.1: ATI = weighted fusion of:
          - Severity (threat level weight)
          - Burst density
          - Battery factor (low battery = higher urgency)
          - Waiting duration (starvation compensation)
          - Retry factor
          - Operational context (infrastructure importance)
        """

        # Severity component (0-100)
        severity_map = {
            ThreatLevel.NORMAL: 20.0,
            ThreatLevel.LOW: 35.0,
            ThreatLevel.MEDIUM: 50.0,
            ThreatLevel.HIGH: 75.0,
            ThreatLevel.CRITICAL: 100.0,
        }
        severity = severity_map.get(threat_level, 20.0)

        # Battery factor: low battery = higher urgency (0-100)
        battery_factor = max(0.0, min(100.0, 100.0 - battery_pct))

        # Waiting factor: longer wait = higher urgency (0-100)
        waiting_factor = min(100.0, waiting_duration * 10.0)

        # Retry factor: more retries = higher urgency (0-100)
        retry_factor = min(100.0, retry_count * 25.0)

        # Context factor: infrastructure importance (0-100)
        context_factor = min(100.0, max(0.0, infrastructure_importance))

        # Multi-parameter weighted fusion
        ati = (
            self.W_SEVERITY * severity
            + self.W_BURST * burst_score
            + self.W_BATTERY * battery_factor
            + self.W_WAITING * waiting_factor
            + self.W_RETRY * retry_factor
            + self.W_CONTEXT * context_factor
        )

        return min(100.0, max(0.0, ati))

    @staticmethod
    def compute_vectorized_ati(
        attack_types: list,
        burst_scores: list | None = None,
        battery_pcts: list | None = None,
        waiting_durations: list | None = None,
        retry_counts: list | None = None,
        infrastructure_importances: list | None = None,
    ) -> list[float]:
        """
        High-performance vectorized ATI calculation across a list of raw event attributes.
        Per PDF Chapter 4.1: ATI = weighted fusion of:
          - Severity (35%)
          - Burst density (20%)
          - Battery factor (10%)
          - Waiting duration (10%)
          - Retry factor (5%)
          - Operational context (20%)
        """
        sev_map = {'normal': 20.0, 'low': 35.0, 'medium': 50.0, 'high': 75.0, 'critical': 100.0}
        n = len(attack_types)
        
        has_custom = bool(burst_scores or battery_pcts or waiting_durations or retry_counts or infrastructure_importances)
        
        atis = []
        _type_ati_cache = {}
        for i in range(n):
            atk_raw = str(attack_types[i]).lower()
            if not has_custom and atk_raw in _type_ati_cache:
                atis.append(_type_ati_cache[atk_raw])
                continue

            mapped = ATTACK_MAPPING.get(atk_raw, 'normal').lower()
            severity = sev_map.get(mapped, 20.0)
            
            burst = burst_scores[i] if (burst_scores and i < len(burst_scores)) else 0.0
            bat = battery_pcts[i] if (battery_pcts and i < len(battery_pcts)) else 100.0
            wait = waiting_durations[i] if (waiting_durations and i < len(waiting_durations)) else 0.0
            retry = retry_counts[i] if (retry_counts and i < len(retry_counts)) else 0
            ctx = infrastructure_importances[i] if (infrastructure_importances and i < len(infrastructure_importances)) else 50.0
            
            battery_factor = max(0.0, min(100.0, 100.0 - bat))
            waiting_factor = min(100.0, wait * 10.0)
            retry_factor = min(100.0, retry * 25.0)
            context_factor = min(100.0, max(0.0, ctx))
            
            ati = (
                ATI_THREAT_WEIGHT * severity
                + ATI_BURST_WEIGHT * burst
                + ATI_ENERGY_WEIGHT * battery_factor
                + ATI_EVENT_WEIGHT * context_factor
            )
            final_ati = min(100.0, max(0.0, ati))
            if not has_custom:
                _type_ati_cache[atk_raw] = final_ati
            atis.append(final_ati)
            
        return atis

    # =========================================================================
    # ATTACK MAPPING
    # =========================================================================

    def map_attack_type(
        self,
        attack_type: str,
    ) -> str:
        """
        Normalize attack names.
        """

        attack = attack_type.strip().lower()

        return ATTACK_MAPPING.get(
            attack,
            "normal",
        ).lower()


     

    

    # =========================================================================
    # NODE EVENT CREATION
    # =========================================================================

    def create_node_event(
        self,
        event: DatasetEvent,
        threat_level: ThreatLevel,
        priority: Priority,
        burst_score: float,
        ati: float,
    ) -> NodeEvent:
        """
        Create a NodeEvent from a DatasetEvent.
        """

        return NodeEvent(

            timestamp=event.timestamp,

            node_id=event.source_ip,

            destination_node=event.destination_ip,

            protocol=event.protocol,

            service=event.service,

            payload_size=event.payload_size or 0,

            attack_type=event.attack_type,

            label=event.label,

            threat_level=threat_level,

            priority=priority,

            burst_score=burst_score,

            adaptive_threat_index=ati,
        )


    # =========================================================================
    # MAIN PIPELINE
    # =========================================================================

    def process(
        self,
        event: DatasetEvent,
    ) -> NodeEvent:
        """
        Execute the complete Stage-1 feature engineering pipeline.

        Per PDF Chapter 4.1: Physical Event → Event Context Object →
        ATI Engine → Dynamic Active Priority Queue
        """

        threat_level = self.assign_threat_level(
            event.attack_type
        )

        priority = self.assign_priority(
            threat_level
        )

        burst_score = self.calculate_burst_score(
            event
        )

        ati = self.calculate_adaptive_threat_index(
            threat_level=threat_level,
            burst_score=burst_score,
            battery_pct=100.0,        # Default; updated at runtime
            waiting_duration=0.0,     # Default; updated at runtime
            retry_count=0,            # Default; updated at runtime
            infrastructure_importance=50.0,  # Default mid-range
        )

        return self.create_node_event(
            event=event,
            threat_level=threat_level,
            priority=priority,
            burst_score=burst_score,
            ati=ati,
        )


    # =========================================================================
    # UTILITIES
    # =========================================================================

    def summary(self) -> None:
        """
        Log processing summary.
        """

        logger.info(
            f"Feature engineering completed for "
            f"{len(self.processed_events)} events."
        )


    def get_processed_events(self) -> List[NodeEvent]:
        """
        Return processed NodeEvent objects.
        """

        return self.processed_events.copy()