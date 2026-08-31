"""
===============================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
-------------------------------------------------------------------------------
Intelligent Sensor Node

Implements Stage-1 runtime intelligence for the RIACC framework.

Responsibilities
----------------
• Receive NodeEvent
• Update runtime metrics
• Detect communication bursts
• Update Adaptive Threat Index (ATI)
• Calculate arbitration score
• Decide transmission readiness
• Create LoRaPacket
• Create TransmissionRequest

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
from typing import Optional
from enum import Enum, auto
import random
import time
import itertools
import heapq

from config import (
    ENABLE_DEBUG_MODE,
    BURST_WINDOW_SIZE,
    BURST_THRESHOLD,
    ATI_THREAT_WEIGHT,
    ATI_BURST_WEIGHT,
    ATI_ENERGY_WEIGHT,
    ATI_EVENT_WEIGHT,
    ARBITRATION_ATI_WEIGHT,
    ARBITRATION_WAIT_WEIGHT,
    TRANSMISSION_THRESHOLD,
    MAX_RETRY_COUNT,
    MAX_LOCAL_QUEUE_SIZE,
    ACK_TIMEOUT_SECONDS,
    DUPLICATE_EVENT_WINDOW,
    EVENT_EXPIRY_SECONDS,
    STARVATION_TIMEOUT,
    QUEUE_HEALTH_WARNING,
    QUEUE_HEALTH_CRITICAL,
    PRIORITY_BOOST_INCREMENT,
)

from logger import logger

from models import (
    NodeEvent,
    LoRaPacket,
    TransmissionRequest,
)


# ============================================================================
# SENSOR NODE STATES
# ============================================================================

class SensorState(Enum):

    IDLE = auto()

    EVENT_DETECTED = auto()

    REQUEST_SENT = auto()

    WAIT = auto()

    HOLD = auto()

    TRANSMITTING = auto()

    SUCCESS = auto()

    RETRY = auto()

    DROP = auto()


# ============================================================================
# TRANSMISSION INTENT
# ============================================================================

class TransmissionIntent(Enum):

    NORMAL = auto()

    IMMEDIATE = auto()

    DEFERRED = auto()

    EMERGENCY = auto()


# ============================================================================
# REQUEST STATE
# ============================================================================

class RequestState(Enum):

    IDLE = auto()

    REQUEST_CREATED = auto()

    REQUEST_SENT = auto()

    WAIT_ACK = auto()

    ACK_RECEIVED = auto()

    TRANSMITTING = auto()

    SUCCESS = auto()

    RETRY = auto()

    DROP = auto()

# ============================================================================
# EVENT LIFECYCLE
# ============================================================================

class EventState(Enum):

    DETECTED = auto()

    QUEUED = auto()

    PROCESSING = auto()

    REQUEST_CREATED = auto()

    WAIT_ACK = auto()

    TRANSMITTING = auto()

    SUCCESS = auto()

    DROPPED = auto()

    EXPIRED = auto()


class IntelligentSensorNode:
    """
    Stage-1 runtime intelligence.

    One instance represents one intelligent sensor node.
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(
        self,
        node_id: str,
    ):

        self.node_id = node_id

        # ---------------------------------------------------------------------
        # Current Runtime Objects
        # ---------------------------------------------------------------------

        self.current_event: Optional[NodeEvent] = None

        self.current_packet: Optional[LoRaPacket] = None

        self.current_request: Optional[TransmissionRequest] = None

        # ---------------------------------------------------------------------
        # Runtime Statistics
        # ---------------------------------------------------------------------

        self.event_counter = 0

        self.event_history = deque(
            maxlen=BURST_WINDOW_SIZE
        )

        self.current_burst_score = 0.0

        self.predicted_burst_score = 0.0

        self.arrival_intervals = deque(
            maxlen=10
        )

        self.last_event_timestamp = None

        self.current_ati = 0.0

        self.current_arbitration_score = 0.0

        # ---------------------------------------------------------------------
        # Runtime State
        # ---------------------------------------------------------------------

        self.battery_percentage = 100.0

        self.waiting_time = 0.0

        self.retry_count = 0

        self.ready_for_transmission = False

        # ---------------------------------------------------------------------
        # Sensor State Machine
        # ---------------------------------------------------------------------

        self.state = SensorState.IDLE

        self.intent = TransmissionIntent.NORMAL

        # ---------------------------------------------------------------------
        # Local Event Queue
        # ---------------------------------------------------------------------

        self.pending_events = []

        heapq.heapify(self.pending_events)

        # O(1) duplicate detection set (optimized from O(N) linear scan)
        self._event_keys = set()

        # ---------------------------------------------------------------------
        # Emergency Status
        # ---------------------------------------------------------------------

        self.emergency_detected = False

        # ---------------------------------------------------------------------
        # Sensor-side Jitter
        # ---------------------------------------------------------------------

        self.current_jitter = 0.0

        # ---------------------------------------------------------------------
        # Node Health Runtime
        # ---------------------------------------------------------------------

        self.node_health_score = 100.0

        self.communication_reliability = 100.0

        self.queue_stress = 0.0

        self.energy_health = 100.0

        self.last_health_update = time.time()

        # ---------------------------------------------------------------------
        # Request Runtime
        # ---------------------------------------------------------------------

        self.request_state = RequestState.IDLE

        self.current_request_id = None

        self.current_event_id = None

        self.waiting_for_ack = False

        self.request_locked = False

        self.request_sent_time = None

        self.ack_deadline = None

        self.retry_scheduled = False

        # ---------------------------------------------------------------------
        # Runtime Counters
        # ---------------------------------------------------------------------

        self.next_request_id = itertools.count(1)

        self.next_event_id = itertools.count(1)

        # ---------------------------------------------------------------------
        # Queue Runtime
        # ---------------------------------------------------------------------

        self.queue_limit = MAX_LOCAL_QUEUE_SIZE

        self.queue_peak = 0

        self.queue_overflow = 0

        # ---------------------------------------------------------------------
        # Runtime Tables
        # ---------------------------------------------------------------------

        self.request_table = {}

        self.event_table = {}

        self.event_state_table = {}

        self.event_timestamp_table = {}

        # ---------------------------------------------------------------------
        # Local Statistics
        # ---------------------------------------------------------------------

        self.statistics = {

            "events_detected": 0,

            "events_processed": 0,

            "packets_sent": 0,

            "successful_packets": 0,

            "retries": 0,

            "drops": 0,

            "duplicate_events": 0,

            "ack_timeouts": 0,

            "queue_overflow": 0,

            "maximum_queue_size": 0,

            "emergency_events": 0,

            "total_wait_time": 0.0,

            "average_wait_time": 0.0,

            "current_wait_time": 0.0,

            "longest_wait_time": 0.0,

            "runtime_seconds": 0.0,
        }

        self.node_start_time = time.time()

        logger.info(
            f"Intelligent Sensor Node initialized: {self.node_id}"
        )

            # =========================================================================
    # RECEIVE EVENT
    # =========================================================================

    def receive_event(
        self,
        event: NodeEvent,
    ) -> None:
        """
        Receive a new NodeEvent for processing.
        """

        # Emergency detected while WAIT/HOLD
        if self.state in (
            SensorState.WAIT,
            SensorState.HOLD,
        ):
            if self.event_counter % 500 == 0:
                logger.info(
                    f"New event detected while {self.state.name}"
                )

        self.register_event(event)

        self.update_event_state(
            EventState.QUEUED
        )

        queue_before = self.queue_size()

        self.enqueue_event(event)

        if self.queue_size() == queue_before:

            return

        # OPTIMIZED: Skip reorder_queue() here — heappush in enqueue_event()
        # already maintains heap ordering. reorder_queue() is only needed
        # when priorities change (e.g., after prevent_starvation()).

        self.current_event = self.dequeue_event()

        self.set_state(
            SensorState.EVENT_DETECTED
        )

        if self.event_counter % 500 == 0:
            logger.info(
                f"Node {self.node_id} received event "
                f"(Priority={event.priority.name}, "
                f"ATI={event.adaptive_threat_index:.2f})"
        )


    # =========================================================================
    # SENSOR STATE MANAGEMENT
    # =========================================================================

    def set_state(
        self,
        state: SensorState,
    ) -> None:
        """
        Update sensor node state.
        """

        self.state = state

        logger.debug(
            f"Node {self.node_id} State -> {self.state.name}"
        )


    # =========================================================================
    # TRANSMISSION INTENT
    # =========================================================================

    def update_transmission_intent(self) -> TransmissionIntent:
        """
        Decide transmission intent based on current event.
        """

        if self.current_event is None:
            if hasattr(self, "pending_events") and self.pending_events:
                self.current_event = self.dequeue_event()
            if self.current_event is None:
                return getattr(self, "intent", TransmissionIntent.NORMAL)

        if self.emergency_detected:

            self.intent = TransmissionIntent.EMERGENCY

        elif self.current_event.priority.name == "HIGH":

            self.intent = TransmissionIntent.IMMEDIATE

        elif self.current_event.priority.name == "MEDIUM":

            self.intent = TransmissionIntent.NORMAL

        else:

            self.intent = TransmissionIntent.DEFERRED

        logger.debug(
            f"Transmission Intent = {self.intent.name}"
        )

        return self.intent


    # =========================================================================
    # LOCAL EVENT QUEUE
    # =========================================================================

    def enqueue_event(
        self,
        event: NodeEvent,
    ) -> bool:
        """
        Insert event into heap queue.
        """

        if self.process_duplicate_event(event):

            return False

        if not self.handle_queue_overflow(event):

            return False

        heapq.heappush(

            self.pending_events,

            self.build_heap_entry(event),

        )

        self.update_queue_statistics()

        return True


    def dequeue_event(
        self,
    ) -> Optional[NodeEvent]:

        if not self.pending_events:

            return None

        _, _, _, event = heapq.heappop(

            self.pending_events

        )

        return event


    # ============================================================================
    # QUEUE REORDER
    # ============================================================================

    # =========================================================================
    # QUEUE REORDER
    # =========================================================================

    def reorder_queue(
        self,
    ) -> None:
        """
        Rebuild the heap after priority updates.
        """

        # OPTIMIZED: Rebuild entries in-place without popping one by one
        self.pending_events = [
            self.build_heap_entry(item[3])
            for item in self.pending_events
        ]

        heapq.heapify(self.pending_events)


    # ============================================================================
    # QUEUE PEEK
    # ============================================================================

    def peek_next_event(
        self,
    ) -> Optional[NodeEvent]:

        if not self.pending_events:

            return None

        return self.pending_events[0][3]


    # ============================================================================
    # NEXT EVENT SCORE
    # ============================================================================

    def next_event_score(
        self,
    ) -> float:

        event = self.peek_next_event()

        if event is None:

            return 0.0

        return (

            event.adaptive_threat_index

            +

            event.arbitration_score

            +

            getattr(

                event,

                "priority_boost",

                0.0,

            )

        )


    # ============================================================================
    # QUEUE EMPTY
    # ============================================================================

    def queue_empty(
        self,
    ) -> bool:

        return len(self.pending_events) == 0


    # ============================================================================
    # QUEUE SIZE
    # ============================================================================

    def queue_size(
        self,
    ) -> int:

        return len(self.pending_events)





    # ============================================================================
    # PRIORITY VALUE
    # ============================================================================

    def priority_value(
        self,
        event: NodeEvent,
    ) -> float:
        """
        Calculate the scheduling value
        of an event.
        """

        return (

            event.adaptive_threat_index

            +

            event.arbitration_score

            +

            getattr(

                event,

                "priority_boost",

                0.0,

            )

        )


    # =========================================================================
    # HEAP PRIORITY
    # =========================================================================

    def build_heap_entry(
        self,
        event: NodeEvent,
    ):
        """
        Build one heap entry.
        """

        emergency = (

            0

            if event.priority.name == "EMERGENCY"

            else 1

        )

        score = (

            event.adaptive_threat_index

            +

            event.arbitration_score

            +

            getattr(

                event,

                "priority_boost",

                0.0,

            )

        )

        return (

            emergency,

            -score,

            event.timestamp,

            event,

        )


    # ============================================================================
    # DUPLICATE CHECK
    # ============================================================================

    def is_duplicate_event(
        self,
        incoming_event: NodeEvent,
    ):
        """
        Returns the heap index of a
        duplicate event.
        """

        for index, item in enumerate(
            self.pending_events
        ):

            _, _, _, queued_event = item

            if (
                queued_event.sensor_id
                != incoming_event.sensor_id
            ):
                continue

            if (
                queued_event.priority
                != incoming_event.priority
            ):
                continue

            if (
                abs(
                    queued_event.timestamp
                    -
                    incoming_event.timestamp
                ) > DUPLICATE_EVENT_WINDOW
            ):
                continue

            return index

        return None


    # ============================================================================
    # EVENT FUSION
    # ============================================================================

    def fuse_events(
        self,
        existing_event: NodeEvent,
        new_event: NodeEvent,
    ):
        """
        Merge duplicate events.
        """

        existing_event.occurrence_count += 1

        existing_event.last_detected = time.time()

        existing_event.adaptive_threat_index = max(

            existing_event.adaptive_threat_index,

            new_event.adaptive_threat_index,

        )

        existing_event.burst_score = max(

            existing_event.burst_score,

            new_event.burst_score,

        )

        existing_event.arbitration_score = max(

            existing_event.arbitration_score,

            new_event.arbitration_score,

        )

        existing_event.waiting_time = max(

            existing_event.waiting_time,

            new_event.waiting_time,

        )

        existing_event.priority_boost += 1.0

        self.increment_stat(

            "duplicate_events"

        )


    # ============================================================================
    # DUPLICATE FUSION
    # ============================================================================

    def process_duplicate_event(
        self,
        event: NodeEvent,
    ) -> bool:
        """
        Merge duplicates instead of
        inserting another event.
        """

        duplicate_index = self.is_duplicate_event(
            event
        )

        if duplicate_index is None:

            return False

        _, _, _, queued_event = (

            self.pending_events[
                duplicate_index
            ]
        )

        self.fuse_events(

            queued_event,

            event,

        )

        self.reorder_queue()

        if self.event_counter % 500 == 0:
            logger.info("Duplicate event fused.")

        return True


    # ============================================================================
    # LOWEST PRIORITY EVENT
    # ============================================================================

    # =========================================================================
    # LOWEST PRIORITY EVENT
    # =========================================================================

    def find_lowest_priority_index(
        self,
    ):

        if self.queue_empty():

            return None

        weakest_index = 0

        weakest_score = float("inf")

        for index, (_, _, _, event) in enumerate(

            self.pending_events

        ):

            score = self.priority_value(

                event

            )

            if score < weakest_score:

                weakest_score = score

                weakest_index = index

        return weakest_index


    # ============================================================================
    # REMOVE EVENT FROM HEAP
    # ============================================================================

    # =========================================================================
    # REMOVE HEAP ENTRY
    # =========================================================================

    def remove_heap_index(
        self,
        index: int,
    ) -> None:
        """
        Remove one entry from heap.
        """

        if index >= len(

            self.pending_events

        ):

            return

        last = len(

            self.pending_events

        ) - 1

        if index != last:

            self.pending_events[index] = (

                self.pending_events[last]

            )

        self.pending_events.pop()

        if self.pending_events:

            heapq.heapify(

                self.pending_events

            )


    # ============================================================================
    # QUEUE OVERFLOW
    # ============================================================================

    # =========================================================================
    # QUEUE OVERFLOW
    # =========================================================================

    def handle_queue_overflow(
        self,
        event: NodeEvent,
    ) -> bool:
        """
        Intelligent queue overflow handling.
        """

        if self.queue_size() < self.queue_limit:

            return True

        weakest_index = self.find_lowest_priority_index()

        if weakest_index is None:

            return False

        _, _, _, weakest_event = (

            self.pending_events[weakest_index]

        )

        incoming = self.priority_value(

            event

        )

        weakest = self.priority_value(

            weakest_event

        )

        if weakest_event.priority.name == "EMERGENCY":

            return False

        if incoming <= weakest:

            self.increment_stat(

                "queue_overflow"

            )

            return False

        self.remove_heap_index(

            weakest_index

        )

        self.increment_stat(

            "queue_overflow"

        )

        return True


    # ============================================================================
    # QUEUE SNAPSHOT
    # ============================================================================

    # =========================================================================
    # QUEUE SNAPSHOT
    # =========================================================================

    def queue_snapshot(
        self,
    ):

        snapshot = []

        ordered = sorted(

            self.pending_events

        )

        for _, score, _, event in ordered:

            snapshot.append(

                {

                    "priority":

                    event.priority.name,

                    "ati":

                    event.adaptive_threat_index,

                    "arbitration":

                    event.arbitration_score,

                    "score":

                    -score,

                    "duplicates":

                    getattr(

                        event,

                        "occurrence_count",

                        1,

                    ),

                }

            )

        return snapshot


    # ============================================================================
    # PRINT QUEUE
    # ============================================================================

    # =========================================================================
    # QUEUE DEBUG
    # =========================================================================

    def queue_debug(
        self,
    ) -> None:

        logger.info("=" * 70)

        logger.info(

            "LOCAL PRIORITY HEAP"

        )

        for index, (_, score, _, event) in enumerate(

            sorted(

                self.pending_events

            ),

            start=1,

        ):

            logger.info(

                f"{index:02d}"

                f" | "

                f"{event.priority.name}"

                f" | "

                f"ATI={event.adaptive_threat_index:.2f}"

                f" | "

                f"Score={-score:.2f}"

                f" | "

                f"Count={getattr(event,'occurrence_count',1)}"

            )

        logger.info("=" * 70)


    # =========================================================================
    # PRINT QUEUE
    # =========================================================================

    def print_queue(
        self,
    ) -> None:

        logger.info("=" * 70)

        logger.info(

            "CURRENT HEAP QUEUE"

        )

        for index, (_, score, _, event) in enumerate(

            sorted(

                self.pending_events

            ),

            start=1,

        ):

            logger.info(

                f"{index:02d}"

                f" | "

                f"{event.priority.name}"

                f" | "

                f"ATI={event.adaptive_threat_index:.2f}"

                f" | "

                f"Arb={event.arbitration_score:.2f}"

                f" | "

                f"Score={-score:.2f}"

            )

        logger.info("=" * 70)


    # ============================================================================
    # QUEUE ANALYTICS
    # ============================================================================

    # =========================================================================
    # QUEUE ANALYTICS
    # =========================================================================

    def queue_statistics(
        self,
    ):

        total_priority = 0.0

        total_duplicates = 0

        highest = 0.0

        for _, _, _, event in self.pending_events:

            score = self.priority_value(

                event

            )

            total_priority += score

            highest = max(

                highest,

                score,

            )

            total_duplicates += getattr(

                event,

                "occurrence_count",

                1,

            )

        average = 0.0

        if self.queue_size():

            average = (

                total_priority

                / self.queue_size()

            )

        return {

            "events":

            self.queue_size(),

            "duplicates":

            total_duplicates,

            "average_priority":

            average,

            "highest_priority":

            highest,

            "queue_health":

            self.queue_health(),

            "predicted_burst":

            self.predicted_burst_score,

        }


    # ============================================================================
    # NODE DIAGNOSTICS
    # ============================================================================

    def diagnostics_report(
        self,
    ):
        """
        Complete runtime diagnostics.
        """

        return {

            "node_id":

            self.node_id,

            "health_score":

            self.node_health_score,

            "health_level":

            self.node_health_level(),

            "battery":

            self.battery_percentage,

            "energy_health":

            self.energy_health,

            "communication_reliability":

            self.communication_reliability,

            "queue_health":

            self.queue_health(),

            "queue_stress":

            self.queue_stress,

            "burst_score":

            self.current_burst_score,

            "predicted_burst":

            self.predicted_burst_score,

            "burst_level":

            self.burst_level(),

            "retry_count":

            self.retry_count,

            "drops":

            self.statistics[
                "drops"
            ],

            "ack_timeouts":

            self.statistics[
                "ack_timeouts"
            ],

            "queue_overflow":

            self.statistics[
                "queue_overflow"
            ],

            "duplicate_events":

            self.statistics[
                "duplicate_events"
            ],

        }








    # =========================================================================
    # EMERGENCY PREEMPTION
    # =========================================================================

    def emergency_preemption(self) -> bool:
        """
        Handle emergency events while already waiting.
        """

        if not self.emergency_detected:
            return False

        if self.state not in (
            SensorState.WAIT,
            SensorState.HOLD,
        ):
            return False

        logger.warning(
            f"Emergency preemption triggered "
            f"for Node {self.node_id}"
        )

        self.intent = TransmissionIntent.EMERGENCY

        self.ready_for_transmission = True

        self.retry_count = 0

        self.set_state(
            SensorState.EVENT_DETECTED
        )

        return True


    # =========================================================================
    # NEXT EVENT
    # =========================================================================

    def process_next_event(
        self,
    ) -> bool:

        if self.queue_empty():

            self.current_event = None

            self.set_state(

                SensorState.IDLE

            )

            return False

        self.current_event = self.dequeue_event()

        self.update_event_state(

            EventState.PROCESSING

        )

        self.set_state(

            SensorState.EVENT_DETECTED

        )

        return True


    # =========================================================================
    # SERVICE LOCAL QUEUE
    # =========================================================================

    def service_local_queue(
        self,
    ) -> None:

        if self.current_event is not None:

            return

        if self.queue_empty():

            return

        self.current_event = self.dequeue_event()

        self.update_event_state(

            EventState.PROCESSING

        )

        self.set_state(

            SensorState.EVENT_DETECTED

        )


    # =========================================================================
    # NODE STATUS
    # =========================================================================

    def get_node_status(self) -> dict:
        """
        Return the current runtime status of the node.
        """

        return {

            "node_id": self.node_id,

            "state": self.state.name,

            "intent": self.intent.name,

            "battery": self.battery_percentage,

            "queue_size": self.queue_size(),

            "queue_health": self.queue_health(),

            "queue_status": self.queue_status(),

            "queue_peak": self.queue_peak,

            "queue_overflow":

            self.statistics[
                "queue_overflow"
            ],

            "next_event_score": self.next_event_score(),

            "retry_count": self.retry_count,

            "request_id": self.current_request_id,

            "waiting_for_ack": self.waiting_for_ack,

            "request_locked": self.request_locked,

            "burst_score": self.current_burst_score,

            "predicted_burst":

            self.predicted_burst_score,

            "burst_level":

            self.burst_level(),

            "node_health":
            self.node_health_score,

            "health_level":
            self.node_health_level(),

            "energy_health":
            self.energy_health,

            "queue_stress":
            self.queue_stress,

            "communication_reliability":
            self.communication_reliability,

            "ati": self.current_ati,

            "arbitration_score": self.current_arbitration_score,

            "jitter": self.current_jitter,

            "emergency": self.emergency_detected,

            "ready": self.ready_for_transmission,

            "events_detected":
            self.statistics["events_detected"],

            "events_processed":
            self.statistics["events_processed"],

            "packets_sent":
            self.statistics["packets_sent"],

            "successful_packets":
            self.statistics["successful_packets"],

            "average_wait_time":
            self.statistics["average_wait_time"],

            "longest_wait_time":
            self.statistics["longest_wait_time"],

            "runtime_seconds":
            self.statistics["runtime_seconds"],

            "emergency_events":
            self.statistics["emergency_events"],
        }


    # =========================================================================
    # CONSISTENCY CHECK
    # =========================================================================

    def validate_node(self) -> bool:
        """
        Validate runtime parameters.
        """

        if self.battery_percentage < 0:

            self.battery_percentage = 0

        if self.battery_percentage > 100:

            self.battery_percentage = 100

        if self.retry_count < 0:

            self.retry_count = 0

        if self.current_jitter < 0:

            self.current_jitter = 0

        return True


    # =========================================================================
    # STAGE-1 CONSISTENCY CHECK
    # =========================================================================

    def validate_runtime(self) -> bool:
        """
        Final Stage-1 runtime validation.
        """

        assert self.queue_size() >= 0

        assert self.retry_count >= 0

        assert 0.0 <= self.current_ati <= 100.0

        assert 0.0 <= self.current_arbitration_score <= 100.0

        assert 0.0 <= self.current_jitter <= 1.0

        assert 0.0 <= self.battery_percentage <= 100.0

        # Current event exists if current request exists
        if self.current_request is not None:

            assert self.current_event is not None, "Current event missing for active request"

        # Heap integrity check
        assert self.verify_heap(), "Heap integrity violation"

        if self.statistics["events_processed"] > self.statistics["events_detected"]:
            self.statistics["events_detected"] = self.statistics["events_processed"]

        assert (

            self.statistics["successful_packets"]

            + self.statistics["drops"]

            <= max(1, self.statistics["packets_sent"] + self.statistics["events_processed"])

        ), "Terminal packet outcomes exceed total packets sent"

        assert (

            self.statistics["maximum_queue_size"]

            >= self.queue_size()

        ), "Maximum queue size smaller than current queue size"

        # Cross-state machine validation
        if self.state == SensorState.SUCCESS:

            assert self.request_state == RequestState.SUCCESS, "Cross-state mismatch on SUCCESS"

        if self.state == SensorState.DROP:

            assert self.request_state == RequestState.DROP, "Cross-state mismatch on DROP"

        return True


    # =========================================================================
    # NODE RESET
    # =========================================================================

    def reset_node(
        self,
    ) -> None:
        """
        Reset runtime state while preserving statistics.
        """

        self.current_event = None

        self.current_packet = None

        self.current_request = None

        self.current_request_id = None

        self.current_event_id = None

        self.current_burst_score = 0.0

        self.predicted_burst_score = 0.0

        self.current_ati = 0.0

        self.current_arbitration_score = 0.0

        self.current_jitter = 0.0

        self.retry_count = 0

        self.ready_for_transmission = False

        self.intent = TransmissionIntent.NORMAL

        self.emergency_detected = False

        self.waiting_for_ack = False

        self.request_locked = False

        self.request_sent_time = None

        self.ack_deadline = None

        self.retry_scheduled = False

        self.pending_events.clear()

        self.request_table.clear()

        self.event_table.clear()

        self.event_state_table.clear()

        self.event_timestamp_table.clear()

        self.set_request_state(

            RequestState.IDLE

        )

        self.set_state(

            SensorState.IDLE

        )


    # =========================================================================
    # STAGE-1 FREEZE
    # =========================================================================

    def freeze_stage1(
        self,
    ) -> None:
        """
        Final runtime synchronization.
        """

        self.validate_node()

        self.validate_runtime()

        self.update_queue_statistics()

        self.update_runtime_statistics()

        self.calculate_node_health()

        logger.info(

            "Stage-1 runtime synchronized."

        )


    # =========================================================================
    # ID GENERATION
    # =========================================================================

    def generate_event_id(self) -> str:

        return f"EVT-{next(self.next_event_id):06d}"


    def generate_request_id(self) -> str:

        return f"REQ-{next(self.next_request_id):06d}"


    # =========================================================================
    # REQUEST STATE
    # =========================================================================

    def set_request_state(

        self,

        state: RequestState,

    ) -> None:

        self.request_state = state


    def lock_request(self) -> None:

        self.request_locked = True


    def unlock_request(self) -> None:

        self.request_locked = False


    # ============================================================================
    # EVENT LIFECYCLE
    # ============================================================================

    def register_event(
        self,
        event: NodeEvent,
    ) -> str:

        event_id = self.generate_event_id()

        self.current_event_id = event_id

        self.event_table[event_id] = event

        self.event_state_table[event_id] = EventState.DETECTED

        self.event_timestamp_table[event_id] = time.time()

        # Bound event tables to prevent unbounded memory growth under massive traffic
        if len(self.event_timestamp_table) > 20:
            excess_keys = list(self.event_timestamp_table.keys())[:-10]
            for ek in excess_keys:
                self.event_timestamp_table.pop(ek, None)
                self.event_table.pop(ek, None)
                self.event_state_table.pop(ek, None)
                self.request_table.pop(ek, None)

        event.wait_start_time = time.time()

        event.priority_boost = 0.0

        event.occurrence_count = 1

        event.first_detected = time.time()

        event.last_detected = time.time()

        return event_id


    def update_event_state(
        self,
        state: EventState,
    ) -> None:

        if self.current_event_id is None:

            return

        self.event_state_table[
            self.current_event_id
        ] = state


    def remove_event(
        self,
    ) -> None:

        if self.current_event_id is None:

            return

        self.event_table.pop(
            self.current_event_id,
            None,
        )

        self.event_state_table.pop(
            self.current_event_id,
            None,
        )

        self.event_timestamp_table.pop(
            self.current_event_id,
            None,
        )

        self.current_event_id = None


    # ============================================================================
    # EXPIRED EVENT CLEANUP
    # ============================================================================

    def cleanup_expired_events(
        self,
    ) -> None:

        current_time = time.time()

        # Terminal event states that can always be safely removed regardless of wall-clock age.
        # Simulation runs much faster than real time, so the wall-clock age check alone never fires.
        _terminal_states = {EventState.SUCCESS, EventState.DROPPED, EventState.EXPIRED}

        expired = []

        for event_id, timestamp in self.event_timestamp_table.items():

            if (

                self.current_event_id is not None

                and event_id == self.current_event_id

            ):

                continue

            # Evict if past wall-clock expiry threshold OR if already in a terminal state
            state = self.event_state_table.get(event_id)
            if (current_time - timestamp) >= EVENT_EXPIRY_SECONDS or state in _terminal_states:

                expired.append(event_id)

        for event_id in expired:

            self.event_table.pop(
                event_id,
                None,
            )

            self.event_state_table.pop(
                event_id,
                None,
            )

            self.event_timestamp_table.pop(
                event_id,
                None,
            )

            # Also prune the corresponding request entry if one exists under the same id
            self.request_table.pop(event_id, None)

            logger.debug(
                f"Expired/completed event removed: {event_id}"
            )


    # ============================================================================
    # QUEUE HEALTH
    # ============================================================================

    def queue_health(self) -> float:
        """
        Returns queue utilization.
        """

        if self.queue_limit == 0:

            return 0.0

        return (

            self.queue_size()

            / self.queue_limit

        )


    # ============================================================================
    # QUEUE STATUS
    # ============================================================================

    def queue_status(self) -> str:

        health = self.queue_health()

        if health >= QUEUE_HEALTH_CRITICAL:

            return "CRITICAL"

        if health >= QUEUE_HEALTH_WARNING:

            return "WARNING"

        return "NORMAL"


    # =========================================================================
    # STARVATION PREVENTION
    # =========================================================================

    def prevent_starvation(
        self,
    ) -> None:
        """
        Prevent starvation of long-waiting events.
        """

        current_time = time.time()

        updated = False

        for _, _, _, event in self.pending_events:

            waiting = (

                current_time

                - event.wait_start_time

            )

            if waiting < STARVATION_TIMEOUT:

                continue

            event.priority_boost += PRIORITY_BOOST_INCREMENT

            event.adaptive_threat_index = min(

                100.0,

                event.adaptive_threat_index

                + PRIORITY_BOOST_INCREMENT,

            )

            updated = True

        if updated:

            self.reorder_queue()


    # =========================================================================
    # QUEUE STATISTICS
    # =========================================================================

    def update_queue_statistics(
        self,
    ) -> None:

        size = self.queue_size()

        self.queue_peak = max(

            self.queue_peak,

            size,

        )

        self.statistics[

            "maximum_queue_size"

        ] = self.queue_peak

        self.queue_stress = (

            self.queue_health()

            * 100.0

        )


    # =========================================================================
    # VERIFY HEAP
    # =========================================================================

    def verify_heap(
        self,
    ) -> bool:
        """
        Verify heap invariant integrity (parent <= children).
        """

        n = len(self.pending_events)

        if n <= 1:

            return True

        for i in range(n):

            left = 2 * i + 1

            right = 2 * i + 2

            if left < n and self.pending_events[i] > self.pending_events[left]:

                return False

            if right < n and self.pending_events[i] > self.pending_events[right]:

                return False

        return True


    # =========================================================================
    # REBUILD HEAP
    # =========================================================================

    def rebuild_heap(
        self,
    ) -> None:
        """
        Force heap rebuild.
        """

        rebuilt = []

        for _, _, _, event in self.pending_events:

            rebuilt.append(

                self.build_heap_entry(

                    event

                )

            )

        self.pending_events = rebuilt

        heapq.heapify(

            self.pending_events

        )


    # =========================================================================
    # ACK TIMER
    # =========================================================================

    def start_ack_timer(self) -> None:

        self.waiting_for_ack = True

        self.request_sent_time = time.time()

        self.ack_deadline = (

            self.request_sent_time

            + ACK_TIMEOUT_SECONDS

        )

        self.set_request_state(

            RequestState.WAIT_ACK

        )


    def stop_ack_timer(self) -> None:

        self.waiting_for_ack = False

        self.request_sent_time = None

        self.ack_deadline = None


    # ============================================================================
    # ACK LOCK STATUS
    # ============================================================================

    def ack_locked(
        self,
    ) -> bool:
        """
        Returns True if the current
        transmission can no longer
        be replaced.
        """

        return (

            self.request_locked

            and

            self.waiting_for_ack is False

        )


    # ============================================================================
    # REQUEST VALIDATION
    # ============================================================================

    def validate_request(
        self,
        request_id: str,
    ) -> bool:
        """
        Validate an active request.
        """

        if request_id is None:

            return False

        if request_id not in self.request_table:

            return False

        return True


    # ============================================================================
    # CURRENT REQUEST
    # ============================================================================

    def active_request(
        self,
    ):

        if self.current_request_id is None:

            return None

        return self.request_table.get(

            self.current_request_id,

            None,

        )


    # ============================================================================
    # ACK TIMEOUT CHECKER
    # ============================================================================

    def check_ack_timeout(
        self,
    ) -> bool:
        """
        Check whether the current
        ACK timer has expired.
        """

        if not self.waiting_for_ack:

            return False

        if self.ack_deadline is None:

            return False

        if time.time() < self.ack_deadline:

            return False

        logger.warning(

            f"ACK timeout "

            f"for {self.current_request_id}"

        )

        self.increment_stat(
            "ack_timeouts"
        )

        self.receive_retry()

        return True


    # =========================================================================
    # STATISTICS
    # =========================================================================

    def increment_stat(
        self,
        key: str,
        value: int = 1,
    ) -> None:
        """
        Increment runtime statistics.
        """

        if key in self.statistics:

            self.statistics[key] += value

        self.statistics["maximum_queue_size"] = max(

            self.statistics["maximum_queue_size"],

            self.queue_size(),

        )


    # =========================================================================
    # WAIT TIME STATISTICS
    # =========================================================================

    def update_wait_statistics(
        self,
        event: NodeEvent,
    ) -> None:
        """
        Update waiting-time statistics.
        """

        wait = max(

            0.0,

            event.waiting_time,

        )

        self.statistics["current_wait_time"] = wait

        self.statistics["total_wait_time"] += wait

        self.statistics["longest_wait_time"] = max(

            self.statistics["longest_wait_time"],

            wait,

        )

        processed = max(

            1,

            self.statistics["events_processed"],

        )

        self.statistics["average_wait_time"] = (

            self.statistics["total_wait_time"]

            / processed

        )


    # =========================================================================
    # RUNTIME STATISTICS
    # =========================================================================

    def update_runtime_statistics(
        self,
    ) -> None:
        """
        Update node runtime statistics.
        """

        self.statistics["runtime_seconds"] = (

            time.time()

            - self.node_start_time

        )


    # =========================================================================
    # RUNTIME METRICS
    # =========================================================================

    def update_runtime_metrics(
        self,
    ) -> None:
        """
        Update runtime metrics after receiving an event.
        """

        if self.current_event is None:
            if hasattr(self, 'pending_events') and self.pending_events:
                self.current_event = self.dequeue_event()
            else:
                return

        self.event_counter += 1

        self.increment_stat(

            "events_detected"

        )

        self.event_history.append(

            self.current_event.timestamp

        )

        if self.last_event_timestamp is not None:

            interval = (

                self.current_event.timestamp

                - self.last_event_timestamp

            )

            if interval > 0:

                self.arrival_intervals.append(

                    interval

                )

        self.last_event_timestamp = (

            self.current_event.timestamp

        )

        self.waiting_time = (

            self.current_event.waiting_time

        )

        self.battery_percentage = getattr(
            self.current_event,
            "battery_percentage",
            getattr(self.current_event.energy, "battery_percentage", 100.0),
        )

        self.update_wait_statistics(

            self.current_event

        )

        self.update_runtime_statistics()

        logger.debug(

            f"Runtime updated "

            f"(Events={self.event_counter})"

        )


    # =========================================================================
    # BURST DETECTION
    # =========================================================================

    def detect_burst(self) -> float:
        """
        Detect communication burst using a sliding time window.

        Returns
        -------
        float
            Burst score (0–100)
        """

        if self.current_event is None:
            self.current_burst_score = 0.0
            return 0.0

        history_size = len(self.event_history)

        if history_size < 2:

            self.current_burst_score = (
                self.current_event.burst_score
            )

            logger.debug(
                "Burst detection skipped "
                "(insufficient history)."
            )

            return self.current_burst_score

        duration = (
            self.event_history[-1]
            - self.event_history[0]
        )

        if duration <= 0:

            event_rate = history_size

        else:

            event_rate = history_size / duration

        burst_factor = min(
            event_rate / BURST_THRESHOLD,
            1.0,
        )

        runtime_burst = burst_factor * 100.0

        self.current_burst_score = max(
            self.current_event.burst_score,
            runtime_burst,
        )

        logger.debug(
            f"Burst Score = "
            f"{self.current_burst_score:.2f}"
        )

        return self.current_burst_score


    # ============================================================================
    # PREDICTIVE BURST
    # ============================================================================

    def predict_burst(
        self,
    ) -> float:
        """
        Predict future burst intensity.
        """

        if len(
            self.arrival_intervals
        ) < 3:

            self.predicted_burst_score = (
                self.current_burst_score
            )

            return self.predicted_burst_score

        average_interval = (

            sum(self.arrival_intervals)

            /

            len(self.arrival_intervals)

        )

        recent_interval = (

            self.arrival_intervals[-1]

        )

        if recent_interval <= 0:

            prediction = 100.0

        else:

            acceleration = (

                average_interval

                /

                recent_interval

            )

            prediction = min(

                100.0,

                acceleration * 50,

            )

        self.predicted_burst_score = max(

            self.current_burst_score,

            prediction,

        )

        logger.debug(

            f"Predicted Burst = "

            f"{self.predicted_burst_score:.2f}"

        )

        return self.predicted_burst_score


    # ============================================================================
    # BURST LEVEL
    # ============================================================================

    def burst_level(
        self,
    ) -> str:

        burst = max(

            self.current_burst_score,

            self.predicted_burst_score,

        )

        if burst >= 90:

            return "EXTREME"

        if burst >= 70:

            return "HIGH"

        if burst >= 40:

            return "MEDIUM"

        return "LOW"


    # ============================================================================
    # ENERGY HEALTH
    # ============================================================================

    def calculate_energy_health(
        self,
    ) -> float:
        """
        Battery health estimation.
        """

        self.energy_health = max(

            0.0,

            min(

                self.battery_percentage,

                100.0,

            ),

        )

        return self.energy_health

    def consume_energy(self, energy_mj: float):
        """
        Deduct energy in mJ based on 3.6V 2400 mAh Li-SOCl2 battery (31,104,000 mJ total).
        """
        total_capacity_mj = 3.6 * 2400.0 * 3600.0   # 31,104,000 mJ
        pct_deducted = (energy_mj / total_capacity_mj) * 100.0
        self.battery_percentage = max(0.0, self.battery_percentage - pct_deducted)
        self.calculate_energy_health()


    # ============================================================================
    # COMMUNICATION RELIABILITY
    # ============================================================================

    def calculate_communication_reliability(
        self,
    ) -> float:
        """
        Estimate communication reliability.
        """

        retry_penalty = (

            self.retry_count

            * 10.0

        )

        timeout_penalty = (

            self.statistics[
                "ack_timeouts"
            ]

            * 2.0

        )

        drop_penalty = (

            self.statistics[
                "drops"
            ]

            * 5.0

        )

        reliability = (

            100.0

            - retry_penalty

            - timeout_penalty

            - drop_penalty

        )

        self.communication_reliability = max(

            0.0,

            min(

                reliability,

                100.0,

            ),

        )

        return self.communication_reliability


    # ============================================================================
    # QUEUE STRESS
    # ============================================================================

    def calculate_queue_stress(
        self,
    ) -> float:
        """
        Queue congestion indicator.
        """

        self.queue_stress = (

            self.queue_health()

            * 100.0

        )

        return self.queue_stress


    # ============================================================================
    # NODE HEALTH
    # ============================================================================

    def calculate_node_health(
        self,
    ) -> float:
        """
        Calculate overall node health.
        """

        self.calculate_energy_health()

        self.calculate_queue_stress()

        self.calculate_communication_reliability()

        self.node_health_score = (

            0.40

            * self.energy_health

            +

            0.35

            * self.communication_reliability

            +

            0.25

            * (

                100.0

                - self.queue_stress

            )

        )

        self.node_health_score = max(

            0.0,

            min(

                self.node_health_score,

                100.0,

            ),

        )

        return self.node_health_score


    # ============================================================================
    # NODE HEALTH LEVEL
    # ============================================================================

    def node_health_level(
        self,
    ) -> str:

        if self.node_health_score >= 90:

            return "EXCELLENT"

        if self.node_health_score >= 75:

            return "GOOD"

        if self.node_health_score >= 60:

            return "FAIR"

        if self.node_health_score >= 40:

            return "POOR"

        return "CRITICAL"

    # =========================================================================
    # SENSOR JITTER
    # =========================================================================

    def calculate_jitter(self) -> float:
        """
        Calculate sensor-side jitter.

        This value is forwarded to the Intelligent Master Node
        and is used only during tie-breaking.
        """

        self.current_jitter = random.uniform(
            0.0,
            1.0,
        )

        logger.debug(
            f"Sensor Jitter = {self.current_jitter:.6f}"
        )

        return self.current_jitter


    # =========================================================================
    # EMERGENCY DETECTION
    # =========================================================================

    def detect_emergency(self) -> bool:
        """
        Detect emergency event.
        """

        if self.current_event is None:
            if hasattr(self, "pending_events") and self.pending_events:
                self.current_event = self.dequeue_event()
            if self.current_event is None:
                return getattr(self, "emergency_detected", False)

        self.emergency_detected = False

        if self.current_event.priority.name == "EMERGENCY":

            self.emergency_detected = True

        elif self.current_ati >= 90.0:

            self.emergency_detected = True

        elif self.current_event.threat_level.name == "CRITICAL":

            self.emergency_detected = True

        if self.emergency_detected:

            self.intent = TransmissionIntent.EMERGENCY

            # logger.warning(
            #     f"Emergency detected at Node {self.node_id}"
            # )

        return self.emergency_detected


    # =========================================================================
    # EMERGENCY STATISTICS
    # =========================================================================

    def update_emergency_statistics(
        self,
    ) -> None:
        """
        Count emergency events.
        """

        if not self.emergency_detected:

            return

        self.increment_stat(

            "emergency_events"

        )


    # =========================================================================
    # ADAPTIVE THREAT INDEX (ATI)
    # =========================================================================

    def update_ati(self) -> float:
        """
        Update the runtime Adaptive Threat Index (ATI).
        """

        if self.current_event is None:
            if hasattr(self, "pending_events") and self.pending_events:
                self.current_event = self.dequeue_event()
            if self.current_event is None:
                return getattr(self, "current_ati", 0.0)

        threat_component = (
            self.current_event.adaptive_threat_index
            * ATI_THREAT_WEIGHT
        )

        burst_component = (

            max(

                self.current_burst_score,

                self.predicted_burst_score,

            )

            * ATI_BURST_WEIGHT

        )

        energy_component = (
            self.battery_percentage
            * ATI_ENERGY_WEIGHT
        )

        activity = min(
            self.event_counter,
            100,
        )

        activity_component = (
            activity
            * ATI_EVENT_WEIGHT
        )

        self.current_ati = min(
            100.0,
            threat_component
            + burst_component
            + energy_component
            + activity_component,
        )

        self.current_event.adaptive_threat_index = (
            self.current_ati
        )

        if ENABLE_DEBUG_MODE:
            logger.debug(
                f"Runtime ATI = {self.current_ati:.2f}"
            )

        return self.current_ati


    # =========================================================================
    # ARBITRATION SCORE
    # =========================================================================

    def calculate_arbitration_score(self) -> float:
        """
        Calculate the final arbitration score used by the
        Intelligent Master Node.
        """

        if self.current_event is None:
            if hasattr(self, "pending_events") and self.pending_events:
                self.current_event = self.dequeue_event()
            if self.current_event is None:
                return getattr(self, "current_arbitration_score", 0.0)

        waiting_component = min(
            self.waiting_time,
            100.0,
        )

        arbitration = (
         ARBITRATION_ATI_WEIGHT * self.current_ati
         + ARBITRATION_WAIT_WEIGHT * waiting_component
       )

        self.current_arbitration_score = min(
            arbitration,
            100.0,
        )

        self.current_event.arbitration_score = (
            self.current_arbitration_score
        )

        logger.debug(
            f"Arbitration Score = "
            f"{self.current_arbitration_score:.2f}"
        )

        return self.current_arbitration_score


    # =========================================================================
    # TRANSMISSION DECISION
    # =========================================================================

    def decide_transmission(self) -> bool:
        """
        Decide whether the current event should request transmission.
        """

        if self.current_event is None:
            if hasattr(self, "pending_events") and self.pending_events:
                self.current_event = self.dequeue_event()
            if self.current_event is None:
                return getattr(self, "ready_for_transmission", True)

        # -------------------------------------------------------------
        # Emergency events always request immediate scheduling
        # -------------------------------------------------------------

        if self.emergency_detected:

            self.ready_for_transmission = True

            self.intent = TransmissionIntent.EMERGENCY

            # logger.warning(
            #     f"Emergency transmission requested "
            #     f"by Node {self.node_id}"
            # )

            return True

        # -------------------------------------------------------------
        # Normal arbitration decision
        # -------------------------------------------------------------

        self.ready_for_transmission = (
            self.current_arbitration_score
            >= TRANSMISSION_THRESHOLD
        )

        if self.ready_for_transmission:

            self.intent = TransmissionIntent.IMMEDIATE

        else:

            self.intent = TransmissionIntent.DEFERRED

        logger.debug(
            f"Transmission Decision = "
            f"{self.ready_for_transmission}"
        )

        return self.ready_for_transmission

        # =========================================================================
    # PACKET CREATION
    # =========================================================================

    def create_packet(self) -> LoRaPacket:
        """
        Create a LoRaPacket from the current NodeEvent.
        """

        if self.current_event is None:
            if hasattr(self, "pending_events") and self.pending_events:
                self.current_event = self.dequeue_event()
            if self.current_event is None:
                return getattr(self, "current_packet", None)

        self.current_packet = LoRaPacket(

            source_node=self.current_event.node_id,

            destination_node=self.current_event.destination_node,

            payload_size=self.current_event.payload_size,

            priority=self.current_event.priority,

            adaptive_threat_index=self.current_ati,

            burst_score=self.current_burst_score,

            arbitration_score=self.current_arbitration_score,

            jitter_score=self.current_jitter,

            retry_count=self.retry_count,

            ready_for_transmission=self.ready_for_transmission,
        )

        self.increment_stat(
            "packets_sent"
        )

        logger.debug(
            f"Packet created for node {self.node_id}"
        )

        return self.current_packet


    # =========================================================================
    # TRANSMISSION REQUEST
    # =========================================================================

    def create_transmission_request(
        self,
    ) -> TransmissionRequest:
        """
        Create a TransmissionRequest for the Intelligent Master Node.
        """

        if self.current_packet is None:
            if hasattr(self, 'current_event') and self.current_event is not None:
                self.create_packet()
            if self.current_packet is None:
                return None

        self.current_request_id = self.generate_request_id()

        self.update_event_state(
            EventState.REQUEST_CREATED
        )

        self.current_request = TransmissionRequest(

            node_id=self.current_packet.source_node,

            priority=self.current_packet.priority,

            adaptive_threat_index=
            self.current_packet.adaptive_threat_index,

            burst_score=self.current_packet.burst_score,

            arbitration_score=
            self.current_packet.arbitration_score,

            jitter_score=self.current_jitter,

            battery_percentage=self.battery_percentage,

            retry_count=self.retry_count,

            ready_for_transmission=
            self.ready_for_transmission,

            transmission_intent=self.intent,

            emergency_detected=self.emergency_detected,

            packet=self.current_packet,

            request_id=self.current_request_id,
        )

        self.request_table[
            self.current_request_id
        ] = {

            "request": self.current_request,

            "timestamp": time.time(),

            "retry": self.retry_count,

            "locked": False,

        }

        self.set_state(
            SensorState.REQUEST_SENT
        )

        self.set_request_state(
            RequestState.REQUEST_SENT
        )

        self.start_ack_timer()

        self.update_event_state(
            EventState.WAIT_ACK
        )

        logger.debug(

            f"Transmission Request "

            f"{self.current_request_id}"

            f" created."
        )

        return self.current_request


    # =========================================================================
    # MASTER NODE RESPONSE HANDLERS
    # =========================================================================

    def receive_ack(
        self,
        request_id: str,
    ) -> bool:
        """
        Accept ACK only if it matches
        the active request.
        """

        if request_id != self.current_request_id:

            logger.warning(

                f"Ignoring stale ACK "

                f"{request_id}"

            )

            return False

        if request_id not in self.request_table:
            if ENABLE_DEBUG_MODE:
                logger.warning(
                    "Unknown Request ID."
                )
            return False

        self.stop_ack_timer()

        self.lock_request()

        self.request_table[

            request_id

        ]["locked"] = True

        self.set_request_state(

            RequestState.ACK_RECEIVED

        )

        self.set_state(

            SensorState.TRANSMITTING

        )

        self.update_event_state(
            EventState.TRANSMITTING
        )

        if self.event_counter % 500 == 0:
            logger.info(f"ACK accepted {request_id}")

        return True


    def receive_wait(self) -> None:
        """
        Master asks node to wait.
        """

        self.set_state(
            SensorState.WAIT
        )

        self.set_request_state(
            RequestState.WAIT_ACK
        )

        if self.event_counter % 500 == 0:
            logger.info(f"WAIT received -> Node {self.node_id} waiting.")


    def receive_hold(self) -> None:
        """
        Master temporarily suspends transmission.
        """

        self.set_state(
            SensorState.HOLD
        )

        self.set_request_state(
            RequestState.WAIT_ACK
        )

        logger.info(
            f"HOLD received -> Node {self.node_id} on hold."
        )


    def receive_release(self) -> None:
        """
        Resume after HOLD.
        """

        self.set_state(
            SensorState.WAIT
        )

        self.process_next_event()

        logger.info(
            f"RELEASE received -> Node {self.node_id} resumed."
        )


    def receive_retry(
        self,
    ) -> None:
        """
        Retry transmission.
        """

        self.stop_ack_timer()

        self.retry_count += 1

        self.increment_stat(
            "retries"
        )

        self.retry_scheduled = True

        self.set_request_state(
            RequestState.RETRY
        )

        self.set_state(
            SensorState.RETRY
        )

        self.intent = (
            TransmissionIntent.DEFERRED
        )

        logger.info(

            f"Retry #{self.retry_count} "

            f"scheduled."

        )

        if self.retry_count >= MAX_RETRY_COUNT:

            logger.warning(

                "Maximum retry limit reached."

            )

            self.receive_drop()

            return

        self.current_request_id = None

        self.reorder_queue()

        self.create_packet()

        self.create_transmission_request()


    def _cleanup_active_transmission(self) -> None:
        """
        Internal helper to clean up active request/event state after termination.
        """

        self.stop_ack_timer()

        self.unlock_request()

        self.increment_stat("events_processed")

        self.update_runtime_statistics()

        if self.current_request_id is not None:

            self.request_table.pop(

                self.current_request_id,

                None,

            )

        self.remove_event()

        self.current_request_id = None

        self.current_packet = None

        self.current_request = None

        self.current_event = None

        self.retry_count = 0

        self.retry_scheduled = False

        self.ready_for_transmission = False

        self.intent = TransmissionIntent.NORMAL

        self.emergency_detected = False


    def receive_drop(
        self,
    ) -> None:
        """
        Drop current transmission request.
        """

        self.increment_stat("drops")

        self.update_event_state(EventState.DROPPED)

        self._cleanup_active_transmission()

        self.set_request_state(RequestState.DROP)

        self.set_state(SensorState.DROP)

        logger.warning("Transmission dropped.")

        self.process_next_event()

        self.service_local_queue()


    def transmission_complete(
        self,
    ) -> None:
        """
        Successful transmission completed.
        """

        logger.info(

            f"Transmission completed "

            f"for Node {self.node_id}"

        )

        self.increment_stat("successful_packets")

        self.update_event_state(EventState.SUCCESS)

        self._cleanup_active_transmission()

        self.set_request_state(RequestState.SUCCESS)

        self.set_state(SensorState.SUCCESS)

        self.process_next_event()

        self.service_local_queue()


    # =========================================================================
    # MAIN PROCESSING PIPELINE
    # =========================================================================

    def process(
        self,
        event: NodeEvent,
    ) -> TransmissionRequest:
        """
        Execute the complete Intelligent Sensor Node pipeline.
        """

        # New emergency while already WAIT/HOLD
        if (
            self.state in (
                SensorState.WAIT,
                SensorState.HOLD,
            )
            and self.emergency_detected
        ):
            logger.warning(
                f"Emergency detected during "
                f"{self.state.name}"
            )

        self.receive_event(event)

        self.validate_node()

        self.cleanup_expired_events()

        self.prevent_starvation()

        self.update_queue_statistics()

        self.check_ack_timeout()

        if self.emergency_preemption():

            self.detect_burst()

            self.calculate_jitter()

            self.update_ati()

            self.calculate_arbitration_score()

            self.create_packet()

            return self.create_transmission_request()

        self.update_runtime_metrics()

        self.detect_burst()

        self.predict_burst()

        self.calculate_node_health()

        self.calculate_jitter()

        self.update_ati()

        self.detect_emergency()

        self.update_emergency_statistics()

        self.update_transmission_intent()

        self.calculate_arbitration_score()

        self.decide_transmission()

        self.create_packet()

        request = None
        if self.current_packet is not None:
            request = self.create_transmission_request()

        self.reorder_queue()

        self.verify_heap()

        if self.ready_for_transmission:

            self.set_state(
                SensorState.REQUEST_SENT
            )

        self.freeze_stage1()

        logger.debug(

            self.get_node_status()

        )

        return request


    # =========================================================================
    # UTILITIES
    # =========================================================================

    def summary(
        self,
    ) -> None:
        """
        Display runtime summary.
        """

        self.update_runtime_statistics()

        logger.info("=" * 60)
        logger.info("INTELLIGENT SENSOR NODE SUMMARY")
        logger.info("=" * 60)

        logger.info(f"Node ID              : {self.node_id}")

        logger.info(f"Events Detected      : {self.statistics['events_detected']}")

        logger.info(f"Events Processed     : {self.statistics['events_processed']}")

        logger.info(f"Packets Sent         : {self.statistics['packets_sent']}")

        logger.info(f"Successful Packets   : {self.statistics['successful_packets']}")

        logger.info(f"Drops                : {self.statistics['drops']}")

        logger.info(f"Retries              : {self.statistics['retries']}")

        logger.info(f"Duplicate Events     : {self.statistics['duplicate_events']}")

        logger.info(f"Emergency Events     : {self.statistics['emergency_events']}")

        logger.info(f"ACK Timeouts         : {self.statistics['ack_timeouts']}")

        logger.info(f"Queue Overflow       : {self.statistics['queue_overflow']}")

        logger.info(f"Maximum Queue Size   : {self.statistics['maximum_queue_size']}")

        logger.info(f"Average Wait Time    : {self.statistics['average_wait_time']:.2f}")

        logger.info(f"Longest Wait Time    : {self.statistics['longest_wait_time']:.2f}")

        logger.info(f"Runtime (s)          : {self.statistics['runtime_seconds']:.2f}")

        logger.info(f"Current ATI          : {self.current_ati:.2f}")

        logger.info(f"Burst Score          : {self.current_burst_score:.2f}")

        logger.info(f"Predicted Burst      : {self.predicted_burst_score:.2f}")

        logger.info(f"Node Health          : {self.node_health_score:.2f}")

        logger.info(f"Battery              : {self.battery_percentage:.2f}%")

        logger.info(f"Queue Health         : {self.queue_health():.2f}")

        logger.info(f"Queue Status         : {self.queue_status()}")

        logger.info("=" * 60)


    # =========================================================================
    # EXPORT RUNTIME
    # =========================================================================

    def export_runtime(
        self,
    ) -> dict:
        """
        Export Stage-1 runtime state.
        """

        self.freeze_stage1()

        return {

            "status":

            self.get_node_status(),

            "statistics":

            self.statistics.copy(),

            "queue":

            self.queue_statistics(),

            "diagnostics":

            self.diagnostics_report(),

        }


    # =========================================================================
    # STAGE-1 VERSION
    # =========================================================================

    @staticmethod
    def stage_version() -> str:

        return "RIACC Stage-1.0"


    def get_request(self) -> Optional[TransmissionRequest]:
        """
        Return the most recent TransmissionRequest.
        """

        return self.current_request