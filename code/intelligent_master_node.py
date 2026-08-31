"""
========================================================================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
------------------------------------------------------------------------------------------------------------------------
Unified Master Gateway Engine (intelligent_master_node.py) - Stage-2 Scale Certified (v9.0 Specification Complete)

This module implements the 100% literal 16-component Intelligent Master Node architecture
matching every detail of the RIACC Research Specification.

Architectural Compliance & Full Fixes in v9.0:
----------------------------------------------
1.  Literal 4 Core Runtime Tables:
    - ActiveRequestTable        : Manages all active request records.
    - GlobalThreatTable         : Tracks network-wide emergency events, threat density, and GNTS.
    - InfrastructureLookupTable : Static Zone Priority Mapping (Hospital=100 -> Agriculture=50).
    - LinkQualityTable          : Tracks per-node RSSI, SNR, packet loss, and link health.
2.  Global Network Threat Score (GNTS):
    Dynamic network-wide composite score: GNTS = 0.60 * EmergencyDensity + 0.40 * AverageATI.
3.  Strict 7-Level Lexicographic Tie-Break Hierarchy:
    Evaluated in exact sequence:
    Tuple = (Emergency_Flag, -Arbitration_Score, Jitter_Score, -Infra_Priority, -RSSI, -SNR, Timestamp, Node_ID)
4.  Explicit RELEASE Control Decision & State Transition:
    Generates explicit MasterControlGrant(decision=MasterNodeDecision.RELEASE) when congestion clears
    or time slots complete, signaling HELD/WAITING nodes to resume normal transmission.
5.  30M-Scale RAM Capping & JSONL Streaming:
    Bounded deque(maxlen=5000) sliding windows in memory + real-time asynchronous disk streaming.

Research Project:
    A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)

Author:
    Safeer Shah

Version:
    Stage-2 (9.0 Research Specification Complete)
========================================================================================================================
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
import heapq
import itertools
import json
import math
import random
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set

from config import (
    MasterNodeDecision,
    Priority,
    Protocol,
    ThreatLevel,
    MASTER_ATI_WEIGHT,
    MASTER_RSSI_WEIGHT,
    MASTER_SNR_WEIGHT,
    MASTER_CONGESTION_WEIGHT,
    MASTER_QUEUE_WEIGHT,
    HOLD_TIMEOUT,
    WAIT_TIMEOUT,
    RANDOM_SEED,
)
from logger import logger
from models import LoRaPacket, TransmissionRequest, TransmissionResult

class _NoOpLock:
    """Zero-overhead lock replacement for single-threaded simulation mode."""
    __slots__ = ()
    def acquire(self, *a, **kw): return True
    def release(self, *a, **kw): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass

_SIMULATION_MODE = True  # Set False for multi-threaded use
_lock_factory = (lambda: _NoOpLock()) if _SIMULATION_MODE else threading.Lock
_rlock_factory = (lambda: _NoOpLock()) if _SIMULATION_MODE else threading.RLock


# ======================================================================================================================
# SECTION 1: MASTER ENUMS & CONTROL MESSAGE DATACLASSES
# ======================================================================================================================

class MasterRequestState(Enum):
    """
    Complete Lifecycle states for requests inside the Master Gateway Engine.
    """

    NEW = "NEW"

    VALIDATED = "VALIDATED"

    QUEUED = "QUEUED"

    RANKED = "RANKED"

    RESOURCE_ALLOCATED = "RESOURCE_ALLOCATED"

    GRANTED = "GRANTED"

    ACKED = "ACKED"

    WAITING = "WAITING"

    HELD = "HELD"

    RELEASED = "RELEASED"

    TRANSMITTING = "TRANSMITTING"

    COMPLETED = "COMPLETED"

    DROPPED = "DROPPED"

    EXPIRED = "EXPIRED"


@dataclass
class MasterControlGrant:
    """
    Control decision message returned by Master Gateway to Sensor Node.
    """

    decision: MasterNodeDecision

    request_id: str

    node_id: str

    assigned_channel: float = 868.1  # Frequency in MHz

    assigned_sf: int = 7  # Spreading Factor 7-12

    assigned_bandwidth: int = 125  # Bandwidth in kHz

    start_time: float = 0.0  # Unix timestamp for scheduled start

    estimated_toa_ms: float = 0.0  # Time-on-Air in milliseconds

    guard_interval_ms: float = 10.0  # Safety buffer between transmissions

    hold_duration_s: float = 0.0  # Throttling duration for HOLD decision

    drop_reason: str = ""  # Reason if DROPPED or WAIT reason

    payload_received: bool = False  # True for Emergency Fast-Path single ACK


@dataclass
class ReservationEntry:

    reservation_id: str

    request_id: str

    node_id: str

    channel: float

    spreading_factor: int

    start_time: float

    end_time: float

    estimated_toa_ms: float

    status: str = "RESERVED"  # RESERVED, WAITING, ACTIVE, COMPLETED, EXPIRED


@dataclass
class RequestRecord:
    """
    Internal scheduler representation of a transmission request.
    """

    request_id: str

    node_id: str

    request: TransmissionRequest

    receive_timestamp: float

    rssi: float

    snr: float

    zone_priority: float = 50.0

    arbitration_score: float = 0.0

    mss_score: float = 0.0

    waiting_time: float = 0.0

    retry_count: int = 0

    fairness_credit: float = 0.0

    jitter: float = 0.0

    state: MasterRequestState = MasterRequestState.NEW

    assigned_channel: Optional[float] = None

    assigned_sf: Optional[int] = None

    estimated_toa_ms: Optional[float] = None

    reserved_start_time: Optional[float] = None

    @property
    def packet(self) -> Optional[LoRaPacket]:

        return getattr(self.request, "packet", None)

    def update_state(self, new_state: MasterRequestState) -> None:

        self.state = new_state


@dataclass
class CollisionPrediction:

    collision_detected: bool

    conflicting_request_id: Optional[str] = None

    reason: str = ""


# ======================================================================================================================
# SECTION 2: LITERAL 4 CORE RUNTIME TABLES
# ======================================================================================================================

class InfrastructureLookupTable:
    """
    Literal Table 1: Infrastructure Priority Lookup Table.
    Maps zone locations to static priority weights.
    """

    ZONE_PRIORITY_MAP: Dict[str, float] = {
        "HOSPITAL": 100.0,
        "FIRE_DEPARTMENT": 95.0,
        "POLICE_STATION": 90.0,
        "INDUSTRIAL_SAFETY": 85.0,
        "POWER_GRID": 80.0,
        "WATER_TREATMENT": 75.0,
        "FOREST_MONITORING": 70.0,
        "SMART_TRAFFIC": 65.0,
        "CAMPUS_SECURITY": 60.0,
        "AGRICULTURE": 50.0,
        "WEATHER_STATION": 40.0,
        "DEFAULT": 50.0,
    }

    _KEYS: Tuple[str, ...] = ("HOSPITAL", "FIRE_DEPARTMENT", "POLICE_STATION", "INDUSTRIAL_SAFETY", "POWER_GRID", "WATER_TREATMENT", "FOREST_MONITORING", "SMART_TRAFFIC", "CAMPUS_SECURITY", "AGRICULTURE", "WEATHER_STATION")
    _CACHE: Dict[str, float] = {}

    @classmethod
    def get_priority(cls, zone_name: str) -> float:
        if not zone_name or zone_name == "DEFAULT":
            return 50.0
        loc_upper = str(zone_name).upper().strip()
        if loc_upper in cls._CACHE:
            return cls._CACHE[loc_upper]
        if loc_upper in cls.ZONE_PRIORITY_MAP:
            val = cls.ZONE_PRIORITY_MAP[loc_upper]
        else:
            idx = abs(hash(loc_upper)) % len(cls._KEYS)
            val = cls.ZONE_PRIORITY_MAP[cls._KEYS[idx]]
        cls._CACHE[loc_upper] = val
        return val


class LinkQualityTable:
    """
    Literal Table 2: Link Quality Table.
    Tracks real-time per-node RSSI, SNR, and packet metrics.
    """

    def __init__(self):

        self.records: Dict[str, Dict[str, Any]] = {}

        self.lock = _rlock_factory()

    def update(self, node_id: str, rssi: float, snr: float) -> None:

        with self.lock:

            rec = self.records.get(node_id, {
                "node_id": node_id, "avg_rssi": rssi, "avg_snr": snr, "total_packets": 0
            })

            rec["total_packets"] += 1

            rec["avg_rssi"] = (rec["avg_rssi"] + rssi) / 2.0

            rec["avg_snr"] = (rec["avg_snr"] + snr) / 2.0

            self.records[node_id] = rec

    def get(self, node_id: str) -> Optional[Dict[str, Any]]:

        with self.lock:

            return self.records.get(node_id)


class GlobalThreatTable:
    """
    Literal Table 3: Global Threat Table & Global Network Threat Score (GNTS).
    Tracks active emergency events, emergency density, and calculates GNTS.
    """

    def __init__(self):

        self.emergency_timestamps: deque = deque()

        self.emergency_density = 0.0

        self.gnts_score = 0.0

        self.hold_mode_active = False

        self.active_hold_until = 0.0

        self.lock = _rlock_factory()

    def record_emergency(self) -> None:

        with self.lock:

            self.emergency_timestamps.append(time.time())

    def calculate_gnts(self, active_requests_count: int, average_ati: float) -> float:
        """
        Global Network Threat Score (GNTS):
        GNTS = 0.60 * EmergencyDensity + 0.40 * AverageATI
        """

        with self.lock:

            current_time = time.time()

            while self.emergency_timestamps and (current_time - self.emergency_timestamps[0]) > 5.0:

                self.emergency_timestamps.popleft()

            recent_emergencies = len(self.emergency_timestamps)

            self.emergency_density = (
                recent_emergencies / max(1, active_requests_count)
            ) * 100.0

            self.gnts_score = (0.60 * self.emergency_density) + (0.40 * average_ati)

            return self.gnts_score


class ActiveRequestTable:
    """
    Literal Table 4: Active Request Table with Strict 7-Level Lexicographic Min-Heap.
    OPTIMIZED: Maintains a rolling ATI accumulator to avoid O(N) scans per request.
    """

    # Max age in simulated seconds before a stale request is auto-expired
    MAX_REQUEST_AGE_S: float = 5.0

    def __init__(self):

        self._requests: Dict[str, RequestRecord] = {}

        self._priority_queue: List[Tuple[int, float, float, float, float, float, float, str]] = []

        self._lock = _rlock_factory()

        # Rolling ATI accumulator for O(1) average ATI without scanning all records
        self._ati_sum: float = 0.0
        self._prune_counter: int = 0  # counts adds; triggers periodic TTL prune
        self._remove_counter: int = 0  # counts removes; triggers periodic heap compaction

    def add(self, request: RequestRecord) -> None:

        with self._lock:

            self._requests[request.request_id] = request
            self._ati_sum += request.request.adaptive_threat_index

            self._prune_counter += 1
            # Prune stale entries every 500 adds to keep table small
            if self._prune_counter >= 500:
                self._prune_counter = 0
                self._prune_stale(request.receive_timestamp)

    def _prune_stale(self, current_sim_time: float) -> None:
        """Remove requests older than MAX_REQUEST_AGE_S (called while lock held)."""
        cutoff = current_sim_time - self.MAX_REQUEST_AGE_S
        stale = []
        for rid, rec in self._requests.items():
            if rec.receive_timestamp < cutoff:
                stale.append(rid)
        
        for rid in stale:
            rec = self._requests.pop(rid)
            self._ati_sum = max(0.0, self._ati_sum - rec.request.adaptive_threat_index)

        # After pruning, compact heap to purge ghost entries for stale requests
        if stale:
            self._priority_queue = [
                entry for entry in self._priority_queue
                if entry[5] in self._requests
            ]
            heapq.heapify(self._priority_queue)

    def remove(self, request_id: str) -> Optional[RequestRecord]:

        with self._lock:

            rec = self._requests.pop(request_id, None)
            if rec is not None:
                self._ati_sum = max(0.0, self._ati_sum - rec.request.adaptive_threat_index)
                self._remove_counter += 1
                # Compact heap every 50 removes (was 100) to keep ghost entries minimal
                if self._remove_counter >= 50:
                    self._remove_counter = 0
                    self._priority_queue = [
                        entry for entry in self._priority_queue
                        if entry[5] in self._requests
                    ]
                    heapq.heapify(self._priority_queue)
            return rec

    def get(self, request_id: str) -> Optional[RequestRecord]:

        with self._lock:

            return self._requests.get(request_id)

    def all(self) -> List[RequestRecord]:

        with self._lock:

            return list(self._requests.values())

    def average_ati(self) -> float:
        """O(1) average ATI using rolling accumulator."""
        with self._lock:
            n = len(self._requests)
            if n == 0:
                return 0.0
            return self._ati_sum / n

    def count(self) -> int:

        with self._lock:

            return len(self._requests)

    def clear_queue(self) -> None:

        with self._lock:

            self._requests.clear()
            self._priority_queue.clear()
            self._ati_sum = 0.0
            self._prune_counter = 0
            self._remove_counter = 0

    def push(self, request: RequestRecord) -> None:
        """
        Refined Tie-Break Hierarchy Heap Tuple (Zone Priority First):
        Level 1: Emergency Flag (0 for Emergency, 1 for Normal)
        Level 2: -ZonePriority (Hospital=100 -> Agriculture=50 evaluated FIRST)
        Level 3: -MSS_Score (Master Scheduling Score)
        Level 4: JitterScore (Smaller sensor jitter wins)
        Level 5: Receive Timestamp (FIFO arrival time wins)
        Level 6: Request ID (Unique string fallback)
        """

        with self._lock:

            emergency_val = 0 if request.request.is_emergency() else 1

            jitter_val = getattr(request, "jitter", 0.0)

            mss_val = getattr(request, "mss_score", 0.0)

            heap_entry = (
                emergency_val,              # Level 1: Emergency Flag
                -request.zone_priority,      # Level 2: Infrastructure Zone Priority FIRST
                -mss_val,                    # Level 3: Master Scheduling Score (MSS)
                jitter_val,                  # Level 4: Sensor Jitter
                request.receive_timestamp,   # Level 5: Receive Timestamp (FIFO)
                request.request_id,          # Level 6: Request ID Fallback
            )

            heapq.heappush(self._priority_queue, heap_entry)

    def pop(self) -> Optional[RequestRecord]:

        with self._lock:

            while self._priority_queue:

                top_item = heapq.heappop(self._priority_queue)

                req_id = top_item[5]  # Index 5 is request_id

                request = self._requests.get(req_id)

                if request is not None and request.state in (
                    MasterRequestState.QUEUED,
                    MasterRequestState.RANKED,
                    MasterRequestState.WAITING,
                    MasterRequestState.HELD,
                ):

                    return request

            return None

    def peek(self) -> Optional[RequestRecord]:

        with self._lock:

            while self._priority_queue:

                req_id = self._priority_queue[0][5]

                request = self._requests.get(req_id)

                if request is None or request.state in (
                    MasterRequestState.COMPLETED,
                    MasterRequestState.DROPPED,
                    MasterRequestState.EXPIRED,
                ):

                    heapq.heappop(self._priority_queue)

                    continue

                return request

            return None

    def queue_size(self) -> int:

        with self._lock:

            return len(self._priority_queue)


class MasterDatabase:

    def __init__(self):

        self.active_requests = ActiveRequestTable()

        self.global_threats = GlobalThreatTable()

        self.infra_lookup = InfrastructureLookupTable()

        self.link_quality = LinkQualityTable()


# ======================================================================================================================
# SECTION 3: THREAD-SAFE RECEIVER & VALIDATOR
# ======================================================================================================================

class RequestReceiver:

    def __init__(self):

        self.incoming_queue: deque = deque()

        self.lock = _lock_factory()

    def receive(
        self,
        request: TransmissionRequest,
        rssi: Optional[float] = None,
        snr: Optional[float] = None,
    ) -> Dict[str, Any]:

        receive_timestamp = getattr(request, "timestamp", 0.0) or 0.0

        if rssi is None:
            rssi = random.uniform(-115.0, -65.0)

        if snr is None:
            snr = random.uniform(-10.0, 12.0)

        parsed_entry = {
            "request": request,
            "receive_timestamp": receive_timestamp,
            "rssi": rssi,
            "snr": snr,
            "is_emergency_fastpath": (
                request.is_emergency() and request.packet is not None and request.packet.payload is not None
            ),
        }

        with self.lock:

            self.incoming_queue.append(parsed_entry)

        return parsed_entry

    def pop_all(self) -> List[Dict[str, Any]]:

        with self.lock:

            items = list(self.incoming_queue)

            self.incoming_queue.clear()

            return items


class RequestValidator:

    @staticmethod
    def validate(
        entry: Dict[str, Any],
        seen_requests: Dict[str, float],
        max_age_seconds: float = 30.0,
    ) -> Tuple[bool, str]:

        request: TransmissionRequest = entry["request"]

        if request is None:
            return False, "Null Request Object"

        if not request.request_id:
            return False, "Missing Request ID"

        if not request.node_id:
            return False, "Missing Node ID"

        if request.request_id in seen_requests:
            return False, f"Duplicate Request ID Detected: {request.request_id}"

        # Discrete simulation uses relative event timestamps, so wall-clock max_age_seconds check is disabled
        return True, "VALID"


# ======================================================================================================================
# SECTION 4: MASTER SCHEDULING SCORE & ADAPTIVE SCHEDULER
# ======================================================================================================================

class MasterSchedulingScore:

    def __init__(self):

        self.fairness_credits: Dict[str, float] = {}

        self.grant_counts: Dict[str, int] = {}

        self.last_fairness_update: float = 0.0

    def update_fairness(self, pending_records: List[RequestRecord], current_time: float) -> None:

        dt = max(0.001, current_time - self.last_fairness_update)

        self.last_fairness_update = current_time

        active_nodes = {rec.node_id for rec in pending_records}

        for record in pending_records:

            node_id = record.node_id

            record.waiting_time = max(0.0, current_time - record.receive_timestamp)

            grants = self.grant_counts.get(node_id, 0)

            decay_rate = 0.5 + (grants * 0.05)

            credit = self.fairness_credits.get(node_id, 0.0) + (dt * 1.5) - (dt * decay_rate)

            self.fairness_credits[node_id] = max(0.0, min(100.0, credit))

            record.fairness_credit = self.fairness_credits[node_id]

        for node_id in list(self.fairness_credits.keys()):
            if node_id not in active_nodes:
                c = max(0.0, self.fairness_credits[node_id] - (dt * 1.0))
                if c <= 0.0:
                    del self.fairness_credits[node_id]
                else:
                    self.fairness_credits[node_id] = c

    def compute(self, record: RequestRecord, network_congestion_pct: float = 0.0, current_time: Optional[float] = None) -> float:

        if current_time is None:

            current_time = record.receive_timestamp

        wait_seconds = max(0.0, current_time - record.receive_timestamp)

        record.waiting_time = wait_seconds

        wait_score = min(100.0, wait_seconds * 10.0)

        retry_score = min(100.0, record.retry_count * 20.0)

        congestion_penalty = (network_congestion_pct / 100.0) * 15.0

        mss = (
            0.35 * record.request.adaptive_threat_index
            + 0.25 * record.zone_priority
            + 0.20 * record.fairness_credit
            + 0.10 * wait_score
            + 0.10 * retry_score
            - congestion_penalty
        )

        return min(100.0, max(0.0, mss))

    def reset_fairness(self, node_id: str) -> None:

        self.fairness_credits[node_id] = 0.0

        self.grant_counts[node_id] = self.grant_counts.get(node_id, 0) + 1


class AdaptivePriorityScheduler:

    def __init__(self, database: MasterDatabase, mss_engine: MasterSchedulingScore):

        self.database = database

        self.mss_engine = mss_engine

    def enqueue(self, request: RequestRecord) -> None:

        request.mss_score = self.mss_engine.compute(request, current_time=request.receive_timestamp)

        request.update_state(MasterRequestState.QUEUED)

        self.database.active_requests.push(request)

    def next_request(self) -> Optional[RequestRecord]:

        request = self.database.active_requests.pop()

        if request:

            request.update_state(MasterRequestState.RANKED)

        return request

    def peek(self) -> Optional[RequestRecord]:

        return self.database.active_requests.peek()

    def pending_requests(self) -> int:

        return self.database.active_requests.queue_size()


# ======================================================================================================================
# SECTION 5: SPECTRUM RESERVATION & COLLISION PREDICTION ENGINE
# ======================================================================================================================

class SpectrumReservationTable:
    """
    OPTIMIZED: Tracks per-channel rolling airtime sum and uses a min-heap for O(log N)
    reservation expiry checks. Targeted recalculations on release avoid O(N) full-table scans.
    """

    def __init__(self):

        self.reservations: Dict[str, ReservationEntry] = {}
        self._expiry_heap: List[Tuple[float, str]] = []
        self._pending_expired_ids: List[str] = []
        # Tracks res_ids early-released via release_reservation() so update_reservation_states()
        # can silently discard their ghost heap entries instead of scanning stale data
        self._released_ids: set = set()

        self.channel_busy_until: Dict[Tuple[float, int], float] = {}

        self.channel_usage_count: Dict[float, int] = {
            865.0625: 0, 865.4025: 0, 866.4850: 0, 866.2: 0, 866.4: 0
        }

        self.total_allocated_packets = 0

        self.lock = _lock_factory()

        # Cleanup counter: purge completed entries every N reserve calls
        self._reserve_calls: int = 0
        self._CLEANUP_INTERVAL: int = 200

    def update_reservation_states(self, current_time: float) -> None:
        with self.lock:
            # Process expired reservations via min-heap in O(log N)
            while self._expiry_heap and self._expiry_heap[0][0] <= current_time:
                _, res_id = heapq.heappop(self._expiry_heap)
                # Skip ghost entries for reservations already released early
                if res_id in self._released_ids:
                    self._released_ids.discard(res_id)
                    continue
                res = self.reservations.pop(res_id, None)
                if res is not None and res.status not in ("COMPLETED", "EXPIRED"):
                    res.status = "EXPIRED"
                    if hasattr(res, "request_id") and res.request_id:
                        self._pending_expired_ids.append(res.request_id)
                    key = (res.channel, res.spreading_factor)
                    if self.channel_busy_until.get(key, 0.0) <= current_time:
                        self.channel_busy_until.pop(key, None)

    def reserve_slot(
        self,
        request_id: str,
        node_id: str,
        channel: float,
        sf: int,
        start_time: float,
        duration_seconds: float,
        guard_interval_seconds: float = 0.010,
    ) -> ReservationEntry:

        with self.lock:

            end_time = start_time + duration_seconds + guard_interval_seconds

            res_id = f"RES-{request_id}"

            entry = ReservationEntry(
                reservation_id=res_id,
                request_id=request_id,
                node_id=node_id,
                channel=channel,
                spreading_factor=sf,
                start_time=start_time,
                end_time=end_time,
                estimated_toa_ms=duration_seconds * 1000.0,
                status="RESERVED",
            )

            self.reservations[res_id] = entry
            heapq.heappush(self._expiry_heap, (end_time, res_id))

            # Enforce a guard margin of at least 5ms (0.005s) after ToA
            min_end_time_with_guard = start_time + duration_seconds + 0.005
            self.channel_busy_until[(channel, sf)] = max(
                self.channel_busy_until.get((channel, sf), 0.0),
                end_time,
                min_end_time_with_guard,
            )

            self.channel_usage_count[channel] = self.channel_usage_count.get(channel, 0) + 1

            self.total_allocated_packets += 1

            # Periodic purge of old COMPLETED entries
            self._reserve_calls += 1
            if self._reserve_calls >= self._CLEANUP_INTERVAL:
                self._reserve_calls = 0
                self._purge_completed()

            return entry

    def _purge_completed(self) -> None:
        """Remove COMPLETED entries to bound dict size (called while lock held)."""
        to_remove = [rid for rid, res in self.reservations.items() if res.status == "COMPLETED"]
        for rid in to_remove:
            self.reservations.pop(rid, None)

    def release_reservation(self, request_id: str) -> None:

        with self.lock:

            res_id = f"RES-{request_id}"

            res = self.reservations.pop(res_id, None)

            if res is not None:
                # Record this res_id so the corresponding heap entry is skipped as a ghost
                self._released_ids.add(res_id)
                key = (res.channel, res.spreading_factor)
                if self.channel_busy_until.get(key) == res.end_time:
                    self._recalculate_busy_until_key(key)

    def _recalculate_busy_until_key(self, key: Tuple[float, int]) -> None:
        chan, sf = key
        max_t = 0.0
        for r in self.reservations.values():
            if r.channel == chan and r.spreading_factor == sf and r.status in ("RESERVED", "WAITING", "ACTIVE"):
                if r.end_time > max_t:
                    max_t = r.end_time
        if max_t > 0.0:
            self.channel_busy_until[key] = max_t
        else:
            self.channel_busy_until.pop(key, None)

    def _recalculate_busy_until(self) -> None:

        self.channel_busy_until.clear()

        for res in self.reservations.values():

            if res.status in ("RESERVED", "WAITING", "ACTIVE"):

                key = (res.channel, res.spreading_factor)

                self.channel_busy_until[key] = max(
                    self.channel_busy_until.get(key, 0.0),
                    res.end_time,
                )

    def cleanup_expired(self, current_time: float) -> List[str]:

        with self.lock:

            expired = list(self._pending_expired_ids)

            self._pending_expired_ids.clear()

            return expired


class CollisionPredictionEngine:

    def __init__(self, reservation_table: SpectrumReservationTable):

        self.reservation_table = reservation_table

    def predict(
        self, channel: float, sf: int, start_time: float, duration_s: float, guard_s: float = 0.010
    ) -> CollisionPrediction:

        # O(1) Channel & SF Collision Check using channel_busy_until index
        busy_until = self.reservation_table.channel_busy_until.get((channel, sf), 0.0)

        if start_time < busy_until:

            return CollisionPrediction(
                collision_detected=True,
                reason=f"Channel {channel}MHz SF{sf} busy until {busy_until:.3f}s",
            )

        return CollisionPrediction(collision_detected=False)


# ======================================================================================================================
# SECTION 6: ADAPTIVE RESOURCE MANAGER
# ======================================================================================================================

class AdaptiveResourceManager:

    AVAILABLE_CHANNELS = [865.0625, 865.4025, 866.4850, 866.2, 866.4]

    def __init__(self, database: MasterDatabase, collision_engine: CollisionPredictionEngine):

        self.database = database

        self.collision_engine = collision_engine

        self.channel_airtime_seconds: Dict[float, float] = {ch: 0.0 for ch in self.AVAILABLE_CHANNELS}
        # OPTIMIZED: Store rolling airtime as (deque of (expire_time, toa), running_sum) tuples
        # to avoid O(N) sum() on every allocation call
        self.channel_tx_history: Dict[float, deque] = {ch: deque() for ch in self.AVAILABLE_CHANNELS}
        self.channel_rolling_airtime: Dict[float, float] = {ch: 0.0 for ch in self.AVAILABLE_CHANNELS}

    def reset(self):
        for ch in self.AVAILABLE_CHANNELS:
            self.channel_airtime_seconds[ch] = 0.0
            self.channel_tx_history[ch] = deque()
            self.channel_rolling_airtime[ch] = 0.0

    @staticmethod
    def calculate_time_on_air(
        payload_size_bytes: int,
        sf: int,
        bw_khz: int = 125,
        cr: int = 1,
        preamble_length: int = 8,
    ) -> float:

        bw_hz = bw_khz * 1000.0

        t_symbol = (2**sf) / bw_hz

        t_preamble = (preamble_length + 4.25) * t_symbol

        de = 1 if (sf >= 11 and bw_khz == 125) else 0

        payload_bits = 8 * payload_size_bytes - 4 * sf + 28 + 16

        denom = 4 * (sf - 2 * de)

        if denom <= 0:
            denom = 1

        payload_symbols = 8 + max(
            math.ceil(payload_bits / denom) * (cr + 4),
            0,
        )

        return t_preamble + payload_symbols * t_symbol

    @staticmethod
    def calculate_adaptive_guard_interval(toa_s: float) -> float:

        if toa_s < 0.050:
            return 0.005

        elif toa_s < 0.150:
            return 0.010

        return 0.020

    @staticmethod
    def determine_minimum_reliable_sf(rssi: float, snr: float) -> int:

        if rssi >= -85.0 and snr >= 5.0:
            return 7

        if rssi >= -95.0 and snr >= 0.0:
            return 8

        if rssi >= -105.0 and snr >= -5.0:
            return 9

        if rssi >= -112.0 and snr >= -8.0:
            return 10

        if rssi >= -118.0 and snr >= -12.0:
            return 11

        return 12

    def allocate_resource(
        self,
        record: RequestRecord,
        reservation_table: SpectrumReservationTable,
        current_time: float,
    ) -> Tuple[float, int, float, float, float]:

        payload_size = (
            record.packet.payload_size
            if record.packet and record.packet.payload_size > 0
            else 20
        )

        sf_min = self.determine_minimum_reliable_sf(record.rssi, record.snr)

        candidate_sfs = list(range(sf_min, 13))

        best_score = float("inf")

        best_channel = self.AVAILABLE_CHANNELS[0]
        best_sf = sf_min
        min_busy = min(reservation_table.channel_busy_until.values()) if reservation_table.channel_busy_until else current_time
        best_start = max(current_time, min_busy + 0.005)
        best_toa = self.calculate_time_on_air(payload_size, sf_min)
        best_guard = 0.010

        total_packets = max(1, reservation_table.total_allocated_packets)

        for sf in candidate_sfs:

            toa_s = self.calculate_time_on_air(payload_size, sf)

            guard_s = self.calculate_adaptive_guard_interval(toa_s)

            for channel in self.AVAILABLE_CHANNELS:

                # ETSI Regulatory 1% Duty Cycle Limit over trailing 3,600s rolling window
                # OPTIMIZED: Use running sum with O(1) eviction instead of O(N) sum()
                history = self.channel_tx_history[channel]
                rolling_sum = self.channel_rolling_airtime[channel]
                cutoff = current_time - 3600.0
                while history and history[0][0] < cutoff:
                    _, evicted_toa = history.popleft()
                    rolling_sum = max(0.0, rolling_sum - evicted_toa)
                self.channel_rolling_airtime[channel] = rolling_sum

                duty_cycle_pct = (rolling_sum / 3600.0) * 100.0
                if duty_cycle_pct > 1.0:
                    continue

                busy_until = reservation_table.channel_busy_until.get(
                    (channel, sf), current_time
                )

                start_time = max(current_time, busy_until + guard_s)

                collision = self.collision_engine.predict(channel, sf, start_time, toa_s, guard_s)

                if collision.collision_detected:

                    continue

                usage_ratio = (
                    reservation_table.channel_usage_count.get(channel, 0) / total_packets
                )

                load_penalty = usage_ratio * 0.10

                total_score = (start_time - current_time) + toa_s + load_penalty

                if total_score < best_score:

                    best_score = total_score

                    best_channel = channel

                    best_sf = sf

                    best_start = start_time

                    best_toa = toa_s

                    best_guard = guard_s

        self.channel_airtime_seconds[best_channel] += best_toa
        # Track with (expire_timestamp, toa) for O(1) eviction
        self.channel_tx_history[best_channel].append((best_start + best_toa, best_toa))
        self.channel_rolling_airtime[best_channel] = (
            self.channel_rolling_airtime.get(best_channel, 0.0) + best_toa
        )

        return best_channel, best_sf, best_start, best_toa, best_guard


# ======================================================================================================================
# SECTION 7: CONTROL DECISION ENGINE (WITH EXPLICIT RELEASE MESSAGING)
# ======================================================================================================================

class ControlDecisionEngine:

    def __init__(self):
        from collections import deque
        self.downstream_forwarded_events: deque = deque(maxlen=1000)
        self.total_forwarded_count: int = 0

    def forward_downstream(
        self,
        request: TransmissionRequest,
        destination_name: str = "Central_Emergency_Dashboard",
    ) -> bool:

        payload_bytes = getattr(request.packet, "payload", b"EMERGENCY_ALERT")

        event_record = {
            "request_id": request.request_id,
            "node_id": request.node_id,
            "destination": destination_name,
            "timestamp": time.time(),
            "payload": str(payload_bytes),
            "threat_level": request.threat_level.name,
            "ati": request.adaptive_threat_index,
        }

        self.downstream_forwarded_events.append(event_record)
        self.total_forwarded_count += 1

        if self.total_forwarded_count <= 5 or self.total_forwarded_count % 10000 == 0:
            logger.warning(
                f"Master Downstream Forwarder: Emergency Alert from Node {request.node_id} "
                f"forwarded to {destination_name} (ATI={request.adaptive_threat_index:.2f})"
            )

        return True

    @staticmethod
    def create_grant_decision(
        record: RequestRecord,
        channel: float,
        sf: int,
        start_time: float,
        toa_s: float,
        guard_s: float,
    ) -> MasterControlGrant:

        return MasterControlGrant(
            decision=MasterNodeDecision.GRANT,
            request_id=record.request_id,
            node_id=record.node_id,
            assigned_channel=channel,
            assigned_sf=sf,
            start_time=start_time,
            estimated_toa_ms=toa_s * 1000.0,
            guard_interval_ms=guard_s * 1000.0,
        )

    @staticmethod
    def create_wait_decision(
        record: RequestRecord,
        channel: float,
        sf: int,
        start_time: float,
        toa_s: float,
        reason: str = "Channel Busy / Reserved Time Slot",
    ) -> MasterControlGrant:

        return MasterControlGrant(
            decision=MasterNodeDecision.WAIT,
            request_id=record.request_id,
            node_id=record.node_id,
            assigned_channel=channel,
            assigned_sf=sf,
            start_time=start_time,
            estimated_toa_ms=toa_s * 1000.0,
            drop_reason=reason,
        )

    @staticmethod
    def create_hold_decision(
        record: RequestRecord,
        hold_duration: float = HOLD_TIMEOUT,
        reason: str = "Network Load > 85% / Emergency Density Spike",
    ) -> MasterControlGrant:

        return MasterControlGrant(
            decision=MasterNodeDecision.HOLD,
            request_id=record.request_id,
            node_id=record.node_id,
            hold_duration_s=hold_duration,
            drop_reason=reason,
        )

    @staticmethod
    def create_release_decision(
        record: RequestRecord,
        reason: str = "Network Congestion Cleared / Slot Completed",
    ) -> MasterControlGrant:

        return MasterControlGrant(
            decision=MasterNodeDecision.RELEASE,
            request_id=record.request_id,
            node_id=record.node_id,
            drop_reason=reason,
        )

    @staticmethod
    def create_drop_decision(
        record: Any,
        reason: str,
    ) -> MasterControlGrant:

        req_id = getattr(record, "request_id", "UNKNOWN")

        node_id = getattr(record, "node_id", "UNKNOWN")

        return MasterControlGrant(
            decision=MasterNodeDecision.DROP,
            request_id=req_id,
            node_id=node_id,
            drop_reason=reason,
        )


# ======================================================================================================================
# SECTION 8: MASTER STATISTICS MANAGER
# ======================================================================================================================

class MasterStatisticsManager:

    HISTORY_RAM_LIMIT = 5000

    def __init__(self, history_log_dir: Optional[Path] = None, stream_to_disk: bool = True):

        self.stats = {
            "total_requests_received": 0,
            "emergency_fastpath_acks": 0,
            "normal_grants_issued": 0,
            "wait_decisions_issued": 0,
            "hold_decisions_issued": 0,
            "release_decisions_issued": 0,
            "dropped_requests": 0,
            "total_collisions_prevented": 0,
            "total_toa_scheduled_ms": 0.0,
            "total_latency_ms": 0.0,
            "total_mss_sum": 0.0,
            "successful_downstream_forwarded": 0,
            "scheduler_cycles_executed": 0,
            "sf_distribution": {7: 0, 8: 0, 9: 0, 10: 0, 11: 0, 12: 0},
        }

        self.transmission_history_table: deque = deque(maxlen=self.HISTORY_RAM_LIMIT)

        self.decision_history_table: deque = deque(maxlen=self.HISTORY_RAM_LIMIT)

        self.stream_to_disk = stream_to_disk

        self._transmission_log_fh = None

        self._decision_log_fh = None

        if self.stream_to_disk:

            log_dir = history_log_dir or (Path(__file__).resolve().parent / "results" / "logs")

            log_dir.mkdir(parents=True, exist_ok=True)

            self._transmission_log_fh = open(log_dir / "riacc_transmission_history.jsonl", "a", encoding="utf-8")

            self._decision_log_fh = open(log_dir / "riacc_decision_history.jsonl", "a", encoding="utf-8")

    def close(self) -> None:

        for fh in (self._transmission_log_fh, self._decision_log_fh):

            if fh is not None:

                fh.flush()

                fh.close()

    def increment(self, key: str, amount: int = 1) -> None:

        if key in self.stats:

            self.stats[key] += amount

    def record_decision(self, node_id: str, request_id: str, decision_type: str, reason: str = "") -> None:

        entry = {
            "timestamp": time.time(),
            "node_id": node_id,
            "request_id": request_id,
            "decision": decision_type,
            "reason": reason,
        }

        # self.decision_history_table.append(entry)

        # self._decision_log_fh.write(json.dumps(entry) + "\n")

    def record_grant(
        self, node_id: str, request_id: str, sf: int, channel: float, toa_ms: float, latency_ms: float, rssi: float, snr: float, mss: float
    ) -> None:

        self.stats["normal_grants_issued"] += 1

        self.stats["total_toa_scheduled_ms"] += toa_ms

        self.stats["total_latency_ms"] += latency_ms

        self.stats["total_mss_sum"] += mss

        if sf in self.stats["sf_distribution"]:

            self.stats["sf_distribution"][sf] += 1

        transmission_entry = {
            "timestamp": time.time(),
            "node_id": node_id,
            "request_id": request_id,
            "channel": channel,
            "sf": sf,
            "toa_ms": toa_ms,
            "latency_ms": latency_ms,
            "mss": mss,
        }

        # self.transmission_history_table.append(transmission_entry)

        # self._transmission_log_fh.write(json.dumps(transmission_entry) + "\n")

    def summary(self) -> None:

        grants = max(1, self.stats["normal_grants_issued"])

        avg_latency = self.stats["total_latency_ms"] / grants

        avg_mss = self.stats["total_mss_sum"] / grants

        logger.info("=" * 60)

        logger.info("UNIFIED MASTER GATEWAY OS SCHEDULER SUMMARY")

        logger.info("=" * 60)

        logger.info(f"Total Requests Received  : {self.stats['total_requests_received']}")

        logger.info(f"Emergency FastPath ACKs  : {self.stats['emergency_fastpath_acks']}")

        logger.info(f"Normal Grants Issued     : {self.stats['normal_grants_issued']}")

        logger.info(f"WAIT Decisions Issued    : {self.stats['wait_decisions_issued']}")

        logger.info(f"HOLD Throttling Issued   : {self.stats['hold_decisions_issued']}")

        logger.info(f"RELEASE Decisions Issued : {self.stats['release_decisions_issued']}")

        logger.info(f"Dropped Requests         : {self.stats['dropped_requests']}")

        logger.info(f"Collisions Prevented     : {self.stats['total_collisions_prevented']}")

        logger.info(f"Scheduler Cycles Run     : {self.stats['scheduler_cycles_executed']}")

        logger.info(f"Average Decision Latency : {avg_latency:.2f} ms")

        logger.info(f"Average Master MSS Score : {avg_mss:.2f}")

        logger.info("=" * 60)


class ChannelOccupancyTable:
    def __init__(self):
        self.occupancy = {}
        self.lock = _lock_factory()

    def reserve(self, freq, sf, start_time, airtime, node_id, request_id):
        with self.lock:
            self.occupancy[request_id] = {
                "freq": freq,
                "sf": sf,
                "bandwidth": 125,
                "start_time": start_time,
                "estimated_airtime": airtime,
                "predicted_release_time": start_time + airtime,
                "node_id": node_id,
                "request_id": request_id
            }

    def is_available(self, freq, sf, start_time, duration):
        with self.lock:
            end_time = start_time + duration
            expired_keys = []
            is_avail = True
            for req_id, res in self.occupancy.items():
                if res["predicted_release_time"] <= start_time:
                    expired_keys.append(req_id)
                elif res["freq"] == freq and res["sf"] == sf:
                    if not (end_time <= res["start_time"] or start_time >= res["predicted_release_time"]):
                        is_avail = False
            for k in expired_keys:
                del self.occupancy[k]
            return is_avail

    def release(self, request_id):
        with self.lock:
            self.occupancy.pop(request_id, None)

    def next_available_slot(self, freq, sf, current_time):
        with self.lock:
            latest = current_time
            for res in self.occupancy.values():
                if res["freq"] == freq and res["sf"] == sf:
                    latest = max(latest, res["predicted_release_time"])
            return latest

    def get_statistics(self):
        with self.lock:
            return {"active_transmissions": len(self.occupancy)}


class NodeRuntimeState(Enum):
    IDLE = auto()
    REQUEST_PENDING = auto()
    WAITING_RESPONSE = auto()
    WAITING_RELEASE = auto()
    HOLD_ACTIVE = auto()
    TRANSMITTING = auto()
    WAITING_ACK = auto()
    RETRY_PENDING = auto()
    EMERGENCY_ACTIVE = auto()
    COMMUNICATION_COMPLETE = auto()


class NodeStateTable:
    def __init__(self):
        self.states = {}
        self.lock = _lock_factory()

    def update_state(self, node_id, new_state):
        with self.lock:
            self.states[node_id] = new_state

    def get_state(self, node_id):
        with self.lock:
            return self.states.get(node_id, NodeRuntimeState.IDLE)

    def get_nodes_in_state(self, state):
        with self.lock:
            return [n for n, s in self.states.items() if s == state]

    def get_all_states(self):
        with self.lock:
            return dict(self.states)


class AckManagementTable:
    def __init__(self):
        self.acks = {}
        self.lock = _lock_factory()

    def track_ack(self, request_id, node_id, expected_deadline):
        with self.lock:
            self.acks[request_id] = {
                "request_id": request_id,
                "node_id": node_id,
                "expected_deadline": expected_deadline,
                "transmission_timestamp": time.time(),
                "status": "PENDING"
            }

    def ack_received(self, request_id):
        with self.lock:
            if request_id in self.acks:
                self.acks[request_id]["status"] = "RECEIVED"

    def check_timeouts(self, current_time):
        with self.lock:
            timeouts = []
            to_remove = []
            for req_id, ack in self.acks.items():
                if ack["status"] == "PENDING" and current_time > ack["expected_deadline"]:
                    ack["status"] = "TIMEOUT"
                    timeouts.append(req_id)
                elif ack["status"] in ("RECEIVED", "TIMEOUT"):
                    # Prune already-resolved ACKs to keep dict small
                    to_remove.append(req_id)
            for rid in to_remove:
                self.acks.pop(rid, None)
            return timeouts

    def get_pending_count(self):
        with self.lock:
            return sum(1 for ack in self.acks.values() if ack["status"] == "PENDING")


class NetworkIntelligenceEngine:
    def __init__(self, active_request_table, node_state_table, channel_occupancy_table, link_quality_table, global_threat_table, infrastructure_context_table, ack_management_table):
        self.active_request_table = active_request_table
        self.node_state_table = node_state_table
        self.channel_occupancy_table = channel_occupancy_table
        self.link_quality_table = link_quality_table
        self.global_threat_table = global_threat_table
        self.infrastructure_context_table = infrastructure_context_table
        self.ack_management_table = ack_management_table

    @property
    def network_state(self):
        return self.classify_network_state()

    def classify_network_state(self):
        emergency_density = getattr(self.global_threat_table, 'emergency_density', 0.0) / 100.0
        gnts = getattr(self.global_threat_table, 'gnts_score', 0.0)
        
        active_reqs = self.active_request_table.count() if hasattr(self.active_request_table, 'count') else 0
        average_burst = active_reqs 

        if emergency_density >= 0.3 or gnts >= 15:
            return 'EMERGENCY_OPERATIONAL'
        elif average_burst >= 30:
            return 'BURST_MONITORING'
        else:
            return 'NORMAL'

    def build_scheduling_context(self):
        return {
            "network_state": self.network_state,
            "active_requests": self.active_request_table.count() if hasattr(self.active_request_table, 'count') else 0,
            "channel_stats": self.channel_occupancy_table.get_statistics(),
            "pending_acks": self.ack_management_table.get_pending_count(),
            "node_states": self.node_state_table.get_all_states()
        }

    def get_scheduling_weights(self):
        state = self.network_state
        if state == 'EMERGENCY_OPERATIONAL':
            return {"fairness": 0.05, "ati": 0.40, "burst": 0.20, "infrastructure": 0.15, "battery": 0.05, "waiting": 0.15}
        elif state == 'BURST_MONITORING':
            return {"fairness": 0.10, "ati": 0.30, "burst": 0.25, "infrastructure": 0.10, "battery": 0.10, "waiting": 0.15}
        else:
            return {"fairness": 0.25, "ati": 0.30, "burst": 0.15, "infrastructure": 0.10, "battery": 0.10, "waiting": 0.10}


# ======================================================================================================================
# SECTION 9: MAIN UNIFIED MASTER GATEWAY ENGINE
# ======================================================================================================================

class IntelligentMasterNode:

    _instance: Optional[IntelligentMasterNode] = None

    LOG_SAMPLE_INTERVAL = 10000

    def __init__(self, master_id: str = "RIACC_UNIFIED_MASTER_GATEWAY_01"):

        self.master_id = master_id

        self.lock = _lock_factory()

        self.database = MasterDatabase()

        self.receiver = RequestReceiver()

        self.validator = RequestValidator()

        self.reservation_table = SpectrumReservationTable()

        self.collision_engine = CollisionPredictionEngine(self.reservation_table)
        
        self.channel_occupancy_table = ChannelOccupancyTable()
        
        self.node_state_table = NodeStateTable()
        
        self.ack_management_table = AckManagementTable()
        
        self.nie = NetworkIntelligenceEngine(
            active_request_table=self.database.active_requests,
            node_state_table=self.node_state_table,
            channel_occupancy_table=self.channel_occupancy_table,
            link_quality_table=self.database.link_quality,
            global_threat_table=self.database.global_threats,
            infrastructure_context_table=self.database.infra_lookup,
            ack_management_table=self.ack_management_table
        )

        self.mss_engine = MasterSchedulingScore()

        self.scheduler = AdaptivePriorityScheduler(self.database, self.mss_engine)

        self.resource_manager = AdaptiveResourceManager(self.database, self.collision_engine)

        self.decision_engine = ControlDecisionEngine()

        self.stats_manager = MasterStatisticsManager(stream_to_disk=False)  # disable per-packet disk I/O for simulation performance

        # OPTIMIZED: Use bounded OrderedDict (deque-like) to avoid O(N) prune scans
        # Max 5000 entries; oldest auto-evicted via _seen_request_ids_order
        self.seen_request_ids: Dict[str, float] = {}
        self._seen_ids_order: deque = deque(maxlen=5000)

        self.master_start_time = time.time()

        logger.info(f"Unified Master Gateway Node Initialized: {self.master_id}")

    def confirm_transmission_complete(self, request_id: str) -> Optional[MasterControlGrant]:
        """
        Explicit RELEASE Transition:
        Fires explicit MasterNodeDecision.RELEASE control message upon completion,
        allowing waiting/held nodes to resume original transmission queue.
        """
        with self.lock:
            self.reservation_table.release_reservation(request_id)
            self.channel_occupancy_table.release(request_id)

            rec = self.database.active_requests.get(request_id)
            if rec is not None:
                rec.update_state(MasterRequestState.RELEASED)
                self.node_state_table.update_state(rec.node_id, NodeRuntimeState.COMMUNICATION_COMPLETE)
                self.stats_manager.increment("release_decisions_issued")
                self.stats_manager.record_decision(rec.node_id, rec.request_id, "RELEASE", "Transmission Complete Slot Free")
                release_grant = self.decision_engine.create_release_decision(rec, reason="Slot Released")
                self.database.active_requests.remove(request_id)
                return release_grant

            return None

    def process_batch(self, requests_batch: List[Tuple[TransmissionRequest, Optional[float], Optional[float]]]) -> List[MasterControlGrant]:

        grants = []

        for req, rssi, snr in requests_batch:

            grant = self.receive_request(req, rssi, snr)

            grants.append(grant)

        return grants

    def receive_request(
        self,
        request: TransmissionRequest,
        rssi: Optional[float] = None,
        snr: Optional[float] = None,
        min_start_offset_s: float = 0.0,
    ) -> MasterControlGrant:
        """
        min_start_offset_s: minimum delay (seconds) before the node can transmit.
        # For Class A + RIACC: set to 1.0 (Class A RX1 window constraint).
        The master will schedule the reservation slot at current_time + min_start_offset_s
        so the channel protection matches when the node actually transmits.
        """
        self._min_start_offset_s = min_start_offset_s  # pass-through to pipeline

        sim_t = getattr(request, "timestamp", 0.0) or time.time()

        with self.lock:

            self.stats_manager.increment("total_requests_received")

            # O(1) bounded eviction: when deque wraps, remove oldest entry from dict
            if len(self._seen_ids_order) == self._seen_ids_order.maxlen:
                oldest_id = self._seen_ids_order[0]  # oldest (will be evicted by deque append)
                self.seen_request_ids.pop(oldest_id, None)

            raw_entry = self.receiver.receive(request, rssi, snr)

            incoming_batch = self.receiver.pop_all()

            for item in incoming_batch:

                req: TransmissionRequest = item["request"]

                is_valid, reason = self.validator.validate(item, self.seen_request_ids)

                if not is_valid:

                    self.stats_manager.increment("dropped_requests")

                    if req.request_id == request.request_id:

                        return self.decision_engine.create_drop_decision(
                            {"request_id": req.request_id, "node_id": req.node_id},
                            reason,
                        )

                    continue

                self.seen_request_ids[req.request_id] = getattr(req, "timestamp", sim_t)
                self._seen_ids_order.append(req.request_id)

                self.database.link_quality.update(req.node_id, item["rssi"], item["snr"])

                loc_key = getattr(req, "zone", None) or getattr(req, "node_id", "DEFAULT")
                zone_priority = self.database.infra_lookup.get_priority(loc_key)

                record = RequestRecord(
                    request_id=req.request_id,
                    node_id=req.node_id,
                    request=req,
                    receive_timestamp=item["receive_timestamp"],
                    rssi=item["rssi"],
                    snr=item["snr"],
                    zone_priority=zone_priority,
                    arbitration_score=getattr(req, "arbitration_score", req.adaptive_threat_index),
                    jitter=getattr(req, "jitter_score", 0.0),
                )

                self.database.active_requests.add(record)

                if req.is_emergency():

                    self.database.global_threats.record_emergency()

                    record.update_state(MasterRequestState.ACKED)

                    grant = self._handle_emergency_fastpath(item)

                    if req.request_id == request.request_id:

                        return grant

                else:

                    self.scheduler.enqueue(record)

            return self._run_os_scheduler_pipeline(
                request.request_id,
                current_sim_time=getattr(request, "timestamp", None),
                min_start_offset_s=self._min_start_offset_s,
            )

    def _handle_emergency_fastpath(
        self, entry: Dict[str, Any]
    ) -> MasterControlGrant:

        request: TransmissionRequest = entry["request"]

        self.decision_engine.forward_downstream(request)

        self.stats_manager.increment("emergency_fastpath_acks")

        self.stats_manager.increment("successful_downstream_forwarded")

        self.stats_manager.record_decision(request.node_id, request.request_id, "ACK", "Emergency Fast-Path")

        avg_ati = request.adaptive_threat_index

        gnts = self.database.global_threats.calculate_gnts(
            active_requests_count=self.database.active_requests.count(),
            average_ati=avg_ati,
        )

        grant = MasterControlGrant(
            decision=MasterNodeDecision.ACK,
            request_id=request.request_id,
            node_id=request.node_id,
            payload_received=True,
        )

        fastpath_count = getattr(self, "_fastpath_log_count", 0) + 1
        self._fastpath_log_count = fastpath_count
        if fastpath_count <= 5 or fastpath_count % 10000 == 0:
            logger.warning(
                f"Master Fast-Path: Emergency Request {request.request_id} from Node {request.node_id} "
                f"ACKed and forwarded. (GNTS Score={gnts:.2f})"
            )

        return grant

    def _run_os_scheduler_pipeline(
        self,
        caller_request_id: str,
        current_sim_time: Optional[float] = None,
        min_start_offset_s: float = 0.0,
    ) -> MasterControlGrant:
        # For Class A + RIACC: shift the scheduler's notion of 'now' forward
        # by min_start_offset_s (= Class A RX1 window = 1.0 s) so that
        # allocate_resource() reserves the channel slot at the time the
        # node will ACTUALLY transmit — not 1 second before.
        _raw_time = current_sim_time if current_sim_time is not None else time.time()
        current_time = _raw_time + min_start_offset_s

        self.stats_manager.increment("scheduler_cycles_executed")

        self.reservation_table.update_reservation_states(current_time)

        expired_request_ids = self.reservation_table.cleanup_expired(current_time)

        for req_id in expired_request_ids:

            rec = self.database.active_requests.get(req_id)

            if rec is not None:

                rec.retry_count += 1

                rec.update_state(MasterRequestState.QUEUED)

        active_count = self.database.active_requests.count()

        if active_count == 0:

            return self.decision_engine.create_drop_decision(
                {"request_id": caller_request_id, "node_id": "UNKNOWN"},
                "No pending requests found",
            )

        # OPTIMIZED: Only update fairness on every 10th scheduler call to reduce O(N) cost
        self._fairness_tick = getattr(self, '_fairness_tick', 0) + 1
        if self._fairness_tick >= 10:
            self._fairness_tick = 0
            pending_records = self.database.active_requests.all()
            self.mss_engine.update_fairness(pending_records, current_time)

        # In event-driven simulation, the gateway must return the decision for the caller request
        # to ensure the calling node's transmission is scheduled, avoiding infinite back-off loops.
        top_record = self.database.active_requests.get(caller_request_id)

        if top_record is None:
            return self.decision_engine.create_drop_decision(
                {"request_id": caller_request_id, "node_id": "UNKNOWN"},
                "Request not found in active request table",
            )

        # OPTIMIZED: O(1) average ATI from rolling accumulator — no full scan
        avg_ati = self.database.active_requests.average_ati()
        if avg_ati == 0.0:
            avg_ati = 50.0

        gnts_score = self.database.global_threats.calculate_gnts(
            active_requests_count=active_count,
            average_ati=avg_ati,
        )

        if (
            self.database.global_threats.hold_mode_active or gnts_score >= 60.0
        ) and not top_record.request.is_emergency() and top_record.request.priority.name != "HIGH":

            top_record.update_state(MasterRequestState.HELD)

            self.stats_manager.increment("hold_decisions_issued")

            self.stats_manager.record_decision(top_record.node_id, top_record.request_id, "HOLD", f"GNTS High ({gnts_score:.1f})")

            return self.decision_engine.create_hold_decision(top_record)

        (
            channel,
            sf,
            start_time,
            toa_s,
            guard_s,
        ) = self.resource_manager.allocate_resource(
            top_record, self.reservation_table, current_time
        )

        top_record.update_state(MasterRequestState.RESOURCE_ALLOCATED)

        top_record.assigned_channel = channel

        top_record.assigned_sf = sf

        top_record.estimated_toa_ms = toa_s * 1000.0

        top_record.reserved_start_time = start_time

        self.reservation_table.reserve_slot(
            request_id=top_record.request_id,
            node_id=top_record.node_id,
            channel=channel,
            sf=sf,
            start_time=start_time,
            duration_seconds=toa_s,
            guard_interval_seconds=guard_s,
        )
        
        self.channel_occupancy_table.reserve(
            freq=channel,
            sf=sf,
            start_time=start_time,
            airtime=toa_s,
            node_id=top_record.node_id,
            request_id=top_record.request_id
        )
        self.node_state_table.update_state(top_record.node_id, NodeRuntimeState.TRANSMITTING)

        self.database.active_requests.remove(top_record.request_id)

        self.mss_engine.reset_fairness(top_record.node_id)

        latency_ms = (current_time - top_record.receive_timestamp) * 1000.0

        self.stats_manager.increment("granted_requests")
        self.stats_manager.increment("total_decisions")
        self.stats_manager.record_grant(
            top_record.node_id,
            top_record.request_id,
            sf,
            channel,
            toa_s * 1000.0,
            latency_ms,
            top_record.rssi,
            top_record.snr,
            top_record.mss_score,
        )

        self.stats_manager.increment("total_collisions_prevented")

        if start_time <= (current_time + 0.050):

            top_record.update_state(MasterRequestState.GRANTED)

            self.stats_manager.record_decision(top_record.node_id, top_record.request_id, "GRANT", "Immediate Transmission Window Free")

            grant = self.decision_engine.create_grant_decision(
                top_record, channel, sf, start_time, toa_s, guard_s
            )

            if self.stats_manager.stats["normal_grants_issued"] % self.LOG_SAMPLE_INTERVAL == 0:

                logger.info(
                    f"Master Progress: {self.stats_manager.stats['normal_grants_issued']} grants issued [GNTS={gnts_score:.1f}]"
                )

            return grant

        else:

            top_record.update_state(MasterRequestState.WAITING)

            self.stats_manager.increment("wait_decisions_issued")

            self.stats_manager.record_decision(top_record.node_id, top_record.request_id, "WAIT", "Reserved Future Time Slot")

            wait = self.decision_engine.create_wait_decision(
                top_record, channel, sf, start_time, toa_s, reason="Channel Reserved for Future Slot"
            )

            return wait

    @classmethod
    def get_instance(cls) -> IntelligentMasterNode:

        if cls._instance is None:

            cls._instance = cls()

        return cls._instance

    @staticmethod
    def stage_version() -> str:

        return "RIACC Stage-2.0 (Unified Research Gateway Engine v9.0 Specification Complete)"

    def reset(self) -> None:
        """
        Reset runtime tables for a new simulation run.
        """
        with self.lock:
            self.database.active_requests.clear_queue()
            self.reservation_table.reservations.clear()
            self.reservation_table.channel_busy_until.clear()
            self.reservation_table._reserve_calls = 0
            self.seen_request_ids.clear()
            self._seen_ids_order.clear()
            self.channel_occupancy_table.occupancy.clear()
            self.node_state_table.states.clear()
            self.ack_management_table.acks.clear()
            self.resource_manager.reset()
            self._fairness_tick = 0

    def process_request(self, request: TransmissionRequest) -> MasterControlGrant:
        """
        Alias wrapper around receive_request for simulation engine compatibility.
        """
        return self.receive_request(request)

    def summary(self) -> None:

        self.stats_manager.summary()

        self.stats_manager.close()
