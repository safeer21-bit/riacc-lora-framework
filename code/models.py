"""
===============================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
-------------------------------------------------------------------------------
Shared Data Models

This module contains all shared data structures used throughout the
RIACC Research Simulator.

Guidelines
----------
• Contains ONLY dataclasses and type definitions.
• No simulation logic.
• No calculations.
• No scheduling algorithms.
• No file operations.

Every simulator module exchanges these standardized objects.

Research Project:
    A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)

Author:
    Safeer Shah

Version:
    1.0
===============================================================================
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import uuid4
import itertools

from config import (
    Priority,
    ThreatLevel,
    NodeState,
    PacketStatus,
    MasterNodeDecision,
    Protocol,
    ChannelState,
)

# Fast counter-based ID generator (replaces uuid4 in hot paths)
_fast_id_counter = itertools.count(1)
def _fast_id() -> str:
    return f"ID_{next(_fast_id_counter)}"


# =============================================================================
# COMMON BASE MODELS
# =============================================================================

@dataclass(slots=True)
class BaseModel:
    """
    Base class inherited by all RIACC data models.

    Provides:
        - Unique object identifier
        - Creation timestamp
    """

    uid: str = field(default_factory=_fast_id)
    created_at: Optional[datetime] = None


@dataclass(slots=True)
class Position:
    """
    Physical position of a sensor node.
    """

    x: float = 0.0

    y: float = 0.0

    z: float = 0.0


@dataclass(slots=True)
class RadioMetrics:
    """
    Wireless channel measurements.
    """

    rssi: Optional[float] = None

    snr: Optional[float] = None

    link_quality: Optional[float] = None

    path_loss: Optional[float] = None


@dataclass(slots=True)
class EnergyStatus:
    """
    Current energy information of a sensor node.
    """

    battery_percentage: float = 100.0

    remaining_energy_mAh: Optional[float] = None

    tx_energy: float = 0.0

    rx_energy: float = 0.0

    idle_energy: float = 0.0

    processing_energy: float = 0.0

    sleep_energy: float = 0.0


@dataclass(slots=True)
class QueueStatus:
    """
    Current transmission queue information.
    """

    queue_length: int = 0

    queue_capacity: int = 0

    average_wait_time: float = 0.0

    dropped_packets: int = 0


@dataclass(slots=True)
class SimulationContext:
    """
    Global simulation information shared between modules.
    """

    protocol: Protocol

    simulation_time: float = 0.0

    event_number: int = 0

    channel_state: ChannelState = ChannelState.IDLE

    current_round: int = 0


    # =============================================================================
# DATASET MODELS
# =============================================================================


@dataclass(slots=True)
class DatasetEvent(BaseModel):
    """
    Raw event loaded directly from the TON-IoT dataset.

    This class stores the original dataset information before any
    preprocessing or feature engineering.
    """

    timestamp: float = 0.0

    source_ip: str = ""

    destination_ip: str = ""

    source_port: Optional[int] = None

    destination_port: Optional[int] = None

    protocol: str = ""

    service: str = ""

    duration: float = 0.0

    payload_size: Optional[int] = None

    response_size: Optional[int] = None

    attack_type: str = "normal"

    label: int = 0

    raw_record: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NodeEvent(BaseModel):
    """
    Processed event used by the Intelligent Sensor Node.

    Generated after feature engineering.
    This is the primary object exchanged between simulator modules.
    """

    # -------------------------------------------------------------------------
    # Dataset Information
    # -------------------------------------------------------------------------

    timestamp: float = 0.0

    node_id: str = ""

    destination_node: str = ""

    protocol: str = ""

    service: str = ""

    payload_size: int = 0

    attack_type: str = "normal"

    label: int = 0


    # -------------------------------------------------------------------------
    # Intelligent Sensor Node Information
    # -------------------------------------------------------------------------

    threat_level: ThreatLevel = ThreatLevel.NORMAL

    priority: Priority = Priority.LOW

    node_state: NodeState = NodeState.IDLE


    # -------------------------------------------------------------------------
    # Runtime Information
    # -------------------------------------------------------------------------

    position: Optional[Position] = None

    energy: Optional[EnergyStatus] = None

    radio: Optional[RadioMetrics] = None

    queue: Optional[QueueStatus] = None


    # -------------------------------------------------------------------------
    # Stage 1 : Intelligent Sensor Node
    # -------------------------------------------------------------------------

    burst_score: float = 0.0

    adaptive_threat_index: float = 0.0


    # -------------------------------------------------------------------------
    # Stage 2 : Node Arbitration
    # -------------------------------------------------------------------------

    waiting_time: float = 0.0

    previous_ack: bool = False

    arbitration_score: float = 0.0

    wait_start_time: float = 0.0

    priority_boost: float = 0.0

    occurrence_count: int = 1

    first_detected: float = 0.0

    last_detected: float = 0.0


    # =============================================================================
# COMMUNICATION MODELS
# =============================================================================


@dataclass(slots=True)
class LoRaPacket(BaseModel):
    """
    Represents a LoRa packet transmitted over the wireless channel.
    """

    packet_id: str = field(default_factory=_fast_id)

    source_node: str = ""

    destination_node: str = ""

    protocol: Protocol = Protocol.LORAWAN_CLASS_A_ADR_RIACC

    payload_size: int = 0

    priority: Priority = Priority.LOW

    threat_level: ThreatLevel = ThreatLevel.NORMAL

    packet_status: PacketStatus = PacketStatus.GENERATED

    time_on_air: float = 0.0

    transmission_power: int = 14

    spreading_factor: int = 7

    coding_rate: str = "4/5"

    bandwidth: int = 125000

    frequency: float = 865000000

    timestamp: float = 0.0

    retry_count: int = 0

    confirmed_message: bool = True

    payload: Optional[bytes] = None


    # -------------------------------------------------------------------------
    # Intelligent Sensor Node Scores
    # -------------------------------------------------------------------------

    adaptive_threat_index: float = 0.0

    burst_score: float = 0.0

    arbitration_score: float = 0.0

    jitter_score: float = 0.0


    # -------------------------------------------------------------------------
    # Transmission State
    # -------------------------------------------------------------------------

    ready_for_transmission: bool = False

    transmission_time: Optional[float] = None


    def transmission_weight(self) -> float:
        """
        Returns a combined packet weight.
        """

        return (

            self.adaptive_threat_index

            + self.burst_score

            + self.arbitration_score

            + self.jitter_score

        )


@dataclass(slots=True)
class TransmissionRequest(BaseModel):
    """
    Transmission request generated by an Intelligent Sensor Node and
    forwarded to the Intelligent Master Node.
    """

    node_id: str = ""

    packet: Optional[LoRaPacket] = None

    adaptive_threat_index: float = 0.0

    arbitration_score: float = 0.0

    priority: Priority = Priority.LOW

    threat_level: ThreatLevel = ThreatLevel.NORMAL

    battery_percentage: float = 100.0

    burst_score: float = 0.0

    waiting_time: float = 0.0

    retry_count: int = 0

    jitter_score: float = 0.0

    transmission_intent: object = None

    emergency_detected: bool = False

    request_id: str = ""

    timestamp: float = 0.0

    ready_for_transmission: bool = True

    frequency: float = 865.0625

    spreading_factor: int = 9

    rssi: float = -85.0

    snr: float = 5.0

    em_scheduled: bool = False

    hold_count: int = 0

    guard_count: int = 0


    def is_emergency(self) -> bool:
        """
        Returns True if the request is an emergency.
        """

        return self.emergency_detected


@dataclass(slots=True)
class TransmissionResult(BaseModel):
    """
    Result returned after packet transmission.
    """

    packet_id: str = ""

    success: bool = False

    packet_status: PacketStatus = PacketStatus.GENERATED

    decision: MasterNodeDecision = MasterNodeDecision.ACK

    rssi: Optional[float] = None

    snr: Optional[float] = None

    collision: bool = False

    acknowledgement_received: bool = False

    retransmission_required: bool = False

    transmission_delay: float = 0.0

    timestamp: float = 0.0

    # =============================================================================
# INTELLIGENT MASTER NODE MODELS
# =============================================================================


@dataclass(slots=True)
class NetworkStatus(BaseModel):
    """
    Current network state observed by the Intelligent Master Node.
    """

    simulation_time: float = 0.0

    active_nodes: int = 0

    transmitting_nodes: int = 0

    waiting_nodes: int = 0

    queued_packets: int = 0

    channel_utilization: float = 0.0

    congestion_level: float = 0.0

    collision_rate: float = 0.0

    average_rssi: float = 0.0

    average_snr: float = 0.0


@dataclass(slots=True)
class SchedulingDecision(BaseModel):
    """
    Decision generated by the Intelligent Master Node Scheduler.
    """

    node_id: str = ""

    packet_id: str = ""

    decision: MasterNodeDecision = MasterNodeDecision.ACK

    priority: Priority = Priority.LOW

    arbitration_score: float = 0.0

    scheduling_score: float = 0.0

    queue_position: int = 0

    scheduled_time: float = 0.0

    reason: str = ""


@dataclass(slots=True)
class MasterNodeStatus(BaseModel):
    """
    Runtime status of the Intelligent Master Node.
    """

    master_node_id: str = "MASTER_NODE"

    processed_packets: int = 0

    successful_packets: int = 0

    dropped_packets: int = 0

    waiting_packets: int = 0

    retransmissions: int = 0

    queue_length: int = 0

    channel_state: ChannelState = ChannelState.IDLE

    network_status: Optional[NetworkStatus] = None


@dataclass(slots=True)
class ADRStatus(BaseModel):
    """
    Adaptive Data Rate (ADR) information for a sensor node.
    """

    enabled: bool = True

    data_rate: int = 0

    spreading_factor: int = 7

    transmission_power: int = 14

    adr_margin: float = 10.0

    history_size: int = 20

    successful_updates: int = 0

    failed_updates: int = 0

    # =============================================================================
# SIMULATION MODELS
# =============================================================================


@dataclass(slots=True)
class SimulationEvent(BaseModel):
    """
    Represents a single event inside the simulator.
    """

    event_id: str = field(default_factory=lambda: str(uuid4()))

    event_time: float = 0.0

    event_type: str = ""

    node_id: str = ""

    timestamp: float = 0.0

    ati: float = 0.0

    battery: float = 100.0

    rssi: float = -80.0

    snr: float = 5.0

    emergency: bool = False

    packet_id: Optional[str] = None

    protocol: Protocol = Protocol.LORAWAN_CLASS_A_ADR_RIACC

    processed: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EventQueue(BaseModel):
    """
    Runtime event queue.
    """

    queue_id: str = "EVENT_QUEUE"

    pending_events: List[SimulationEvent] = field(default_factory=list)

    total_events: int = 0

    processed_events: int = 0

    dropped_events: int = 0


@dataclass(slots=True)
class SimulationClock(BaseModel):
    """
    Global simulation clock.
    """

    current_time: float = 0.0

    start_time: float = 0.0

    end_time: float = 0.0

    time_step: float = 0.001

    current_iteration: int = 0


@dataclass(slots=True)
class SimulationState(BaseModel):
    """
    Overall simulation state.
    """

    protocol: Protocol = Protocol.LORAWAN_CLASS_A_ADR_RIACC

    running: bool = False

    paused: bool = False

    completed: bool = False

    simulation_clock: Optional[SimulationClock] = None

    event_queue: Optional[EventQueue] = None

    network_status: Optional[NetworkStatus] = None


    # =============================================================================
# STATISTICS MODELS
# =============================================================================


@dataclass(slots=True)
class NodeStatistics(BaseModel):
    """
    Performance statistics of an individual sensor node.
    """

    node_id: str = ""

    generated_packets: int = 0

    transmitted_packets: int = 0

    successful_packets: int = 0

    dropped_packets: int = 0

    retransmissions: int = 0

    collisions: int = 0

    average_delay: float = 0.0

    average_rssi: float = 0.0

    average_snr: float = 0.0

    remaining_battery: float = 100.0


@dataclass(slots=True)
class NetworkStatistics(BaseModel):
    """
    Overall network performance statistics.
    """

    total_nodes: int = 0

    total_packets: int = 0

    successful_packets: int = 0

    dropped_packets: int = 0

    collided_packets: int = 0

    retransmissions: int = 0

    packet_delivery_ratio: float = 0.0

    packet_loss_ratio: float = 0.0

    throughput: float = 0.0

    average_delay: float = 0.0

    average_latency: float = 0.0

    average_jitter: float = 0.0

    channel_utilization: float = 0.0

    total_energy_consumption: float = 0.0

    network_lifetime: float = 0.0


@dataclass(slots=True)
class RIACCStatistics(BaseModel):
    """
    Performance statistics of the RIACC framework.
    """

    average_ati: float = 0.0

    average_burst_score: float = 0.0

    average_arbitration_score: float = 0.0

    average_priority: float = 0.0

    ack_count: int = 0

    wait_count: int = 0

    hold_count: int = 0

    release_count: int = 0

    scheduler_decisions: int = 0

    queue_peak_length: int = 0

    average_queue_length: float = 0.0


@dataclass(slots=True)
class SimulationStatistics(BaseModel):
    """
    Complete statistics produced after one simulation run.
    """

    protocol: Protocol = Protocol.LORAWAN_CLASS_A_ADR_RIACC

    network: NetworkStatistics = field(default_factory=NetworkStatistics)

    riacc: RIACCStatistics = field(default_factory=RIACCStatistics)

    node_statistics: List[NodeStatistics] = field(default_factory=list)

    execution_time: float = 0.0

    simulation_completed: bool = False

    # =============================================================================
# EXPERIMENT MODELS
# =============================================================================


@dataclass(slots=True)
class ProtocolResult(BaseModel):
    """
    Results produced by a single communication protocol.
    """

    protocol: Protocol = Protocol.LORAWAN_CLASS_A_ADR_RIACC

    simulation_statistics: SimulationStatistics = field(
        default_factory=SimulationStatistics
    )

    execution_time: float = 0.0

    completed: bool = False

    notes: str = ""


@dataclass(slots=True)
class ComparisonResult(BaseModel):
    """
    Comparison between all evaluated communication protocols.
    """

    protocol_results: List[ProtocolResult] = field(default_factory=list)

    best_packet_delivery_ratio: Optional[Protocol] = None

    best_throughput: Optional[Protocol] = None

    lowest_delay: Optional[Protocol] = None

    lowest_packet_loss: Optional[Protocol] = None

    lowest_energy_consumption: Optional[Protocol] = None

    longest_network_lifetime: Optional[Protocol] = None

    lowest_collision_rate: Optional[Protocol] = None


@dataclass(slots=True)
class ExperimentConfiguration(BaseModel):
    """
    Configuration used for a simulation experiment.
    """

    experiment_name: str = ""

    protocol: Protocol = Protocol.LORAWAN_CLASS_A_ADR_RIACC

    random_seed: int = 42

    total_nodes: int = 0

    simulation_time: float = 0.0

    dataset_name: str = ""

    repetitions: int = 1


@dataclass(slots=True)
class ExperimentResult(BaseModel):
    """
    Final research output produced by one complete experiment.
    """

    configuration: ExperimentConfiguration = field(
        default_factory=ExperimentConfiguration
    )

    comparison: ComparisonResult = field(
        default_factory=ComparisonResult
    )

    generated_timestamp: datetime = field(
        default_factory=datetime.utcnow
    )

    ieee_ready: bool = True

    validated: bool = False