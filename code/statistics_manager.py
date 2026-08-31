# =============================================================================
# statistics_manager.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Statistics Manager
#
# Collects raw simulation statistics.
# =============================================================================

from dataclasses import dataclass, asdict
from typing import Dict
import time


# =============================================================================
# SIMULATION STATISTICS
# =============================================================================

@dataclass
class SimulationStatistics:

    simulation_start: float = 0.0

    simulation_end: float = 0.0

    simulation_time: float = 0.0

    generated_packets: int = 0

    transmitted_packets: int = 0

    delivered_packets: int = 0

    dropped_packets: int = 0

    collided_packets: int = 0

    emergency_packets: int = 0

    granted_requests: int = 0

    waiting_requests: int = 0

    held_requests: int = 0

    rejected_requests: int = 0

    active_nodes: int = 0

    active_channels: int = 0

    queue_length: int = 0

    average_queue_length: float = 0.0

    average_waiting_time: float = 0.0

    average_rssi: float = 0.0

    average_snr: float = 0.0

    average_ati: float = 0.0

    total_energy_consumed: float = 0.0


# =============================================================================
# STATISTICS MANAGER
# =============================================================================

class StatisticsManager:

    """
    Collects raw simulation statistics.

    No KPI calculations are performed here.
    """

    def __init__(self):

        self.reset()

    # ------------------------------------------------------------------

    def reset(self):

        self.stats = SimulationStatistics()

        self.stats.simulation_start = time.time()

        self.queue_samples = []

        self.waiting_samples = []

        self.rssi_samples = []

        self.snr_samples = []

        self.ati_samples = []

    # ------------------------------------------------------------------

    def packet_generated(self):

        self.stats.generated_packets += 1

    # ------------------------------------------------------------------

    def packet_transmitted(self):

        self.stats.transmitted_packets += 1

    # ------------------------------------------------------------------

    def packet_delivered(self):

        self.stats.delivered_packets += 1

    # ------------------------------------------------------------------

    def packet_dropped(self):

        self.stats.dropped_packets += 1

    # ------------------------------------------------------------------

    def collision_detected(self):

        self.stats.collided_packets += 1

    # ------------------------------------------------------------------

    def emergency_packet(self):

        self.stats.emergency_packets += 1

    # ------------------------------------------------------------------

    def request_granted(self):

        self.stats.granted_requests += 1

    # ------------------------------------------------------------------

    def request_waiting(self):

        self.stats.waiting_requests += 1

    # ------------------------------------------------------------------

    def request_held(self):

        self.stats.held_requests += 1

    # ------------------------------------------------------------------

    def request_rejected(self):

        self.stats.rejected_requests += 1

    # ------------------------------------------------------------------

    def update_queue_length(
        self,
        queue_length: int
    ):

        self.stats.queue_length = queue_length

        self.queue_samples.append(queue_length)

    # ------------------------------------------------------------------

    def update_active_nodes(
        self,
        nodes: int
    ):

        self.stats.active_nodes = nodes

    # ------------------------------------------------------------------

    def update_active_channels(
        self,
        channels: int
    ):

        self.stats.active_channels = channels

    # ------------------------------------------------------------------

    def add_waiting_time(
        self,
        waiting_time: float
    ):

        self.waiting_samples.append(waiting_time)

    # ------------------------------------------------------------------

    def add_rssi(
        self,
        rssi: float
    ):

        self.rssi_samples.append(rssi)

    # ------------------------------------------------------------------

    def add_snr(
        self,
        snr: float
    ):

        self.snr_samples.append(snr)

    # ------------------------------------------------------------------

    def add_ati(
        self,
        ati: float
    ):

        self.ati_samples.append(ati)

    # ------------------------------------------------------------------

    def add_energy(
        self,
        energy: float
    ):

        self.stats.total_energy_consumed += energy

    # ------------------------------------------------------------------

    def finalize(self):

        self.stats.simulation_end = time.time()

        self.stats.simulation_time = (

            self.stats.simulation_end -

            self.stats.simulation_start

        )

        if self.queue_samples:

            self.stats.average_queue_length = (

                sum(self.queue_samples) /

                len(self.queue_samples)

            )

        if self.waiting_samples:

            self.stats.average_waiting_time = (

                sum(self.waiting_samples) /

                len(self.waiting_samples)

            )

        if self.rssi_samples:

            self.stats.average_rssi = (

                sum(self.rssi_samples) /

                len(self.rssi_samples)

            )

        if self.snr_samples:

            self.stats.average_snr = (

                sum(self.snr_samples) /

                len(self.snr_samples)

            )

        if self.ati_samples:

            self.stats.average_ati = (

                sum(self.ati_samples) /

                len(self.ati_samples)

            )

    # ------------------------------------------------------------------

    def get_statistics(self) -> SimulationStatistics:

        return self.stats

    # ------------------------------------------------------------------

    def as_dict(self) -> Dict:

        return asdict(self.stats)

    # ------------------------------------------------------------------

    def print_summary(self):

        self.finalize()

        print("\n==============================")

        print(" SIMULATION STATISTICS ")

        print("==============================")

        for key, value in self.as_dict().items():

            print(f"{key:30} : {value}")

        print("==============================")

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"StatisticsManager("
            f"Generated={self.stats.generated_packets}, "
            f"Delivered={self.stats.delivered_packets}, "
            f"Collisions={self.stats.collided_packets})"

        )