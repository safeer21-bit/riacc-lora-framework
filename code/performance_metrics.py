# =============================================================================
# performance_metrics.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Performance Metrics
#
# Calculates performance indicators from simulation statistics.
# =============================================================================

from dataclasses import dataclass, asdict
from typing import Any
from statistics_manager import SimulationStatistics


# =============================================================================
# PERFORMANCE METRICS
# =============================================================================

@dataclass
class PerformanceMetrics:

    packet_delivery_ratio: float = 0.0

    packet_loss_ratio: float = 0.0

    collision_rate: float = 0.0

    throughput: float = 0.0

    average_queue_length: float = 0.0

    average_waiting_time: float = 0.0

    average_rssi: float = 0.0

    average_snr: float = 0.0

    average_ati: float = 0.0

    energy_per_packet: float = 0.0

    channel_utilization: float = 0.0

    emergency_delivery_ratio: float = 0.0

    scheduler_efficiency: float = 0.0


# =============================================================================
# PERFORMANCE ANALYZER
# =============================================================================

class PerformanceAnalyzer:

    """
    Computes research metrics from collected statistics.
    """

    def __init__(self):

        self.metrics = PerformanceMetrics()

    # ------------------------------------------------------------------

    def compute(
        self,
        statistics: Any
    ) -> PerformanceMetrics:
        """
        Compute performance metrics from dictionary or SimulationStatistics.
        """
        if isinstance(statistics, dict):

            mode = str(statistics.get("communication_mode", ""))

            sim_time = max(float(statistics.get("simulation_time", 1.0)), 1e-6)

            ch_stats = statistics.get("channel", {})

            master_stats = statistics.get("master_gateway", {})

            transmitted = float(ch_stats.get("transmitted", 0))

            delivered = float(ch_stats.get("delivered", 0))

            collisions = float(ch_stats.get("collisions", 0))

            dropped = float(ch_stats.get("dropped", 0))

            channel_busy_time = float(ch_stats.get("channel_busy_time", 0.0))

            generated = float(statistics.get("generated", statistics.get("total_generated", max(1.0, transmitted + dropped))))

            total_gen = max(1.0, generated)

            # Core KPIs measured from simulation
            self.metrics.packet_delivery_ratio = min(100.0, max(0.0, (delivered / total_gen) * 100.0))

            self.metrics.packet_loss_ratio = max(0.0, (dropped / total_gen) * 100.0)

            self.metrics.collision_rate = max(0.0, (collisions / total_gen) * 100.0)

            self.metrics.throughput = delivered / sim_time

            # Measured link quality
            rssi_sum = float(statistics.get("rssi_sum", 0.0))

            snr_sum = float(statistics.get("snr_sum", 0.0))

            ati_sum = float(statistics.get("ati_sum", 0.0))

            self.metrics.average_rssi = (rssi_sum / delivered) if delivered > 0 else 0.0

            self.metrics.average_snr = (snr_sum / delivered) if delivered > 0 else 0.0

            self.metrics.average_ati = (ati_sum / total_gen) if total_gen > 0 else 0.0

            # Measured queue & waiting time
            wait_sum = float(statistics.get("wait_sum", 0.0))

            self.metrics.average_waiting_time = (wait_sum / total_gen) if total_gen > 0 else 0.0

            queue_samples = statistics.get("queue_samples", [])

            self.metrics.average_queue_length = (sum(queue_samples) / len(queue_samples)) if queue_samples else 0.0

            # Measured energy & utilization
            energy_sum = float(statistics.get("energy_sum", 0.0))

            self.metrics.energy_per_packet = (energy_sum / delivered) if delivered > 0 else ((energy_sum / total_gen) if total_gen > 0 else 0.0)

            # Multi-channel utilization: IN865 uses 5 sub-bands total
            # (3 normal traffic channels + 2 dedicated emergency channels)
            # True spectrum occupancy = busy_time / (N_channels * sim_time)
            mode_str = str(statistics.get("communication_mode", ""))
            is_multi_ch = ("RIACC" in mode_str.upper() or "CLASS_A" in mode_str.upper())
            n_channels = 5 if is_multi_ch else 1  # Pure ALOHA uses single channel
            self.metrics.channel_utilization = (
                min(100.0, (channel_busy_time / (n_channels * sim_time)) * 100.0)
                if sim_time > 0 else 0.0
            )

            # Emergency delivery ratio (purely empirical)
            em_gen = float(statistics.get("emergency_generated", 0))
            em_del = float(statistics.get("emergency_delivered", 0))
            self.metrics.emergency_delivery_ratio = (em_del / em_gen * 100.0) if em_gen > 0 else 0.0

            # Scheduler efficiency (purely empirical)
            grants = float(master_stats.get("granted_requests", 0) or master_stats.get("normal_grants_issued", 0) + master_stats.get("emergency_fastpath_acks", 0))
            holds = float(master_stats.get("hold_decisions_issued", 0))
            waits = float(master_stats.get("wait_decisions_issued", 0))
            drops = float(master_stats.get("dropped_requests", 0))

            tot_reqs = grants + holds + waits + drops
            self.metrics.scheduler_efficiency = (grants / tot_reqs * 100.0) if tot_reqs > 0 else 0.0

            return self.metrics

        return self.evaluate(statistics)

    def evaluate(
        self,
        statistics: SimulationStatistics
    ) -> PerformanceMetrics:

        generated = statistics.generated_packets

        transmitted = statistics.transmitted_packets

        delivered = statistics.delivered_packets

        dropped = statistics.dropped_packets

        collisions = statistics.collided_packets

        simulation_time = max(statistics.simulation_time, 1e-6)

        # --------------------------------------------------------------
        # Packet Delivery Ratio
        # --------------------------------------------------------------

        if generated > 0:

            self.metrics.packet_delivery_ratio = (

                delivered / generated

            ) * 100

            self.metrics.packet_loss_ratio = (

                dropped / generated

            ) * 100

            self.metrics.collision_rate = (

                collisions / generated

            ) * 100

        # --------------------------------------------------------------
        # Throughput
        # --------------------------------------------------------------

        self.metrics.throughput = (

            delivered /

            simulation_time

        )

        # --------------------------------------------------------------
        # Queue
        # --------------------------------------------------------------

        self.metrics.average_queue_length = (

            statistics.average_queue_length

        )

        self.metrics.average_waiting_time = (

            statistics.average_waiting_time

        )

        # --------------------------------------------------------------
        # Link Quality
        # --------------------------------------------------------------

        self.metrics.average_rssi = (

            statistics.average_rssi

        )

        self.metrics.average_snr = (

            statistics.average_snr

        )

        self.metrics.average_ati = (

            statistics.average_ati

        )

        # --------------------------------------------------------------
        # Energy
        # --------------------------------------------------------------

        if delivered > 0:

            self.metrics.energy_per_packet = (

                statistics.total_energy_consumed /

                delivered

            )

        # --------------------------------------------------------------
        # Channel Utilization
        # --------------------------------------------------------------

        if statistics.active_channels > 0:

            self.metrics.channel_utilization = (

                transmitted /

                statistics.active_channels

            )

        # --------------------------------------------------------------
        # Emergency Packet Success
        # --------------------------------------------------------------

        if statistics.emergency_packets > 0:

            self.metrics.emergency_delivery_ratio = (

                statistics.granted_requests /

                statistics.emergency_packets

            ) * 100

        # --------------------------------------------------------------
        # Scheduler Efficiency
        # --------------------------------------------------------------

        total_requests = (

            statistics.granted_requests +

            statistics.waiting_requests +

            statistics.held_requests +

            statistics.rejected_requests

        )

        if total_requests > 0:

            self.metrics.scheduler_efficiency = (

                statistics.granted_requests /

                total_requests

            ) * 100

        return self.metrics

    # ------------------------------------------------------------------

    def get_metrics(self):

        return self.metrics

    # ------------------------------------------------------------------

    def as_dict(self):

        return asdict(self.metrics)

    # ------------------------------------------------------------------

    def print_summary(self):

        print("\n" + "=" * 65)
        print("                   SIMULATION PERFORMANCE REPORT                  ")
        print("=" * 65)
        print(f"  Packet Delivery Ratio (PDR)       : {self.metrics.packet_delivery_ratio:.2f} %")
        print(f"  Collision Rate                    : {self.metrics.collision_rate:.2f} %")
        print(f"  Packet Loss Ratio                 : {self.metrics.packet_loss_ratio:.2f} %")
        print(f"  Emergency Delivery Ratio          : {self.metrics.emergency_delivery_ratio:.2f} %")
        print(f"  Average Waiting Time              : {self.metrics.average_waiting_time:.4f} s")
        print(f"  Energy Consumed Per Packet        : {self.metrics.energy_per_packet:.2f} mJ")
        print(f"  Average Adaptive Threat Index(ATI): {self.metrics.average_ati:.2f}")
        print("=" * 65 + "\n")

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"PerformanceAnalyzer("
            f"PDR={self.metrics.packet_delivery_ratio:.2f}%, "
            f"Throughput={self.metrics.throughput:.2f}, "
            f"Collision={self.metrics.collision_rate:.2f}%)"

        )

    # ------------------------------------------------------------------

    @staticmethod
    def print_per_node_table(statistics: dict):
        """
        Print per-node statistics table with columns:
        Node ID | Dropped | Collisions | Burst Events | Normal Events |
        Emergency Events | Delay | Throughput | PDR

        Per PDF Chapter 4: Gateway maintains per-node runtime intelligence.
        """

        per_node = statistics.get("per_node_stats", {})

        if not per_node:
            print("\n  [No per-node statistics available]\n")
            return

        sim_time = max(float(statistics.get("simulation_time", 1.0)), 1e-6)

        # Sort nodes by ID for consistent output
        sorted_nodes = sorted(per_node.keys())
        total_nodes = len(sorted_nodes)

        print("\n" + "=" * 130)
        print(f"  PER-NODE PERFORMANCE TABLE  |  Total Nodes: {total_nodes}")
        print("=" * 130)

        # Header
        header = (
            f"{'Node ID':<20} | {'Dropped':>8} | {'Collisions':>10} | "
            f"{'Burst':>7} | {'Normal':>8} | {'Emergency':>10} | "
            f"{'Delay(s)':>10} | {'Throughput(pkts/s)':>18} | {'PDR(%)':>8}"
        )
        print(header)
        print("-" * 130)

        # Aggregate totals over all active nodes
        total_dropped = 0
        total_collisions = 0
        total_burst = 0
        total_normal = 0
        total_emergency = 0
        total_delay = 0.0
        total_delivered = 0
        total_generated = 0

        for nid in sorted_nodes:
            ns = per_node[nid]
            total_dropped += ns.get("dropped", 0)
            total_collisions += ns.get("collisions", 0)
            total_burst += ns.get("burst_events", 0)
            total_normal += ns.get("normal_events", 0)
            total_emergency += ns.get("emergency_events", 0)
            total_delay += ns.get("total_delay", 0.0)
            total_delivered += ns.get("delivered", 0)
            total_generated += ns.get("generated", 0)

        # Single overall network summary row across all nodes
        total_avg_delay = (total_delay / total_delivered) if total_delivered > 0 else 0.0
        total_throughput = total_delivered / sim_time
        total_pdr = (total_delivered / total_generated * 100.0) if total_generated > 0 else 0.0

        label = f"All Nodes ({total_nodes} Total)"
        summary_row = (
            f"{label:<20} | {total_dropped:>8} | {total_collisions:>10} | "
            f"{total_burst:>7} | {total_normal:>8} | {total_emergency:>10} | "
            f"{total_avg_delay:>10.4f} | {total_throughput:>18.6f} | {total_pdr:>8.2f}"
        )
        print(summary_row)
        print("=" * 130 + "\n")