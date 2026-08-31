# =============================================================================
# simulation_engine.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Simulation Engine
# =============================================================================

import gc
import zlib
from typing import Optional

from simulation_clock import SimulationClock
from event_queue import EventQueue
from packet_builder import PacketBuilder
from lora_channel import LoRaChannel

from models import TransmissionRequest, NodeEvent
from config import Priority, ThreatLevel
from intelligent_master_node import IntelligentMasterNode, MasterNodeDecision
from intelligent_sensor_node import IntelligentSensorNode, SensorState, RequestState, EventState
from comparison_engine import CommunicationMode
from logger import logger


def det_hash(s: str) -> int:
    return zlib.crc32(s.encode('utf-8'))


# =============================================================================
# SIMULATION ENGINE
# =============================================================================

class SimulationEngine:
    """
    Discrete Event Simulation Engine.

    Responsibilities
    ----------------
    • Generate packets
    • Schedule transmissions
    • Advance simulation time
    • Deliver packets
    • Execute Intelligent Master Node
    """

    def __init__(
        self,
        master_node: Optional[IntelligentMasterNode] = None
    ):

        self.clock = SimulationClock()

        self.event_queue = EventQueue()

        self.packet_builder = PacketBuilder()

        self.channel = LoRaChannel()

        self.master_node = master_node or IntelligentMasterNode.get_instance()

        self.running = False

        self.communication_mode = CommunicationMode.PURE_ALOHA

        # Pre-computed mode flags (set once in set_communication_mode, not per-packet)
        self._is_riacc = False
        self._is_class_a = False
        self._is_pure_aloha_riacc = False
        self._is_class_a_riacc = False

        # Real metric accumulators
        self._generated = 0
        self._rssi_sum = 0.0
        self._snr_sum = 0.0
        self._ati_sum = 0.0
        self._energy_sum = 0.0          # mJ
        self._wait_sum = 0.0            # seconds
        self._enqueue_times = {}        # request_id -> enqueue time
        self._emergency_generated = 0
        self._emergency_delivered = 0
        # O(1) running counters — avoids O(N) sum() over per_node_stats in emit_live_stats()
        self._normal_total = 0
        self._emergency_total = 0
        self._queue_samples = []        # sampled queue length per step
        self._sf_channel_counts = {}
        self._time_series = []
        # Previous-snapshot scalars for instantaneous differential fields (4 ints, zero RAM overhead)
        self._prev_snap_deliv = 0
        self._prev_snap_gen   = 0
        self._prev_snap_coll  = 0
        self._prev_snap_energy_mj = 0.0
        # RIACC novelty instantaneous prev-snapshot scalars (3 more — zero RAM overhead)
        self._prev_snap_emg_del  = 0   # emergency delivered
        self._prev_snap_fp_acks  = 0   # fastpath acks
        self._prev_snap_hold     = 0   # hold decisions
        self._prev_snap_ati_sum  = 0.0 # ATI sum for instantaneous ATI per window
        # Dedicated step-level snapshot counter — increments inside step(), never reset by load_events()
        # Fixes: _generated is set at chunk boundaries (always multiples of 20) causing every step() to snapshot
        self._snap_counter = 0

    # ------------------------------------------------------------------

    def initialize(self):

        self.clock.reset()

        self.channel.reset()

        self.event_queue.clear()

        self.master_node.reset()

        self._generated = 0
        self._rssi_sum = 0.0
        self._snr_sum = 0.0
        self._ati_sum = 0.0
        self._energy_sum = 0.0
        self._wait_sum = 0.0
        self._enqueue_times.clear()
        self._emergency_generated = 0
        self._emergency_delivered = 0
        self._normal_total = 0
        self._emergency_total = 0
        self._queue_samples.clear()
        self._channel_busy_until = {}
        self._seen_emergency_ids: set = set()  # OPTIMIZED: avoid re-logging same emergency
        self._last_tx_freq = 865.0625
        self._last_tx_sf = 7
        self._sf_channel_counts.clear()
        self._time_series.clear()
        self._last_snapshot_sec = -1
        self._prev_snap_deliv     = 0
        self._prev_snap_gen       = 0
        self._prev_snap_coll      = 0
        self._prev_snap_energy_mj = 0.0
        self._prev_snap_emg_del   = 0
        self._prev_snap_fp_acks   = 0
        self._prev_snap_hold      = 0
        self._prev_snap_ati_sum   = 0.0
        self._snap_counter        = 0

        # Per-node statistics tracking (PDF Chapter 4 requirement)
        self._per_node_stats = {}  # node_id -> dict of counters
        self.sensor_nodes = {}     # node_id -> IntelligentSensorNode instance

    # ------------------------------------------------------------------

    def set_communication_mode(
        self,
        mode: CommunicationMode
    ):
        """
        Sets the communication strategy used during simulation.
        Pre-computes mode boolean flags once so transmit_packet() avoids
        repeated string 'in' scans on every single packet.
        """
        self.communication_mode = mode
        m_name = mode.name
        m_val  = mode.value
        self._is_riacc           = "RIACC" in m_name or "RIACC" in m_val
        self._is_class_a         = "CLASS_A" in m_name
        self._is_pure_aloha_riacc = ("PURE_ALOHA" in m_name and self._is_riacc) or ("Pure ALOHA" in m_val and self._is_riacc)
        self._is_class_a_riacc   = ("CLASS_A" in m_name and self._is_riacc) or ("Class A" in m_val and self._is_riacc)

    # ------------------------------------------------------------------

    def load_events(
        self,
        events,
        mode: Optional[CommunicationMode] = None
    ):
        """
        Converts surveillance events into packets and schedules them.
        """

        active_mode = mode or self.communication_mode

        packets = self.packet_builder.build_batch(
            events,
            mode=active_mode
        )

        for packet in packets:

            self.schedule_packet(packet)

    # ------------------------------------------------------------------

    def _ensure_node_stats(self, node_id: str) -> dict:
        """Ensure per-node stats dict exists for a given node and return it."""
        st = self._per_node_stats.get(node_id)
        if st is None:
            st = {
                "generated": 0,
                "delivered": 0,
                "dropped": 0,
                "collisions": 0,
                "emergency_events": 0,
                "normal_events": 0,
                "burst_events": 0,
                "total_delay": 0.0,
                "total_energy": 0.0,
            }
            self._per_node_stats[node_id] = st
        return st

    def schedule_packet(
        self,
        packet: TransmissionRequest
    ):
        self._generated += 1

        self._ati_sum += getattr(packet, "adaptive_threat_index", 0.0)

        is_emergency = getattr(packet, "emergency_detected", False)
        if is_emergency:
            self._emergency_generated += 1
            self._emergency_total += 1
        else:
            self._normal_total += 1

        self._enqueue_times[packet.request_id] = packet.timestamp
        # Cap enqueue_times dict: if it grows past 50 000 entries, evict the oldest
        # 10 000 to prevent unbounded RAM growth under extreme sustained load.
        if len(self._enqueue_times) > 50000:
            excess = list(self._enqueue_times.keys())[:10000]
            for k in excess:
                self._enqueue_times.pop(k, None)

        # Per-node tracking (O(1) cached lookup)
        nid = getattr(packet, "node_id", "UNKNOWN")
        nst = self._ensure_node_stats(nid)
        nst["generated"] += 1

        if is_emergency:
            nst["emergency_events"] += 1
        else:
            nst["normal_events"] += 1

        # Track burst events (ATI >= 40 but not emergency)
        ati_val = getattr(packet, "adaptive_threat_index", 0.0)
        if not is_emergency and ati_val >= 40.0:
            nst["burst_events"] += 1

        # High-performance sampled event trace logging (every 500th event for dynamic live terminal view)
        if self._generated % 500 == 1:
            mode_name = self.communication_mode.name

            if "RIACC" in mode_name:
                # RIACC: show full Stage-1 intelligence trace with ATI and gateway arbitration
                status_tag = "EMERGENCY (Fast-Path)" if is_emergency else ("BURST (ATI>={:.0f})".format(ati_val) if ati_val >= 40.0 else "Normal")
                sensor_action = "Queued --> TX Request --> [GATEWAY] Arbitrated --> GRANT"
                logger.info(
                    f"[SENSOR] {nid:<18} --> Sensed ({status_tag}) --> ATI={ati_val:>5.1f} --> {sensor_action}"
                )
            elif "CLASS_A" in mode_name:
                # Baseline Class A: standard LoRaWAN uplink, no intelligence markers
                pkt_type = "Emergency Uplink" if is_emergency else "Normal Uplink"
                sf_used = getattr(packet, 'spreading_factor', 9) if hasattr(packet, 'spreading_factor') else 9
                logger.info(
                    f"[PKT]    {nid:<18} --> {pkt_type} --> LoRaWAN Class A TX (SF={sf_used}, BW=125kHz, TP=14dBm, No RIACC)"
                )
            else:
                # Baseline Pure ALOHA: uncoordinated unslotted, no intelligence markers
                pkt_type = "Emergency" if is_emergency else "Normal"
                logger.info(
                    f"[PKT]    {nid:<18} --> {pkt_type} Packet --> Unslotted ALOHA TX (Random Access, No Coordination)"
                )

        # Instantiate IntelligentSensorNode for the node if not present
        if nid not in self.sensor_nodes:
            self.sensor_nodes[nid] = IntelligentSensorNode(nid)

        if self._is_riacc:
            # RIACC modes: route through full Stage-1 intelligence pipeline
            # (burst detection, ATI, arbitration score, state machine)
            event_obj = NodeEvent(
                timestamp=packet.timestamp,
                node_id=nid,
                payload_size=32,
                attack_type="normal" if not is_emergency else "critical",
                priority=Priority.EMERGENCY if is_emergency else Priority.LOW,
                threat_level=ThreatLevel.CRITICAL if is_emergency else ThreatLevel.NORMAL,
                adaptive_threat_index=ati_val
            )
            self.sensor_nodes[nid].process(event_obj)  # Invoke Stage-1 node processing & state machine
        # Baseline modes (Pure ALOHA / Class A without RIACC): nodes are stateless standard devices.
        # Stage-1 intelligence is not present. Only telemetry stat tracking is needed.

        self.event_queue.schedule(

            event_time=packet.timestamp,

            event_type="TRANSMIT",

            callback=self.transmit_packet,

            packet=packet,

            description="Packet Transmission"

        )

    # ------------------------------------------------------------------

    def transmit_packet(
        self,
        packet: TransmissionRequest
    ):
        toa = self.packet_builder.estimate_time_on_air(
            packet
        )

        # Track active frequency channel and SF for dynamic terminal status
        pfreq = getattr(packet, "frequency", 865.0625)
        self._last_tx_freq = (pfreq / 1e6) if pfreq > 1000.0 else pfreq
        self._last_tx_sf = getattr(packet, "spreading_factor", 7)

        # Track wait time
        enq_time = self._enqueue_times.get(packet.request_id, packet.timestamp)
        wait_time = max(0.0, self.clock.now() - enq_time)
        self._wait_sum += wait_time

        # Use pre-computed flags (set once in set_communication_mode)
        is_riacc_mode       = self._is_riacc
        is_class_a_riacc    = self._is_class_a_riacc
        is_pure_aloha_riacc = self._is_pure_aloha_riacc

        if is_riacc_mode:
            # 1. Emergency Fast-Path (First-time emergency event)
            if getattr(packet, "emergency_detected", False) and packet.em_scheduled is False:
                packet.em_scheduled = True
                if is_pure_aloha_riacc:
                    em_freq = 865.0625           # ALOHA single channel
                else:
                    em_channels = [865.0625, 865.4025, 866.4850, 866.2, 866.4]
                    em_freq = em_channels[det_hash(packet.request_id) % 5]

                em_slot = max(self.clock.now() + 0.002, self._channel_busy_until.get((em_freq, 7), self.clock.now()) + 0.045)
                
                # --- ENFORCE 5.0s HARDWARE BUFFER / TTL ---
                if (em_slot - self.clock.now()) > 5.0:
                    self.channel.statistics["dropped"] += 1
                    return
                # ------------------------------------------

                self._channel_busy_until[(em_freq, 7)] = em_slot + 0.045
                packet.frequency = em_freq
                packet.spreading_factor = 7

                self._energy_sum += (25.0 * 0.035 * 1000.0)

                self.event_queue.schedule(
                    event_time=em_slot,
                    event_type="TRANSMIT",
                    callback=self.transmit_packet,
                    packet=packet,
                    description="RIACC Fast-Path Emergency Alert"
                )
                return

            # 2. Master Gateway Arbitration for ALL Initial Packet Attempts (retry_count == 0)
            if getattr(packet, "retry_count", 0) == 0:
                nid = getattr(packet, "node_id", "UNKNOWN")
                if nid not in self.sensor_nodes:
                    self.sensor_nodes[nid] = IntelligentSensorNode(nid)
                sensor_node = self.sensor_nodes[nid]

                sensor_node.current_request_id = packet.request_id
                sensor_node.set_state(SensorState.EVENT_DETECTED)
                sensor_node.set_request_state(RequestState.REQUEST_SENT)

                _is_emergency_pkt = getattr(packet, "emergency_detected", False)

                if is_class_a_riacc and not _is_emergency_pkt:
                    timing_offset = 1.0   # Class A: RX1 window = 1.0s
                else:
                    timing_offset = 0.0   # ALOHA or Emergency: immediate slot

                grant = self.master_node.receive_request(
                    request=packet,
                    rssi=getattr(packet, "rssi", -80.0),
                    snr=getattr(packet, "snr", 5.0),
                    min_start_offset_s=timing_offset,
                )

                decision = getattr(grant, "decision", None)

                # DROP: master explicitly rejected this request
                if decision == MasterNodeDecision.DROP:
                    self._dropped_by_master = getattr(self, "_dropped_by_master", 0) + 1
                    sensor_node.receive_drop()
                    if self.master_node and hasattr(packet, "request_id") and packet.request_id:
                        self.master_node.confirm_transmission_complete(packet.request_id)
                    return

                # HOLD: network congested, back off then re-enter pipeline
                if decision == MasterNodeDecision.HOLD:
                    # Cap total HOLD retries to prevent infinite event-queue growth.
                    # Under extreme load, packets can loop in HOLD indefinitely,
                    # filling the event queue with millions of stale events → OOM.
                    MAX_HOLD_RETRIES = 10
                    packet.hold_count += 1
                    if packet.hold_count > MAX_HOLD_RETRIES:
                        # Drop silently — extreme congestion, packet TTL exceeded
                        self.channel.statistics["dropped"] = self.channel.statistics.get("dropped", 0) + 1
                        nid = getattr(packet, "node_id", "UNKNOWN")
                        nst = self._ensure_node_stats(nid)
                        nst["dropped"] += 1
                        if self.master_node and hasattr(packet, "request_id") and packet.request_id:
                            self.master_node.confirm_transmission_complete(packet.request_id)
                        return
                    base_hold = max(0.05, getattr(grant, "hold_duration_s", 0.5))
                    jitter = ((det_hash(packet.request_id) % 100) / 1000.0)
                    hold_s = base_hold + jitter
                    packet.retry_count = 0
                    sensor_node.receive_hold()
                    self.event_queue.schedule(
                        event_time=self.clock.now() + hold_s,
                        event_type="TRANSMIT",
                        callback=self.transmit_packet,
                        packet=packet,
                        description="RIACC HOLD Back-off Re-entry"
                    )
                    return

                # Node-ID ownership guard
                grant_node = getattr(grant, "node_id", packet.node_id)
                if grant_node != packet.node_id:
                    # Cap ownership-guard retries to prevent loop buildup
                    packet.guard_count += 1
                    if packet.guard_count > 5:
                        self.channel.statistics["dropped"] = self.channel.statistics.get("dropped", 0) + 1
                        if self.master_node and hasattr(packet, "request_id") and packet.request_id:
                            self.master_node.confirm_transmission_complete(packet.request_id)
                        return
                    packet.retry_count = 0
                    self.event_queue.schedule(
                        event_time=self.clock.now() + 0.1,
                        event_type="TRANSMIT",
                        callback=self.transmit_packet,
                        packet=packet,
                        description="RIACC Back-off: Grant Issued to Other Node"
                    )
                    return

                # ACK / GRANT handling
                channels_865 = [865.0625, 865.4025, 866.4850, 866.2, 866.4]
                if decision == MasterNodeDecision.ACK:
                    packet.retry_count = 1
                    if not getattr(packet, "frequency", None) or packet.frequency < 800.0:
                        packet.frequency = channels_865[det_hash(packet.request_id) % 5]
                        packet.spreading_factor = 7
                elif decision != MasterNodeDecision.GRANT and decision != MasterNodeDecision.WAIT:
                    logger.warning(f"Unhandled decision type for request {packet.request_id}: {decision}")
                    return
                else:
                    LORAWLAN_CHANNELS = [865.0625, 865.4025, 866.4850]
                    if is_pure_aloha_riacc:
                        pass  # Pure ALOHA fixed to 865.0625 MHz, static SF9
                    elif is_class_a_riacc:
                        assigned_freq = getattr(grant, "assigned_channel", None)
                        if assigned_freq and round(float(assigned_freq), 1) in LORAWLAN_CHANNELS:
                            packet.frequency = round(float(assigned_freq), 1)
                        else:
                            packet.frequency = LORAWLAN_CHANNELS[det_hash(packet.request_id) % 3]

                        assigned_sf = getattr(grant, "assigned_sf", None)
                        if assigned_sf and 7 <= int(assigned_sf) <= 12 and not _is_emergency_pkt:
                            packet.spreading_factor = int(assigned_sf)

                    packet.retry_count = 1

                master_start = getattr(grant, "start_time", 0.0)
                if master_start and master_start > self.clock.now() + 0.001:
                    sensor_node.receive_wait()
                    scheduled_time = master_start
                else:
                    sensor_node.receive_ack(packet.request_id)
                    scheduled_time = self.clock.now() + 0.002

                dl_energy_mj = 25.0 * 0.035 * 1000.0
                self._energy_sum += dl_energy_mj
                if 'sensor_node' in locals() and sensor_node:
                    sensor_node.consume_energy(dl_energy_mj)

                self.event_queue.schedule(
                    event_time=scheduled_time,
                    event_type="TRANSMIT",
                    callback=self.transmit_packet,
                    packet=packet,
                    description="RIACC Master Gateway Scheduled Transmission"
                )
                return


        # ── Baseline / fall-through: direct uncoordinated transmission ──────
        # Use pre-computed flag (no per-packet string lookup needed)
        sf = getattr(packet, "spreading_factor", 9)
        power_mw = 25.0 if sf <= 9 else 35.0
        tx_energy_mj = power_mw * toa * 1000.0
        self._energy_sum += tx_energy_mj
        if 'sensor_node' in locals() and sensor_node:
            sensor_node.consume_energy(tx_energy_mj)

        tx_time = self.clock.now()

        if is_riacc_mode:
            # RIACC Adaptive Transmit Power Control
            packet.rssi = max(getattr(packet, "rssi", -80.0), -85.0)
            tx_time += 0.001

        # Track empirical SF usage for heatmap
        pfreq = getattr(packet, "frequency", 865.0625)
        # Heatmap tracking
        ch = packet.frequency
        sf = packet.spreading_factor
        self._sf_channel_counts[(ch, sf)] = self._sf_channel_counts.get((ch, sf), 0) + 1

        self.channel.transmit(
            packet,
            current_time=tx_time,
            toa=toa,
        )

    # ------------------------------------------------------------------

    def deliver_packets(
        self,
    ):
        if not self.channel.completed_transmissions:
            return

        packets = self.channel.delivered_packets()

        for packet in packets:

            self._rssi_sum += getattr(packet, "rssi", -85.0)
            self._snr_sum += getattr(packet, "snr", 5.0)

            is_emergency = bool(getattr(packet, "emergency", False) or getattr(packet, "emergency_detected", False) or getattr(packet, "adaptive_threat_index", 0.0) >= 60.0)
            if is_emergency:
                self._emergency_delivered += 1

            # Per-node delivered tracking — single lookup (removed redundant duplicate call)
            nid = getattr(packet, "node_id", "UNKNOWN")
            nst = self._ensure_node_stats(nid)
            nst["delivered"] += 1

            # Update IntelligentSensorNode successful delivery status
            s_node = self.sensor_nodes.get(nid)
            if s_node:
                s_node.set_state(SensorState.SUCCESS)
                s_node.set_request_state(RequestState.SUCCESS)
                s_node.update_event_state(EventState.SUCCESS)

            # Track per-node delay (reuse nst from above — no second lookup)
            enq_time = self._enqueue_times.pop(getattr(packet, "request_id", ""), getattr(packet, "timestamp", 0.0))
            pkt_delay = max(0.0, self.clock.now() - enq_time)
            nst["total_delay"] += pkt_delay

        # Track per-node collisions, drops, and release master node reservations/occupancy
        for tx in self.channel.completed_transmissions:
            nid = getattr(tx.request, "node_id", "UNKNOWN")
            nst = self._ensure_node_stats(nid)
            
            # Always pop from _enqueue_times whether delivered or collided
            self._enqueue_times.pop(getattr(tx.request, "request_id", ""), None)
            
            if tx.collided:
                nst["collisions"] += 1
            if tx.collided and not tx.delivered:
                nst["dropped"] += 1
                
                # --- NEW FIX: Notify node of drop so it cleans up memory ---
                s_node = self.sensor_nodes.get(nid)
                if s_node:
                    s_node.set_state(SensorState.DROP)
                    s_node.set_request_state(RequestState.DROP)
                    s_node.update_event_state(EventState.DROPPED)
                # -----------------------------------------------------------

            # Release Master Node reservations/occupancy — only for RIACC modes.
            # Baseline (Pure ALOHA / Class A) packets were never scheduled through the
            # Master Gateway, so it must not be notified on completion either.
            if self._is_riacc and self.master_node and hasattr(tx.request, "request_id") and tx.request.request_id:
                self.master_node.confirm_transmission_complete(tx.request.request_id)

        self.channel.clear_completed()

    # ------------------------------------------------------------------

    def step(
        self,
    ):
        """
        Executes one simulation step.
        """

        if self.event_queue.empty():

            return False

        event = self.event_queue.next_event()

        if event is None:

            return False

        # Clamp backwards-time events (can occur at CSV chunk boundaries when
        # dataset timestamps are not perfectly monotonic) rather than crashing.
        delta = event.event_time - self.clock.now()
        if delta < 0:
            delta = 0.0  # process stale event at current clock time

        self.clock.advance(delta)

        event.callback(

            *event.args,

            **event.kwargs

        )

        self.channel.update(

            self.clock.now()

        )

        self.deliver_packets()

        if self._generated % 500 == 0:
            self._queue_samples.append(len(self.event_queue))
            # Cap queue_samples to last 2000 readings to bound RAM
            if len(self._queue_samples) > 2000:
                del self._queue_samples[:500]

        # Increment step-level counter (independent of _generated, which is set at chunk-load boundaries)
        self._snap_counter += 1
        # Snapshot every 20 step() calls → ~100,000 uniform snapshots across 2M events
        should_snapshot = (self._snap_counter % 20 == 0)

        if should_snapshot:
            # Empirical correlated time series tracking (sampled every 20 events)
            ch_st = self.channel.statistics
            deliv = int(ch_st.get("delivered", 0))
            drop  = int(ch_st.get("dropped", 0))
            coll  = int(ch_st.get("collisions", 0))
            gen   = max(1, self._generated)
            pdr   = (deliv / gen) * 100.0
            avg_ati = self._ati_sum / gen
            queued  = len(self.event_queue)
            retries = coll / max(1, deliv)

            emg_gen = self._emergency_generated
            emg_del = self._emergency_delivered
            emg_drp = max(0, emg_gen - emg_del)
            emg_pdr = (emg_del / max(1, emg_gen)) * 100.0
            norm_gen = self._normal_total
            norm_del = max(0, deliv - emg_del)
            norm_pdr = (norm_del / max(1, norm_gen)) * 100.0

            freq_mhz = getattr(self, "_last_tx_freq", 865.0625)
            if freq_mhz > 1000.0:
                freq_mhz = freq_mhz / 1e6
            sf_val = getattr(self, "_last_tx_sf", 9)

            m_stats = getattr(self.master_node, "stats_manager", None)
            m_dict = getattr(m_stats, "stats", {}) if m_stats else {}
            col_prev = int(m_dict.get("total_collisions_prevented", 0)) if self._is_riacc else 0
            hold_cnt = int(m_dict.get("hold_decisions_issued", 0)) if self._is_riacc else 0
            fp_acks  = int(m_dict.get("emergency_fastpath_acks", 0)) if self._is_riacc else 0

            cur_time = max(1e-3, float(self.clock.now()))
            goodput = (deliv * 32.0 * 8.0) / cur_time
            energy_kj = self._energy_sum / 1e6
            energy_per_del = self._energy_sum / max(1, deliv)
            avg_wait_ms = (self._wait_sum / gen) * 1000.0

            self._time_series.append({
                "time": round(float(self.clock.now()), 4),
                "mode": self.communication_mode.value if hasattr(self.communication_mode, "value") else str(self.communication_mode),
                "nodes": max(1, len(self._per_node_stats)),
                "generated": gen,
                "normal_generated": norm_gen,
                "emergency_generated": emg_gen,
                "delivered": deliv,
                "normal_delivered": norm_del,
                "emergency_delivered": emg_del,
                "dropped": drop,
                "emergency_dropped": emg_drp,
                "collisions": coll,
                "collisions_prevented": col_prev,
                "pdr": round(pdr, 2),
                "normal_pdr": round(norm_pdr, 2),
                "emergency_pdr": round(emg_pdr, 2),
                "retries": round(float(min(8.0, retries)), 3),
                "channel_freq_mhz": round(float(freq_mhz), 4),
                "spreading_factor": int(sf_val),
                "avg_ati": round(float(avg_ati), 2),
                "hold_decisions": hold_cnt,
                "fastpath_acks": fp_acks,
                "energy_kj": round(energy_kj, 4),
                "energy_per_delivered_mj": round(energy_per_del, 2),
                "avg_delay_ms": round(avg_wait_ms, 2),
                "goodput_bps": round(goodput, 2),
                "queued": queued,
                # ── Instantaneous differential fields — computed from prev-snapshot scalars ──
                # 4 core variation fields
                "inst_pdr": round(
                    ((deliv - self._prev_snap_deliv) / max(1, gen - self._prev_snap_gen)) * 100.0
                    if gen > self._prev_snap_gen else pdr, 2),
                "inst_goodput_bps": round(
                    ((deliv - self._prev_snap_deliv) * 32.0 * 8.0)
                    / max(1e-6, float(self.clock.now()) - (self._time_series[-1]["time"] if self._time_series else 0.0) + 1e-6)
                    if self._time_series else goodput, 2),
                "inst_collisions": max(0, coll - self._prev_snap_coll),
                "inst_energy_mj": round(max(0.0, (self._energy_sum / 1e3) - self._prev_snap_energy_mj), 4),
                # 3 RIACC novelty instantaneous fields (show framework-specific activity per window)
                "inst_emergency_pdr": round(
                    ((emg_del - self._prev_snap_emg_del) / max(1, gen - self._prev_snap_gen)) * 100.0
                    if gen > self._prev_snap_gen else emg_pdr, 2),
                "inst_fastpath_acks": max(0, fp_acks - self._prev_snap_fp_acks),
                "inst_hold_decisions": max(0, hold_cnt - self._prev_snap_hold),
                # Collision avoidance ratio: what fraction of potential collisions were prevented (RIACC only, 0 for baselines)
                "collision_avoidance_ratio": round(
                    col_prev / max(1, col_prev + coll) * 100.0, 2),
                # Instantaneous ATI of the current 20-event window — shows real threat spikes (10→100) not the flat running average
                "inst_ati": round(
                    (self._ati_sum - self._prev_snap_ati_sum) / max(1, gen - self._prev_snap_gen)
                    if gen > self._prev_snap_gen else avg_ati, 2),
            })
            # Update all 8 prev-snapshot scalars (8 scalar assignments — zero RAM overhead)
            self._prev_snap_deliv     = deliv
            self._prev_snap_gen       = gen
            self._prev_snap_coll      = coll
            self._prev_snap_energy_mj = self._energy_sum / 1e3
            self._prev_snap_emg_del   = emg_del
            self._prev_snap_fp_acks   = fp_acks
            self._prev_snap_hold      = hold_cnt
            self._prev_snap_ati_sum   = self._ati_sum
            # Cap time_series to 100,000 snapshots to preserve full timeline across multi-million event runs without truncation
            if len(self._time_series) > 100000:
                del self._time_series[:10000]

        return True

    # ------------------------------------------------------------------

    def emit_live_stats(self):
        """Emits real-time live channel & node statistics for terminal dashboard rendering."""
        ch_stats = self.channel.statistics          # direct dict access — no copy overhead
        deliv = int(ch_stats.get("delivered", 0))
        drop  = int(ch_stats.get("dropped", 0))
        coll  = int(ch_stats.get("collisions", 0))
        gen   = max(1, self._generated)
        pdr   = (deliv / gen) * 100.0

        mode_name = self.communication_mode.name
        # O(1) running counters — no O(N) sum() over per_node_stats
        norm    = self._normal_total
        emg     = self._emergency_total
        retries = coll / max(1, deliv)

        nodes_cnt = len(self._per_node_stats) if self._per_node_stats else 18

        freq_mhz = getattr(self, "_last_tx_freq", 865.0625)
        if freq_mhz > 1000.0:
            freq_mhz = freq_mhz / 1e6
        sf_val = getattr(self, "_last_tx_sf", 7)

        if "RIACC" in mode_name:
            rf_str = f"Ch: {freq_mhz:.1f} MHz ~ SF: {sf_val}"
        elif "CLASS_A" in mode_name:
            rf_str = f"Ch: {freq_mhz:.1f} MHz ~ SF: {sf_val} (Gateway-assigned)"
        else:
            rf_str = f"Ch: {freq_mhz:.1f} MHz ~ SF: {sf_val} (Static)"

        logger.info(
            f"[STATS] Nodes: {nodes_cnt} | Packets: {self._generated:,} | "
            f"Success: {deliv:,} | Collisions: {coll:,} | Dropped: {drop:,} | PDR: {pdr:.1f}% | "
            f"Normal: {norm:,} | Emergency: {emg:,} | Retries: {retries:.2f} | RF: {rf_str}"
        )

    def run(
        self,
    ):
        """
        Executes the complete simulation and flushes remaining active channel transmissions.
        """

        self.running = True

        self.clock.start()

        step_count = 0
        while self.running:

            executed = self.step()

            if not executed:

                break

            step_count += 1
            if step_count % 500 == 0:
                self.emit_live_stats()

        # Flush any remaining in-flight transmissions in the channel
        self.channel.flush(self.clock.now() + 10.0)
        self.deliver_packets()
        self.emit_live_stats()

        self.running = False

    # ------------------------------------------------------------------

    def stop(
        self,
    ):

        self.running = False

    # ------------------------------------------------------------------

    def simulation_time(
        self,
    ):

        return self.clock.now()

    # ------------------------------------------------------------------

    def pending_events(
        self,
    ):

        return len(self.event_queue)

    # ------------------------------------------------------------------

    def statistics(
        self,
    ):

        stats = {

            "simulation_time":
                self.clock.now(),

            "communication_mode":
                self.communication_mode.value if hasattr(self.communication_mode, "value") else str(self.communication_mode),

            "pending_events":
                len(self.event_queue),

            "generated":
                self._generated,

            "rssi_sum":
                self._rssi_sum,

            "snr_sum":
                self._snr_sum,

            "ati_sum":
                self._ati_sum,

            "energy_sum":
                self._energy_sum,

            "wait_sum":
                self._wait_sum,

            "emergency_generated":
                self._emergency_generated,

            "emergency_delivered":
                self._emergency_delivered,

            "queue_samples":
                self._queue_samples,

            "sf_channel_counts":
                dict(self._sf_channel_counts),

            "time_series":
                list(self._time_series),

            "channel":
                self.channel.get_statistics(),

        }

        if hasattr(self.master_node, "stats_manager"):

            stats["master_gateway"] = self.master_node.stats_manager.stats

        # Per-node statistics for table output
        stats["per_node_stats"] = dict(self._per_node_stats)

        return stats

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"SimulationEngine("
            f"time={self.clock.now():.2f}, "
            f"events={len(self.event_queue)})"

        )