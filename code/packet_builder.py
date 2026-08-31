# =============================================================================
# packet_builder.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Packet Builder
#
# Converts surveillance events into LoRa TransmissionRequest packets.
# =============================================================================

from dataclasses import dataclass
from typing import Optional, Any
import uuid
import random
import math
import zlib

from models import TransmissionRequest, SimulationEvent, LoRaPacket
from config import Priority, ThreatLevel


# =============================================================================
# PACKET CONFIGURATION
# =============================================================================

@dataclass
class PacketConfiguration:
    """
    Default LoRa packet configuration.
    """

    payload_size: int = 32          # Bytes
    frequency: float = 865.0625        # MHz
    spreading_factor: int = 9
    coding_rate: str = "4/5"
    bandwidth: int = 125            # kHz


# =============================================================================
# PACKET BUILDER
# =============================================================================

class PacketBuilder:
    """
    Builds TransmissionRequest packets from SimulationEvent objects.
    """

    def __init__(
        self,
        config: Optional[PacketConfiguration] = None
    ):

        self.config = config or PacketConfiguration()

    # ------------------------------------------------------------------

    def build(
        self,
        event: SimulationEvent,
        mode: Optional[Any] = None
    ) -> TransmissionRequest:
        """
        Convert one SimulationEvent into a TransmissionRequest with mode-aware radio configuration.
        """

        node_id_val = str(getattr(event, "node_id", "NODE_01"))
        timestamp_val = float(getattr(event, "timestamp", getattr(event, "event_time", 0.0)))
        ati_val = float(getattr(event, "ati", getattr(event, "adaptive_threat_index", 0.0)))
        battery_val = float(getattr(event, "battery", getattr(event, "battery_percentage", 100.0)))
        is_emg_flag = bool(getattr(event, "emergency", False))
        is_crit = (str(getattr(event, "attack_type", "normal")).lower() in ("critical", "ddos", "ransomware", "injection"))
        is_hi_ati = (ati_val >= 60.0)

        emergency_val = is_emg_flag or is_crit or is_hi_ati
        req_id = getattr(event, "request_id", self._generate_request_id())
        mode_str = str(getattr(mode, "value", mode or "Pure ALOHA"))

        h_val = zlib.crc32(f"{req_id}_{node_id_val}".encode('utf-8'))

        # Use RSSI/SNR from event if present, otherwise calculate path loss
        if hasattr(event, "rssi") and getattr(event, "rssi", None) is not None:
            rssi_val = float(event.rssi)
        else:
            dist_m = 300.0 + (h_val % 2200)
            path_loss = 100.0 + 28.0 * math.log10(dist_m / 100.0)
            rssi_val = round(-14.0 - path_loss, 2)

        if hasattr(event, "snr") and getattr(event, "snr", None) is not None:
            snr_val = float(event.snr)
        else:
            snr_val = round(rssi_val + 120.0, 2)

        # ────────────────────────────────────────────────────────────────────
        # Baseline-constrained Radio Parameter Assignment
        # RIACC piggybacks on each baseline's physical radio constraints:
        #
        #  Pure ALOHA (with or without RIACC):
        #    - Channel : 865.0625 MHz only  (ALOHA = 1 channel, no frequency hopping)
        #    - SF      : 9               (ALOHA = static, no link adaptation)
        #    - RIACC role: control WHEN the node transmits (time-slot arbitration)
        #
        #  Class A (with or without RIACC):
        #    - Channels: 865.0625, 865.4025, 866.4850 MHz  (3 standard LoRaWAN IN865 sub-bands)
        #    - SF      : Gateway-assigned or distance-based (SF7–SF12)
        #    - RIACC role: assign timeslot & SF within these 3 channels via RX1/RX2 window
        #
        #  Emergency packets in any RIACC mode:
        #    - RIACC Emergency Fast-Path overrides with SF7 on 866.4 MHz
        #      (dedicated emergency channel — explicitly claimed in the RIACC paper)
        # ────────────────────────────────────────────────────────────────────

        # Standard LoRaWAN IN865 3 sub-band channels (shared by Class A and Class B)
        class_a_channels = [865.0625, 865.4025, 866.4850]

        if emergency_val and ("RIACC" in mode_str):
            # RIACC Emergency Fast-Path: dedicated emergency channel + SF7
            # Applies to all RIACC modes (ALOHA, Class A, Class B)
            # This is RIACC's explicit emergency override — documented in the paper
            freq_val = 866.4
            sf_val = 7
        elif "Class A" in mode_str:
            # Class A (baseline and RIACC): SF from link quality (path-loss based).
            # For Class A + RIACC: master gateway will assign final SF via grant.assigned_sf.
            # For Class A baseline: this distance-based SF is used directly (no ADR, no gateway).
            if rssi_val >= -100:
                sf_val = 7
            elif rssi_val >= -105:
                sf_val = 8
            elif rssi_val >= -110:
                sf_val = 9
            elif rssi_val >= -115:
                sf_val = 10
            elif rssi_val >= -120:
                sf_val = 11
            else:
                sf_val = 12
            channel_idx = h_val % 3           # Only 3 Class A sub-bands (not 5)
            freq_val = class_a_channels[channel_idx]
        else:
            # Pure ALOHA (baseline and RIACC): single channel 865.0625 MHz, static SF9
            # RIACC on Pure ALOHA only controls timing — radio parameters unchanged
            sf_val = 9
            freq_val = 865.0625                  # ALOHA = 1 channel only

        packet_obj = LoRaPacket(
            source_node=node_id_val,
            payload_size=self.config.payload_size,
            spreading_factor=sf_val,
            frequency=freq_val,
            timestamp=timestamp_val,
            adaptive_threat_index=ati_val,
        )

        request = TransmissionRequest(
            request_id=req_id,
            node_id=node_id_val,
            timestamp=timestamp_val,
            packet=packet_obj,
            adaptive_threat_index=ati_val,
            battery_percentage=battery_val,
            emergency_detected=emergency_val,
            priority=Priority.EMERGENCY if emergency_val else Priority.LOW,
            threat_level=ThreatLevel.CRITICAL if emergency_val else ThreatLevel.NORMAL,
            frequency=freq_val,
            spreading_factor=sf_val,
            rssi=rssi_val,
            snr=snr_val,
        )

        return request

    # ------------------------------------------------------------------

    def build_batch(
        self,
        events,
        mode: Optional[Any] = None
    ):
        """
        Build packets from multiple surveillance events with mode configuration.
        """

        packets = []

        for event in events:

            packets.append(
                self.build(event, mode=mode)
            )

        return packets

    # ------------------------------------------------------------------

    def estimate_time_on_air(
        self,
        packet: TransmissionRequest
    ) -> float:
        """
        Approximate LoRa Time-on-Air (seconds).
        """

        sf = getattr(packet, "spreading_factor", None)

        if sf is None and hasattr(packet, "packet") and packet.packet is not None:

            sf = getattr(packet.packet, "spreading_factor", self.config.spreading_factor)

        if sf is None:

            sf = self.config.spreading_factor

        bw = getattr(packet, "bandwidth", None)

        if bw is None and hasattr(packet, "packet") and packet.packet is not None:

            bw = getattr(packet.packet, "bandwidth", self.config.bandwidth)

        if bw is None:

            bw = self.config.bandwidth

        bw_hz = bw * 1000.0 if bw < 10000 else float(bw)

        # Official Semtech SX1276 / SX1262 LoRa Time-on-Air Formula
        t_sym = (2 ** sf) / bw_hz
        t_preamble = (8 + 4.25) * t_sym
        pl = getattr(packet, "payload_size", 32)
        if pl == 32 and hasattr(packet, "packet") and packet.packet is not None:
            pl = getattr(packet.packet, "payload_size", 32)

        # STRICT MTU CLAMP (20 - 50 bytes)
        pl = max(20, min(50, pl))


        low_dr_opt = 1 if sf >= 11 else 0
        term1 = 8 * pl - 4 * sf + 28 + 16 - 0  # 16 CRC, Explicit Header
        denom = 4 * (sf - 2 * low_dr_opt)
        n_payload_sym = 8 + max(math.ceil(term1 / denom) * (1 + 4), 0)
        toa = t_preamble + (n_payload_sym * t_sym)

        return round(toa, 4)

    # ------------------------------------------------------------------

    def assign_channel(
        self,
        packet: TransmissionRequest,
        channels=None
    ):
        """
        Randomly assign one of the available channels.
        """

        if channels is None:

            channels = [

                865.0625,

                865.4025,

                866.4850,

                866.2,

                866.4

            ]

        packet.frequency = random.choice(channels)

        return packet

    # ------------------------------------------------------------------

    def randomize_link_quality(
        self,
        packet: TransmissionRequest
    ):
        """
        Adds small channel variations.
        """

        packet.rssi += random.uniform(-2, 2)

        packet.snr += random.uniform(-1, 1)

        return packet

    # ------------------------------------------------------------------

    def _generate_request_id(self):

        return "REQ_" + uuid.uuid4().hex[:8].upper()

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"PacketBuilder("
            f"SF={self.config.spreading_factor}, "
            f"BW={self.config.bandwidth}kHz, "
            f"Payload={self.config.payload_size}B)"

        )

    