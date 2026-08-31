# =============================================================================
# lora_channel.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# LoRa Wireless Channel Simulator
# =============================================================================

from dataclasses import dataclass
from typing import Dict, List, Tuple

from models import TransmissionRequest


# =============================================================================
# CHANNEL TRANSMISSION
# =============================================================================

@dataclass
class ChannelTransmission:
    """
    Represents one active LoRa transmission.
    """

    request: TransmissionRequest

    start_time: float

    end_time: float

    frequency: float

    spreading_factor: int

    collided: bool = False

    delivered: bool = False


# Receiver sensitivity thresholds by SF (Semtech SX1302 / SX1262 spec)
SF_SENSITIVITY: Dict[int, float] = {7: -123.0, 8: -126.0, 9: -129.0, 10: -132.0, 11: -134.5, 12: -137.0}


# =============================================================================
# LORA CHANNEL
# =============================================================================

class LoRaChannel:
    """
    Simulates the wireless LoRa channel with O(1) channel-indexed collision detection and removals.

    Responsibilities
    ----------------
    • Accept packet transmissions
    • Detect collisions (exact physical time overlap on same freq & SF)
    • Release completed transmissions
    • Deliver successfully received packets
    """

    def __init__(self):

        self.active_transmissions: List[ChannelTransmission] = []

        # Channel-indexed bucket: (freq, sf) -> Dict[int, ChannelTransmission]
        self._active_by_channel: Dict[Tuple[float, int], Dict[int, ChannelTransmission]] = {}

        self.completed_transmissions: List[ChannelTransmission] = []

        self.statistics = {

            "transmitted": 0,

            "delivered": 0,

            "collisions": 0,

            "dropped": 0,

            "channel_busy_time": 0.0

        }

    # ------------------------------------------------------------------

    def transmit(
        self,
        packet: TransmissionRequest,
        current_time: float,
        toa: float
    ) -> ChannelTransmission:
        """
        Start a packet transmission.
        """

        freq_val = getattr(packet, "frequency", None)
        if (freq_val is None or freq_val < 100.0) and hasattr(packet, "packet") and packet.packet is not None:
            freq_val = getattr(packet.packet, "frequency", 865.0625)
        if freq_val is None:
            freq_val = 865.0625

        sf_val = getattr(packet, "spreading_factor", None)
        if sf_val is None and hasattr(packet, "packet") and packet.packet is not None:
            sf_val = getattr(packet.packet, "spreading_factor", 9)
        if sf_val is None:
            sf_val = 9

        # Synchronize inner packet object if present
        if hasattr(packet, "packet") and packet.packet is not None:
            packet.packet.frequency = freq_val
            packet.packet.spreading_factor = sf_val

        transmission = ChannelTransmission(

            request=packet,

            start_time=current_time,

            end_time=current_time + toa,

            frequency=freq_val,

            spreading_factor=sf_val,

        )

        self.statistics["transmitted"] += 1
        self.statistics["channel_busy_time"] += toa

        self._detect_collision(transmission)

        self.active_transmissions.append(transmission)

        channel_key = (freq_val, sf_val)
        if channel_key not in self._active_by_channel:
            self._active_by_channel[channel_key] = {}
        self._active_by_channel[channel_key][id(transmission)] = transmission

        return transmission

    # ------------------------------------------------------------------

    def _detect_collision(
        self,
        new_tx: ChannelTransmission
    ):
        """
        Real-world Semtech LoRa PHY Co-Channel Capture Effect:
        When two packets overlap on the same frequency and spreading factor:
        - If RSSI_stronger - RSSI_weaker >= 6.0 dB (Semtech capture threshold),
          the stronger packet SURVIVES and is successfully demodulated.
        - If RSSI_diff < 6.0 dB, both packets collide and are destroyed.
        """
        channel_key = (new_tx.frequency, new_tx.spreading_factor)
        bucket = self._active_by_channel.get(channel_key)
        if not bucket:
            return

        new_rssi = getattr(new_tx.request, "rssi", -80.0)
        min_sens = SF_SENSITIVITY.get(new_tx.spreading_factor, -125.0)

        # If signal is below receiver sensitivity, drop packet due to noise floor
        if new_rssi < min_sens:
            new_tx.collided = True
            self.statistics["collisions"] += 1
            return

        for tx in bucket.values():
            if tx.end_time <= new_tx.start_time:
                continue

            overlap = (new_tx.start_time < tx.end_time and new_tx.end_time > tx.start_time)

            if overlap:
                cand_rssi = getattr(tx.request, "rssi", -80.0)

                # Semtech 6 dB Co-Channel Capture Effect
                if new_rssi - cand_rssi >= 6.0:
                    # New packet is >= 6dB stronger: new_tx SURVIVES, existing tx collides
                    if not tx.collided:
                        tx.collided = True
                        self.statistics["collisions"] += 1
                elif cand_rssi - new_rssi >= 6.0:
                    # Existing packet is >= 6dB stronger: tx SURVIVES, new_tx collides
                    if not new_tx.collided:
                        new_tx.collided = True
                        self.statistics["collisions"] += 1
                else:
                    # SIR < 6dB: Both packets collide and destroy each other
                    if not tx.collided:
                        tx.collided = True
                        self.statistics["collisions"] += 1
                    if not new_tx.collided:
                        new_tx.collided = True
                        self.statistics["collisions"] += 1

    # ------------------------------------------------------------------

    def update(
        self,
        current_time: float
    ):
        """
        Move completed transmissions out of the channel in O(1) per completion.
        """

        remaining = []

        for tx in self.active_transmissions:

            if tx.end_time <= current_time:

                if tx.collided:

                    self.statistics["dropped"] += 1

                else:

                    tx.delivered = True

                    self.statistics["delivered"] += 1

                self.completed_transmissions.append(tx)

                # O(1) removal from channel bucket dict
                channel_key = (tx.frequency, tx.spreading_factor)
                bucket = self._active_by_channel.get(channel_key)
                if bucket:
                    bucket.pop(id(tx), None)
                    if not bucket:
                        del self._active_by_channel[channel_key]

            else:

                remaining.append(tx)

        self.active_transmissions = remaining

    # ------------------------------------------------------------------

    def flush(self, final_time: float = float('inf')):
        """
        Forces completion of all remaining active transmissions in channel.
        """
        if not self.active_transmissions:
            return
        max_end = max(tx.end_time for tx in self.active_transmissions)
        self.update(max_end + 0.001)

    # ------------------------------------------------------------------

    def delivered_packets(self):

        packets = []

        for tx in self.completed_transmissions:

            if tx.delivered:

                packets.append(tx.request)

        return packets

    # ------------------------------------------------------------------

    def clear_completed(self):

        self.completed_transmissions.clear()

    # ------------------------------------------------------------------

    def utilization(self):

        return len(self.active_transmissions)

    # ------------------------------------------------------------------

    def get_statistics(self):

        return dict(self.statistics)

    # ------------------------------------------------------------------

    def reset(self):

        self.active_transmissions.clear()

        self.completed_transmissions.clear()

        self.statistics = {

            "transmitted": 0,

            "delivered": 0,

            "collisions": 0,

            "dropped": 0,

            "channel_busy_time": 0.0

        }

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"LoRaChannel("
            f"Active={len(self.active_transmissions)}, "
            f"Delivered={self.statistics['delivered']}, "
            f"Collisions={self.statistics['collisions']})"

        )


    