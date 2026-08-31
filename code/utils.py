"""
===============================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
-------------------------------------------------------------------------------
Utility Functions

This module contains generic helper functions used throughout the
RIACC Research Simulator.

Guidelines
----------
• No simulator logic.
• No protocol logic.
• No dataset-specific functions.
• No scheduling algorithms.

Research Project:
    A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)

Author:
    Safeer Shah

Version:
    1.0
===============================================================================
"""

from __future__ import annotations

import random
import time
import math
from datetime import datetime
from uuid import uuid4
from typing import Any, Sequence

from config import RANDOM_SEED

# =============================================================================
# RANDOM INITIALIZATION
# =============================================================================

random.seed(RANDOM_SEED)


# =============================================================================
# TIME UTILITIES
# =============================================================================

def current_time() -> float:
    """
    Return the current system time in seconds.
    """
    return time.time()


def current_datetime() -> datetime:
    """
    Return the current local datetime.
    """
    return datetime.now()


def elapsed_time(start_time: float) -> float:
    """
    Return elapsed time (seconds) since start_time.
    """
    return time.time() - start_time


def format_time(seconds: float) -> str:
    """
    Convert seconds into HH:MM:SS format.
    """
    seconds = max(0, int(seconds))

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    return f"{hours:02}:{minutes:02}:{secs:02}"


# =============================================================================
# RANDOM UTILITIES
# =============================================================================

def random_float(minimum: float, maximum: float) -> float:
    """
    Generate a random floating-point value.
    """
    return random.uniform(minimum, maximum)


def random_int(minimum: int, maximum: int) -> int:
    """
    Generate a random integer.
    """
    return random.randint(minimum, maximum)


def random_choice(items: Sequence[Any]) -> Any:
    """
    Return a random element from a sequence.
    """
    if not items:
        raise ValueError("Cannot choose from an empty sequence.")

    return random.choice(items)


def random_probability() -> float:
    """
    Generate a random probability in the range [0.0, 1.0].
    """
    return random.random()


# =============================================================================
# ID UTILITIES
# =============================================================================

def generate_uuid() -> str:
    """
    Generate a universally unique identifier.
    """
    return str(uuid4())

# =============================================================================
# MATHEMATICAL UTILITIES
# =============================================================================

def clamp(value: float, minimum: float, maximum: float) -> float:
    """
    Restrict a value to the specified range.
    """
    return max(minimum, min(value, maximum))


def normalize(
    value: float,
    minimum: float,
    maximum: float
) -> float:
    """
    Normalize a value to the range [0.0, 1.0].
    """

    if maximum == minimum:
        return 0.0

    normalized = (value - minimum) / (maximum - minimum)

    return clamp(normalized, 0.0, 1.0)


def percentage(part: float, total: float) -> float:
    """
    Calculate percentage safely.
    """

    if total == 0:
        return 0.0

    return (part / total) * 100.0


def average(values: Sequence[float]) -> float:
    """
    Calculate arithmetic mean.
    """

    if not values:
        return 0.0

    return sum(values) / len(values)


def safe_division(
    numerator: float,
    denominator: float,
    default: float = 0.0,
) -> float:
    """
    Safely divide two numbers.
    """

    if denominator == 0:
        return default

    return numerator / denominator


# =============================================================================
# VALIDATION UTILITIES
# =============================================================================

def validate_probability(value: float) -> bool:
    """
    Validate probability value.
    """

    return 0.0 <= value <= 1.0


def validate_percentage(value: float) -> bool:
    """
    Validate percentage value.
    """

    return 0.0 <= value <= 100.0


def validate_range(
    value: float,
    minimum: float,
    maximum: float,
) -> bool:
    """
    Validate that a value lies within a given range.
    """

    return minimum <= value <= maximum


def is_positive(value: float) -> bool:
    """
    Check whether a value is positive.
    """

    return value > 0


def is_non_negative(value: float) -> bool:
    """
    Check whether a value is non-negative.
    """

    return value >= 0

# =============================================================================
# LoRa UTILITIES
# =============================================================================

def dbm_to_mw(dbm: float) -> float:
    """
    Convert dBm to milliwatts.
    """

    return 10 ** (dbm / 10.0)


def mw_to_dbm(mw: float) -> float:
    """
    Convert milliwatts to dBm.
    """

    if mw <= 0:
        raise ValueError("Power must be greater than zero.")

    return 10 * math.log10(mw)


def calculate_path_loss(
    tx_power_dbm: float,
    rssi_dbm: float,
) -> float:
    """
    Calculate path loss in dB.
    """

    return tx_power_dbm - rssi_dbm


def estimate_link_quality(
    rssi: float,
    snr: float,
) -> float:
    """
    Estimate normalized link quality (0-100%).
    """

    rssi_score = normalize(rssi, -120.0, -40.0)
    snr_score = normalize(snr, -20.0, 10.0)

    return ((rssi_score * 0.6) + (snr_score * 0.4)) * 100.0


def airtime_placeholder(
    payload_size: int,
    spreading_factor: int,
    bandwidth: int,
) -> float:
    """
    Placeholder for Time-on-Air calculation.

    This will be replaced with the complete LoRaWAN
    Time-on-Air equation in the PHY layer module.
    """

    symbol_time = (2 ** spreading_factor) / bandwidth

    return payload_size * symbol_time


# =============================================================================
# FORMATTING UTILITIES
# =============================================================================

def separator(
    character: str = "-",
    length: int = 80,
) -> str:
    """
    Generate a separator line.
    """

    return character * length


def print_banner(title: str) -> None:
    """
    Print a formatted section banner.
    """

    print()
    print(separator("="))
    print(title.center(80))
    print(separator("="))


def format_percentage(value: float) -> str:
    """
    Format a percentage value.
    """

    return f"{value:.2f}%"


def format_db(value: float) -> str:
    """
    Format a dB value.
    """

    return f"{value:.2f} dB"


def format_dbm(value: float) -> str:
    """
    Format a dBm value.
    """

    return f"{value:.2f} dBm"

