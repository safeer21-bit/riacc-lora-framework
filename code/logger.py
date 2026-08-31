"""
===============================================================================
A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
-------------------------------------------------------------------------------
Centralized Logging Module

Every module in the simulator must use this logger.

Individual modules must never create their own logger instances.

Research Project:
    A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)

Author:
    Safeer Shah

Version:
    1.0
===============================================================================
"""

from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

from config import (
    LOG_DIR,
    ENABLE_LOGGING,
    ENABLE_DEBUG_MODE,
    SIMULATOR_NAME,
)


# =============================================================================
# LOG DIRECTORY
# =============================================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

import os
LOG_FILE = LOG_DIR / f"riacc_simulation_{os.getpid()}.log"


# =============================================================================
# LOGGER CONFIGURATION
# =============================================================================

LOGGER_NAME = "RIACC"

LOGGER_FORMAT = (
    "%(asctime)s | "
    "%(levelname)-8s | "
    "%(message)s"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# =============================================================================
# LOGGER INSTANCE
# =============================================================================

logger = logging.getLogger(LOGGER_NAME)

logger.setLevel(
    logging.DEBUG if ENABLE_DEBUG_MODE else logging.INFO
)

logger.propagate = False


# =============================================================================
# FORMATTER
# =============================================================================

formatter = logging.Formatter(
    fmt=LOGGER_FORMAT,
    datefmt=DATE_FORMAT,
)


# =============================================================================
# FILE HANDLER
# =============================================================================

from logging import FileHandler

file_handler = FileHandler(
    LOG_FILE,
    mode="w",
    encoding="utf-8",
)

file_handler.setFormatter(formatter)

# OPTIMIZED: File handler writes only WARNING+ to prevent millions of INFO lines
# saturating disk I/O during 1M-event simulations.
# Console handler remains at INFO so VS Code output stays readable.
file_handler.setLevel(
    logging.DEBUG if ENABLE_DEBUG_MODE else logging.WARNING
)


# =============================================================================
# CONSOLE HANDLER (SINGLE-LINE OVERWRITE IN TERMINAL)
# =============================================================================

# =============================================================================
# CONSOLE HANDLER (DUAL-LINE IN-PLACE TERMINAL OVERWRITE)
# =============================================================================
# WORKER IPC LOG QUEUE FOR MULTIPROCESSING DASHBOARD
# =============================================================================

_ipc_queue = None
_worker_idx = 0
_worker_name = ""
_ipc_counter = 0

def set_worker_ipc(queue, worker_idx: int, worker_name: str):
    """Configures worker process to route console logs to main process IPC queue."""
    global _ipc_queue, _worker_idx, _worker_name, _ipc_counter
    _ipc_queue = queue
    _worker_idx = worker_idx
    _worker_name = worker_name
    _ipc_counter = 0


# =============================================================================
# CONSOLE HANDLER (DUAL-LINE IN-PLACE TERMINAL OVERWRITE)
# =============================================================================

class DualLineConsoleHandler(logging.StreamHandler):
    """
    Custom console handler that maintains dedicated in-place updating lines in VS Code terminal:
      - In worker processes: Routes log messages to IPC queue for clean 4-line scenario dashboard.
      - In main/sequential process: Maintains 2 dedicated in-place updating lines (Master Line 1, Sensor Line 2).
    """
    def __init__(self, stream=None):
        import sys
        target_stream = stream or sys.stdout
        if hasattr(target_stream, 'reconfigure'):
            try:
                target_stream.reconfigure(encoding='utf-8', errors='replace')
            except Exception:
                pass
        super().__init__(target_stream)
        self.has_active_lines = False
        self.current_line_is_sensor = False
        self.dashboard_active = False

    def emit(self, record):
        try:
            msg = self.format(record)
            raw_msg = record.getMessage()

            # If inside worker or sequential process with IPC queue, send to IPC queue for main process dashboard
            if _ipc_queue is not None:
                # Rate-limit normal sensor messages to avoid IPC queue lock contention while keeping terminal dynamic
                if "[SENSOR]" in raw_msg and "EMERGENCY" not in raw_msg:
                    global _ipc_counter
                    _ipc_counter += 1
                    if _ipc_counter % 5 != 0:
                        return
                try:
                    _ipc_queue.put((_worker_idx, _worker_name, msg, raw_msg))
                except Exception:
                    pass
                return

            # Suppress direct stdout writes when main dashboard loop is active
            if getattr(self, 'dashboard_active', False):
                return

            enc = getattr(self.stream, 'encoding', None) or 'utf-8'
            try:
                msg = msg.encode(enc, errors='replace').decode(enc, errors='replace')
            except Exception:
                msg = msg.replace('──>', '-->')

            # Static section headers, banners, or final summary reports move to a new line
            is_static_header = any(kw in raw_msg for kw in [
                "===", "---", "SIMULATION PERFORMANCE REPORT", "COMPLETED",
                "Banner", "Started", "TOTAL SIMULATION", "PERFORMANCE",
                "PARALLEL", "WORKER", "EXPERIMENTAL SCENARIO", "Report",
                "Initialization", "Research Conclusion", "VALIDATION REPORT"
            ])

            if is_static_header:
                if self.has_active_lines:
                    self.stream.write("\n")
                    self.has_active_lines = False
                self.stream.write(msg + "\n")
                self.stream.flush()
                return

            is_master = any(kw in raw_msg for kw in ["Master Progress:", "Master Fast-Path:", "Master Downstream Forwarder:", "[MASTER]"])
            is_sensor = "[SENSOR]" in raw_msg

            if is_master:
                if not self.has_active_lines:
                    self.stream.write(f"\r{msg}\033[K\n")
                    self.stream.flush()
                    self.has_active_lines = True
                    self.current_line_is_sensor = False
                else:
                    if self.current_line_is_sensor:
                        self.stream.write(f"\033[A\r{msg}\033[K\n")
                        self.stream.flush()
                    else:
                        self.stream.write(f"\r{msg}\033[K\n")
                        self.stream.flush()
            elif is_sensor:
                if not self.has_active_lines:
                    self.stream.write("\r[MASTER] Gateway Active\033[K\n")
                    self.stream.write(f"\r{msg}\033[K")
                    self.stream.flush()
                    self.has_active_lines = True
                    self.current_line_is_sensor = True
                else:
                    if not self.current_line_is_sensor:
                        self.stream.write(f"\r{msg}\033[K")
                        self.stream.flush()
                        self.current_line_is_sensor = True
                    else:
                        self.stream.write(f"\r{msg}\033[K")
                        self.stream.flush()
            else:
                if self.has_active_lines:
                    self.stream.write("\n")
                    self.has_active_lines = False
                self.stream.write(msg + "\n")
                self.stream.flush()
        except Exception:
            self.handleError(record)


console_handler = DualLineConsoleHandler()

console_handler.setFormatter(formatter)

console_handler.setLevel(
    logging.DEBUG if ENABLE_DEBUG_MODE else logging.INFO
)


# =============================================================================
# REGISTER HANDLERS
# =============================================================================

if ENABLE_LOGGING and not logger.handlers:

    logger.addHandler(file_handler)

    logger.addHandler(console_handler)


# =============================================================================
# STARTUP MESSAGE (ONLY MAIN PROCESS)
# =============================================================================

import multiprocessing

if multiprocessing.current_process().name == 'MainProcess':
    logger.info("=" * 80)
    logger.info(f"{SIMULATOR_NAME} Logger Initialized")
    logger.info(f"Log File : {LOG_FILE}")
    logger.info(f"Session Started : {datetime.now()}")
    logger.info("=" * 80)


# =============================================================================
# RIACC LOGGER
# =============================================================================

class RIACCLogger:
    """
    Centralized logger used throughout the RIACC simulator.

    This class wraps Python's logging module and provides a unified interface
    for every simulator component.
    """

    def __init__(self):
        self.logger = logger

    # =========================================================================
    # INTERNAL LOGGER
    # =========================================================================

    def _log(self, level: str, message: str):
        """
        Internal logging function.

        Parameters
        ----------
        level : str
            INFO, WARNING, ERROR, DEBUG

        message : str
            Log message.
        """

        if not ENABLE_LOGGING:
            return

        level = level.upper()

        if level == "INFO":
            self.logger.info(message)

        elif level == "WARNING":
            self.logger.warning(message)

        elif level == "ERROR":
            self.logger.error(message)

        elif level == "DEBUG":

            if ENABLE_DEBUG_MODE:
                self.logger.debug(message)

    # =========================================================================
    # GENERAL LOGGING
    # =========================================================================

    def info(self, message: str):
        """Log an informational message."""
        self._log("INFO", message)

    def warning(self, message: str):
        """Log a warning message."""
        self._log("WARNING", message)

    def error(self, message: str):
        """Log an error message."""
        self._log("ERROR", message)

    def debug(self, message: str):
        """Log a debug message."""
        self._log("DEBUG", message)

    # =========================================================================
    # SECTION HEADER
    # =========================================================================

    def section(self, title: str):
        """
        Print a formatted section header.
        """

        line = "=" * 80

        self.info(line)

        self.info(title)

        self.info(line)

    # =========================================================================
    # SUBSECTION HEADER
    # =========================================================================

    def subsection(self, title: str):
        """
        Print a formatted subsection header.
        """

        line = "-" * 60

        self.info(line)

        self.info(title)

        self.info(line)


 
# =============================================================================
# SPECIALIZED LOGGING METHODS
# =============================================================================

    def dataset(self, message: str):
        """Log dataset-related events."""
        self.info(f"[DATASET] {message}")

    def simulation(self, message: str):
        """Log simulation events."""
        self.info(f"[SIMULATION] {message}")

    def protocol(self, protocol: str, message: str):
        """Log protocol-related events."""
        self.info(f"[{protocol}] {message}")

    def node(self, node_id: str, message: str):
        """Log Intelligent Sensor Node events."""
        self.info(f"[NODE-{node_id}] {message}")

    def master(self, message: str):
        """Log Intelligent Master Node events."""
        self.info(f"[MASTER] {message}")

    def scheduler(self, message: str):
        """Log scheduler decisions."""
        self.info(f"[SCHEDULER] {message}")

    def channel(self, message: str):
        """Log LoRa channel events."""
        self.info(f"[CHANNEL] {message}")

    def packet(self, packet_id: str, message: str):
        """Log packet events."""
        self.info(f"[PACKET-{packet_id}] {message}")

    def adr(self, message: str):
        """Log Adaptive Data Rate events."""
        self.info(f"[ADR] {message}")

    def ati(self, node_id: str, ati: float):
        """Log Adaptive Threat Index values."""
        self.info(f"[ATI] Node={node_id} ATI={ati:.4f}")

    def burst(self, node_id: str, burst_score: float):
        """Log Burst Detection results."""
        self.info(f"[BURST] Node={node_id} Score={burst_score:.4f}")

    def arbitration(self, node_id: str, score: float):
        """Log Arbitration Score."""
        self.info(f"[ARBITRATION] Node={node_id} Score={score:.4f}")

    def performance(self, metric: str, value):
        """Log performance metrics."""
        self.info(f"[PERFORMANCE] {metric} = {value}")

    def statistics(self, message: str):
        """Log statistics generation."""
        self.info(f"[STATISTICS] {message}")

    def validation(self, message: str):
        """Log research validation."""
        self.info(f"[VALIDATION] {message}")

    def experiment(self, message: str):
        """Log experiment execution."""
        self.info(f"[EXPERIMENT] {message}")

    def exception(self, exception: Exception):
        """Log an exception with traceback."""
        if ENABLE_LOGGING:
            self.logger.exception(exception)

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def banner(self, title: str):
        """
        Print a formatted banner.
        """
        border = "=" * 80

        self.info("")
        self.info(border)
        self.info(title.center(80))
        self.info(border)
        self.info("")

    def separator(self, character: str = "-", length: int = 80):
        """
        Print a separator line.
        """
        self.info(character * length)

    def clear_log_file(self):
        """
        Clear the log file.
        """
        try:
            with open(LOG_FILE, "w", encoding="utf-8"):
                pass

            self.info("Log file cleared.")

        except Exception as exc:
            self.logger.exception(exc)

    def shutdown(self):
        """
        Close all logging handlers safely.
        """
        self.info("Shutting down logger...")

        handlers = self.logger.handlers[:]

        for handler in handlers:
            handler.flush()
            handler.close()
            self.logger.removeHandler(handler)

    def log_simulation_summary(
        self,
        protocol: str,
        execution_time: float,
        packets: int,
        success: int,
    ):
        """
        Print a concise simulation summary.
        """
        pdr = (success / packets * 100) if packets > 0 else 0.0

        self.banner(f"{protocol} Simulation Summary")

        self.performance(
            "Execution Time (s)",
            f"{execution_time:.3f}"
        )

        self.performance(
            "Generated Packets",
            packets
        )

        self.performance(
            "Successful Packets",
            success
        )

        self.performance(
            "Packet Delivery Ratio (%)",
            f"{pdr:.2f}"
        )

# =============================================================================
# GLOBAL LOGGER INSTANCE
# =============================================================================

riacc_logger = RIACCLogger()