# =============================================================================
# simulation_clock.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Simulation Clock
#
# Provides a virtual clock for discrete-event simulation.
# =============================================================================

from dataclasses import dataclass
from typing import Optional
import time


@dataclass
class ClockSnapshot:
    """
    Snapshot of simulation time.
    """

    simulation_time: float
    real_time: float
    speed_factor: float


class SimulationClock:
    """
    Virtual simulation clock.

    Features
    --------
    ✔ Start
    ✔ Pause
    ✔ Resume
    ✔ Stop
    ✔ Manual Tick
    ✔ Fast Forward
    ✔ Reset
    ✔ Time Snapshot
    """

    def __init__(self, speed_factor: float = 1.0):

        self.speed_factor = speed_factor

        self.simulation_time = 0.0

        self.running = False

        self.paused = False

        self.start_real_time: Optional[float] = None

        self.last_real_time: Optional[float] = None

    # ------------------------------------------------------------------

    def start(self):

        if self.running:
            return

        now = time.time()

        self.start_real_time = now

        self.last_real_time = now

        self.running = True

        self.paused = False

    # ------------------------------------------------------------------

    def stop(self):

        self.running = False

        self.paused = False

    # ------------------------------------------------------------------

    def pause(self):

        if self.running:

            self.paused = True

    # ------------------------------------------------------------------

    def resume(self):

        if self.running and self.paused:

            self.last_real_time = time.time()

            self.paused = False

    # ------------------------------------------------------------------

    def tick(self):
        """
        Advance simulation according to elapsed real time.
        """

        if not self.running:

            return self.simulation_time

        if self.paused:

            return self.simulation_time

        current = time.time()

        elapsed = current - self.last_real_time

        self.simulation_time += elapsed * self.speed_factor

        self.last_real_time = current

        return self.simulation_time

    # ------------------------------------------------------------------

    def advance(self, seconds: float):
        """
        Advance simulation manually.
        """

        if seconds < 0:
            raise ValueError("Cannot move simulation backwards.")

        self.simulation_time += seconds

    # ------------------------------------------------------------------

    def fast_forward(self, seconds: float):

        self.advance(seconds)

    # ------------------------------------------------------------------

    def reset(self):

        self.simulation_time = 0.0

        self.running = False

        self.paused = False

        self.start_real_time = None

        self.last_real_time = None

    # ------------------------------------------------------------------

    def now(self) -> float:

        return self.simulation_time

    # ------------------------------------------------------------------

    def snapshot(self) -> ClockSnapshot:

        return ClockSnapshot(

            simulation_time=self.simulation_time,

            real_time=time.time(),

            speed_factor=self.speed_factor,

        )

    # ------------------------------------------------------------------

    def is_running(self) -> bool:

        return self.running

    # ------------------------------------------------------------------

    def is_paused(self) -> bool:

        return self.paused

    # ------------------------------------------------------------------

    def set_speed(self, speed: float):

        if speed <= 0:

            raise ValueError("Speed factor must be positive.")

        self.speed_factor = speed

    # ------------------------------------------------------------------

    def __repr__(self):

        return (

            f"SimulationClock("
            f"time={self.simulation_time:.3f}, "
            f"running={self.running}, "
            f"paused={self.paused}, "
            f"speed={self.speed_factor}x)"

        )


    