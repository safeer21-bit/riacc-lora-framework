# =============================================================================
# event_queue.py
#
# A Runtime Intelligence and Adaptive Control-Based Communication Management Framework for LoRa Networks (RIACC)
# Event Queue
#
# Discrete Event Simulation Queue
# =============================================================================

from dataclasses import dataclass, field
from typing import Callable, Optional
import heapq
import itertools


# =============================================================================
# EVENT
# =============================================================================

@dataclass(order=True)
class SimulationEvent:
    """
    Represents one simulation event.
    """

    event_time: float

    priority: int

    event_id: int = field(compare=True)

    event_type: str = field(compare=False)

    callback: Callable = field(compare=False)

    args: tuple = field(default_factory=tuple, compare=False)

    kwargs: dict = field(default_factory=dict, compare=False)

    description: str = field(default="", compare=False)

    cancelled: bool = field(default=False, compare=False)


# =============================================================================
# EVENT QUEUE
# =============================================================================

class EventQueue:
    """
    Priority Queue for discrete-event simulation.

    Events are always executed in chronological order.
    """

    def __init__(self):

        self._queue = []

        self._counter = itertools.count()

    # ------------------------------------------------------------------

    def schedule(
        self,
        event_time: float,
        event_type: str,
        callback: Callable,
        *args,
        priority: int = 0,
        description: str = "",
        **kwargs
    ) -> int:
        """
        Schedule a new event.
        """

        event = SimulationEvent(

            event_time=event_time,

            priority=priority,

            event_id=next(self._counter),

            event_type=event_type,

            callback=callback,

            args=args,

            kwargs=kwargs,

            description=description,

        )

        heapq.heappush(
            self._queue,
            event,
        )

        return event.event_id

    # ------------------------------------------------------------------

    def next_event(
        self,
    ) -> Optional[SimulationEvent]:
        """
        Returns the next scheduled event.
        """

        while self._queue:

            event = heapq.heappop(
                self._queue
            )

            if not event.cancelled:

                return event

        return None

    # ------------------------------------------------------------------

    def peek(
        self,
    ) -> Optional[SimulationEvent]:
        """
        View next event without removing it.
        """

        while self._queue:

            event = self._queue[0]

            if event.cancelled:

                heapq.heappop(self._queue)

                continue

            return event

        return None

    # ------------------------------------------------------------------

    def cancel(
        self,
        event_id: int,
    ) -> bool:
        """
        Cancel an event.
        """

        for event in self._queue:

            if event.event_id == event_id:

                event.cancelled = True

                return True

        return False

    # ------------------------------------------------------------------

    def execute_next(
        self,
    ):
        """
        Execute the next event.
        """

        event = self.next_event()

        if event is None:

            return None

        return event.callback(
            *event.args,
            **event.kwargs,
        )

    # ------------------------------------------------------------------

    def clear(
        self,
    ):

        self._queue.clear()

    # ------------------------------------------------------------------

    def size(
        self,
    ) -> int:

        return len(self._queue)

    # ------------------------------------------------------------------

    def empty(
        self,
    ) -> bool:

        return len(self._queue) == 0

    # ------------------------------------------------------------------

    def events(
        self,
    ):

        return sorted(
            self._queue,
            key=lambda e: (
                e.event_time,
                e.priority,
            ),
        )

    # ------------------------------------------------------------------

    def __len__(
        self,
    ):

        return len(self._queue)

    # ------------------------------------------------------------------

    def __repr__(
        self,
    ):

        return (

            f"EventQueue("
            f"events={len(self._queue)})"

        )


    