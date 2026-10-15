"""Core data structures and runtime components for Project PHANTOM."""

from phantom.core.event import Event, EventType, VectorClock
from phantom.core.trace import Trace
from phantom.core.graph import CausalGraph
from phantom.core.oracle import FailureOracle, OracleResult, OracleVerdict
from phantom.core.scheduler import EventScheduler, SimulationState

__all__ = [
    "Event",
    "EventType",
    "VectorClock",
    "Trace",
    "CausalGraph",
    "FailureOracle",
    "OracleResult",
    "OracleVerdict",
    "EventScheduler",
    "SimulationState",
]
