"""
Project PHANTOM: Automated Failure-Reconstruction and Causal Minimization Engine.

Discovers why distributed software fails by learning the smallest set of event timings
and injected faults needed to deterministically reproduce a failure.
"""

from phantom.api import (
    analyze_trace,
    export_reproducer_bundle,
    import_otel_trace,
    minimize_trace,
    replay_trace,
    verify_reproducer_script,
)
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.graph import CausalGraph, CausalInconsistencyError, CausalCycleError
from phantom.core.oracle import FailureOracle, OracleResult, OracleVerdict
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace

__version__ = "0.1.0"
__author__ = "Mr-Nobody-Anonymous"

__all__ = [
    "Event",
    "EventType",
    "VectorClock",
    "Trace",
    "CausalGraph",
    "CausalInconsistencyError",
    "CausalCycleError",
    "FailureOracle",
    "OracleResult",
    "OracleVerdict",
    "EventScheduler",
    "minimize_trace",
    "analyze_trace",
    "replay_trace",
    "export_reproducer_bundle",
    "import_otel_trace",
    "verify_reproducer_script",
]
