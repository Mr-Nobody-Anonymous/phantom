"""
Benchmark case base models and harness definitions.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


@dataclass
class BenchmarkCase:
    """A benchmark failure scenario with execution trace, oracle, and scheduler."""
    case_id: str
    category: str  # "Benchmark A", "Benchmark B", "Benchmark C"
    name: str
    description: str
    trace: Trace
    oracle: FailureOracle
    scheduler: EventScheduler
    ground_truth_minimal_events: Set[str] = field(default_factory=set)

    def stats(self) -> Dict[str, Any]:
        return {
            "case_id": self.case_id,
            "category": self.category,
            "name": self.name,
            "total_events": len(self.trace.events),
            "fault_events": len(self.trace.fault_events()),
            "failure_signature": self.trace.failure_signature,
            "ground_truth_minimal": len(self.ground_truth_minimal_events),
        }
