"""
Base interfaces and result metrics for failure minimization algorithms.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional, Set

from phantom.core.oracle import FailureOracle, OracleResult
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


@dataclass
class MinimizerResult:
    """Detailed benchmark and reconstruction result for an algorithm execution."""
    algorithm_name: str
    original_event_count: int
    minimized_event_count: int
    reduction_ratio: float
    executions_count: int
    reproduction_rate: float
    reproduced: bool
    failure_preserved: bool
    wall_clock_time: float
    minimized_trace: Trace
    retained_event_ids: List[str] = field(default_factory=list)
    necessary_events: List[str] = field(default_factory=list)
    irrelevant_events: List[str] = field(default_factory=list)
    confidence_score: float = 0.0
    search_history: List[Dict[str, Any]] = field(default_factory=list)
    initial_state: Dict[str, Any] = field(default_factory=dict)

    def summary(self) -> Dict[str, Any]:
        return {
            "algorithm": self.algorithm_name,
            "original_events": self.original_event_count,
            "minimized_events": self.minimized_event_count,
            "reduction_ratio": round(self.reduction_ratio, 4),
            "reduction_percentage": f"{round(self.reduction_ratio * 100, 1)}%",
            "executions_count": self.executions_count,
            "reproduced": self.reproduced,
            "failure_preserved": self.failure_preserved,
            "reproduction_rate": round(self.reproduction_rate, 3),
            "confidence_score": round(self.confidence_score, 3),
            "wall_clock_seconds": round(self.wall_clock_time, 4),
            "necessary_events_count": len(self.necessary_events),
            "irrelevant_events_count": len(self.irrelevant_events),
        }


class BaseMinimizer(ABC):
    """Abstract base class for trace minimization and failure reconstruction."""

    def __init__(self, name: str, max_executions: int = 200, seed: Optional[int] = 42):
        self.name = name
        self.max_executions = max_executions
        self.seed = seed

    @abstractmethod
    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        """Find the minimal subtrace reproducing the target failure."""
        pass

    def verify_stability(
        self,
        candidate_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
        repeat_count: int = 10,
    ) -> float:
        """Run repeated replays to measure reproduction stability."""
        if repeat_count <= 0 or not candidate_trace.events:
            return 0.0
        successes = 0
        for _ in range(repeat_count):
            res = scheduler.replay(candidate_trace, oracle)
            if res.is_reproduced:
                successes += 1
        return successes / repeat_count
