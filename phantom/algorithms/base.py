"""
Base interfaces, search budgets, ablation configs, and result metrics for failure minimization algorithms.
Reflects explicit minimality levels (1-minimal, fault-minimal, globally-minimal),
checkpointing, and multi-budget termination reasons.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from phantom.core.oracle import FailureOracle, OracleResult
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class MinimalityLevel(str, Enum):
    """Rigorous classification of schedule minimality claims."""
    ONE_EVENT_MINIMAL = "1-MINIMAL"        # Removing any single event prevents reproduction under tested conditions
    FAULT_MINIMAL = "FAULT-MINIMAL"        # All remaining fault injection events are proven counterfactually necessary
    GLOBALLY_MINIMAL = "GLOBALLY-MINIMAL"  # Smallest valid schedule proven by bounded exhaustive subset exploration
    UNMINIMIZED = "UNMINIMIZED"            # Not proven minimal or search terminated early


@dataclass
class SearchBudget:
    """Configurable search execution, time, and resource limits."""
    max_executions: int = 200
    time_budget_seconds: Optional[float] = None
    max_fault_injections: Optional[int] = None
    max_memory_mb: Optional[float] = None

    def is_exhausted(self, executions: int, elapsed_time: float) -> Tuple[bool, str]:
        if executions >= self.max_executions:
            return True, "BUDGET_EXECUTIONS_EXHAUSTED"
        if self.time_budget_seconds and elapsed_time >= self.time_budget_seconds:
            return True, "BUDGET_TIME_EXHAUSTED"
        return False, "RUNNING"


@dataclass
class AblationConfig:
    """Ablation flags to isolate and measure contributions of individual PHANTOM subsystems."""
    enable_causal_slicing: bool = True
    enable_fault_pruning: bool = True
    enable_conflict_prioritization: bool = True
    enable_delta_debugging: bool = True
    enable_necessity_ledger: bool = True

    def to_dict(self) -> Dict[str, bool]:
        return {
            "causal_slicing": self.enable_causal_slicing,
            "fault_pruning": self.enable_fault_pruning,
            "conflict_prioritization": self.enable_conflict_prioritization,
            "delta_debugging": self.enable_delta_debugging,
            "necessity_ledger": self.enable_necessity_ledger,
        }


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
    minimality_level: MinimalityLevel = MinimalityLevel.ONE_EVENT_MINIMAL
    termination_reason: str = "CONVERGED"
    search_history: List[Dict[str, Any]] = field(default_factory=list)
    initial_state: Dict[str, Any] = field(default_factory=dict)
    divergences_detected: List[Dict[str, Any]] = field(default_factory=list)
    source_trace_hash: str = ""
    resumed_from_checkpoint: bool = False
    ablation_config: Dict[str, Any] = field(default_factory=dict)
    uncertain_causality: bool = False
    uncertainty_reasons: List[str] = field(default_factory=list)

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
            "minimality_level": self.minimality_level.value,
            "termination_reason": self.termination_reason,
            "reproduction_rate": round(self.reproduction_rate, 3),
            "confidence_score": round(self.confidence_score, 3),
            "wall_clock_seconds": round(self.wall_clock_time, 4),
            "necessary_events_count": len(self.necessary_events),
            "irrelevant_events_count": len(self.irrelevant_events),
            "source_trace_hash": self.source_trace_hash[:12] if self.source_trace_hash else "",
            "divergences_count": len(self.divergences_detected),
            "uncertain_causality": self.uncertain_causality,
        }


class BaseMinimizer(ABC):
    """Abstract base class for trace minimization and failure reconstruction."""

    def __init__(
        self,
        name: str,
        max_executions: int = 200,
        seed: Optional[int] = 42,
        time_budget_seconds: Optional[float] = None,
        checkpoint_file: Optional[str | Path] = None,
        resume: bool = False,
        ablation: Optional[AblationConfig] = None,
        strict_graph: bool = False,
    ):
        self.name = name
        self.max_executions = max_executions
        self.seed = seed
        self.budget = SearchBudget(
            max_executions=max_executions,
            time_budget_seconds=time_budget_seconds,
        )
        self.checkpoint_file = Path(checkpoint_file) if checkpoint_file else None
        self.resume = resume
        self.ablation = ablation or AblationConfig()
        self.strict_graph = strict_graph

    @abstractmethod
    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        """Find the minimal subtrace reproducing the target failure."""
        pass

    def save_checkpoint(
        self,
        step: int,
        active_set: Set[str],
        history: List[Dict[str, Any]],
        phase: str,
    ) -> None:
        """Persist intermediate search state for crash resilience."""
        if not self.checkpoint_file:
            return
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "algorithm": self.name,
            "step": step,
            "phase": phase,
            "active_set": sorted(list(active_set)),
            "history": history,
            "timestamp": time.time(),
        }
        self.checkpoint_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load persisted checkpoint state if available and resume requested."""
        if not self.checkpoint_file or not self.resume or not self.checkpoint_file.exists():
            return None
        try:
            return json.loads(self.checkpoint_file.read_text(encoding="utf-8"))
        except Exception:
            return None

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
