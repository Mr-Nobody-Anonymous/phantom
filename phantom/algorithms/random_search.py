"""
Baseline 1: Stochastic random search minimizer.
Uniformly samples subsets of events and timings without causal guidance.
"""

from __future__ import annotations
import random
import time
from typing import Any, Dict, List, Optional, Set

from phantom.algorithms.base import BaseMinimizer, MinimizerResult
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class RandomSearchMinimizer(BaseMinimizer):
    """
    Baseline minimizer: Uniformly samples random subsets of events and evaluates them.
    Tracks the minimal successful reproducing subset within a fixed execution budget.
    """

    def __init__(self, max_executions: int = 150, seed: Optional[int] = 42):
        super().__init__(name="RandomSearch", max_executions=max_executions, seed=seed)

    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        rng = random.Random(self.seed)
        start_time = time.perf_counter()

        all_ids = [e.event_id for e in original_trace.events]
        total_events = len(all_ids)
        history: List[Dict[str, Any]] = []

        # First verify the original trace reproduces the failure
        initial_check = scheduler.replay(original_trace, oracle)
        executions = 1
        history.append({
            "step": 1,
            "size": total_events,
            "verdict": initial_check.verdict.value,
        })

        if not initial_check.is_reproduced:
            return MinimizerResult(
                algorithm_name=self.name,
                original_event_count=total_events,
                minimized_event_count=total_events,
                reduction_ratio=0.0,
                executions_count=executions,
                reproduction_rate=0.0,
                reproduced=False,
                failure_preserved=False,
                wall_clock_time=time.perf_counter() - start_time,
                minimized_trace=original_trace,
                search_history=history,
            )

        best_subset: Set[str] = set(all_ids)
        best_size = total_events

        # Always keep failure event if present
        fail_id = original_trace.failure_event_id

        # Random subset exploration
        for step in range(2, self.max_executions + 1):
            # Sample subset size between 1 and total_events - 1
            sample_size = rng.randint(1, max(1, total_events - 1))
            other_ids = [eid for eid in all_ids if eid != fail_id]
            k = min(sample_size, len(other_ids))
            sampled = set(rng.sample(other_ids, k))
            if fail_id:
                sampled.add(fail_id)

            candidate_trace = original_trace.filter(sampled)
            res = scheduler.replay(candidate_trace, oracle)
            executions += 1
            history.append({
                "step": step,
                "size": len(sampled),
                "verdict": res.verdict.value,
            })

            if res.is_reproduced and len(sampled) < best_size:
                best_subset = sampled
                best_size = len(sampled)

        minimized_trace = original_trace.filter(best_subset)
        elapsed = time.perf_counter() - start_time
        stability = self.verify_stability(minimized_trace, oracle, scheduler, repeat_count=5)
        reduction = 1.0 - (best_size / total_events) if total_events > 0 else 0.0

        return MinimizerResult(
            algorithm_name=self.name,
            original_event_count=total_events,
            minimized_event_count=best_size,
            reduction_ratio=reduction,
            executions_count=executions,
            reproduction_rate=stability,
            reproduced=True,
            failure_preserved=True,
            wall_clock_time=elapsed,
            minimized_trace=minimized_trace,
            retained_event_ids=sorted(list(best_subset)),
            search_history=history,
        )
