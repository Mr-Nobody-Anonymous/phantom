"""
Baseline 2: Classical Delta Debugging (ddmin) algorithm.
Adapted from Andreas Zeller & Ralf Hildebrandt (IEEE TSE 2002) for event traces.
"""

from __future__ import annotations
import math
import time
from typing import Any, Dict, List, Optional, Set

from phantom.algorithms.base import BaseMinimizer, MinimizerResult
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class DeltaDebuggingMinimizer(BaseMinimizer):
    """
    Classical ddmin implementation that minimizes event traces to a 1-minimal
    failure-inducing subset by testing partitions and their complements.
    """

    def __init__(self, max_executions: int = 200, seed: Optional[int] = 42):
        super().__init__(name="DeltaDebugging_ddmin", max_executions=max_executions, seed=seed)

    def _split(self, items: List[str], n: int) -> List[List[str]]:
        """Split a list of items into n roughly equal chunks."""
        k = len(items)
        chunk_size = math.ceil(k / n)
        chunks = []
        for i in range(0, k, chunk_size):
            chunks.append(items[i : i + chunk_size])
        return chunks

    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        start_time = time.perf_counter()
        history: List[Dict[str, Any]] = []

        all_ids = [e.event_id for e in original_trace.events]
        total_events = len(all_ids)
        fail_id = original_trace.failure_event_id

        # Initial check
        initial_check = scheduler.replay(original_trace, oracle)
        executions = 1
        history.append({
            "step": executions,
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

        # Delta debugging operates on removable events (excluding required failure assertion event)
        c_items = [eid for eid in all_ids if eid != fail_id]
        n = 2

        def test_candidate(subset_items: List[str]) -> bool:
            nonlocal executions
            if executions >= self.max_executions:
                return False
            cand_set = set(subset_items)
            if fail_id:
                cand_set.add(fail_id)
            cand_trace = original_trace.filter(cand_set)
            res = scheduler.replay(cand_trace, oracle)
            executions += 1
            history.append({
                "step": executions,
                "size": len(cand_set),
                "verdict": res.verdict.value,
            })
            return res.is_reproduced

        while len(c_items) >= 2 and executions < self.max_executions:
            subsets = self._split(c_items, min(n, len(c_items)))
            reduced = False

            # 1. Test subsets (Δ_i)
            for sub in subsets:
                if len(sub) < len(c_items) and test_candidate(sub):
                    c_items = sub
                    n = 2
                    reduced = True
                    break

            if reduced:
                continue

            # 2. Test complements (c_items \ Δ_i)
            for sub in subsets:
                complement = [item for item in c_items if item not in sub]
                if complement and len(complement) < len(c_items) and test_candidate(complement):
                    c_items = complement
                    n = max(n - 1, 2)
                    reduced = True
                    break

            if reduced:
                continue

            # 3. Increase granularity if neither succeeded
            if n < len(c_items):
                n = min(len(c_items), 2 * n)
            else:
                break

        final_subset = set(c_items)
        if fail_id:
            final_subset.add(fail_id)

        minimized_trace = original_trace.filter(final_subset)
        elapsed = time.perf_counter() - start_time
        stability = self.verify_stability(minimized_trace, oracle, scheduler, repeat_count=5)
        min_size = len(minimized_trace.events)
        reduction = 1.0 - (min_size / total_events) if total_events > 0 else 0.0

        return MinimizerResult(
            algorithm_name=self.name,
            original_event_count=total_events,
            minimized_event_count=min_size,
            reduction_ratio=reduction,
            executions_count=executions,
            reproduction_rate=stability,
            reproduced=True,
            failure_preserved=True,
            wall_clock_time=elapsed,
            minimized_trace=minimized_trace,
            retained_event_ids=sorted(list(final_subset)),
            search_history=history,
        )
