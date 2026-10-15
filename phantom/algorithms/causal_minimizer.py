"""
PHANTOM Causal Minimizer: Trace-guided causal minimization engine.
Combines partial-order event graphs, backward causal slicing, counterfactual
perturbation testing, and causal delta debugging.
"""

from __future__ import annotations
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from phantom.algorithms.base import BaseMinimizer, MinimizerResult
from phantom.core.event import Event, EventType
from phantom.core.graph import CausalGraph
from phantom.core.oracle import FailureOracle, OracleResult
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class PhantomCausalMinimizer(BaseMinimizer):
    """
    Automated failure-reconstruction engine that uses trace-guided causal analysis
    to find the minimal reproducible failure schedule with minimal test executions.
    """

    def __init__(self, max_executions: int = 200, seed: Optional[int] = 42):
        super().__init__(name="PHANTOM_CausalMinimizer", max_executions=max_executions, seed=seed)

    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        start_time = time.perf_counter()
        history: List[Dict[str, Any]] = []
        executions = 0

        all_ids = [e.event_id for e in original_trace.events]
        total_events = len(all_ids)
        fail_id = original_trace.failure_event_id

        # 0. Initial baseline check
        initial_check = scheduler.replay(original_trace, oracle)
        executions += 1
        history.append({
            "step": executions,
            "phase": "initial_validation",
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

        # 1. Build Causal Dependency Graph
        causal_graph = CausalGraph(original_trace)

        # 2. Phase 1: Backward Causal Cone Extraction (Causal Slicing)
        # Events outside the causal cone of the failure event cannot causally affect the failure.
        if fail_id:
            causal_cone = causal_graph.causal_cone(fail_id)
        else:
            causal_cone = set(all_ids)

        # Retain events in the causal cone
        active_set: Set[str] = set(causal_cone)
        if len(active_set) < total_events:
            # Verify that the causal cone alone reproduces the failure
            cand_trace = original_trace.filter(active_set)
            res = scheduler.replay(cand_trace, oracle)
            executions += 1
            history.append({
                "step": executions,
                "phase": "causal_slicing",
                "size": len(active_set),
                "verdict": res.verdict.value,
            })
            if not res.is_reproduced:
                # Fallback to full set if external side-effects leaked
                active_set = set(all_ids)

        # 3. Phase 2: Counterfactual Fault Pruning
        # Injected faults (delays, drops, restarts) are tested for causal necessity.
        # If removing a fault still reproduces the failure, it is an irrelevant perturbation.
        fault_events = [e for e in original_trace.events if e.is_fault() and e.event_id in active_set]
        for fe in fault_events:
            if executions >= self.max_executions:
                break
            without_fault = active_set - {fe.event_id}
            cand_trace = original_trace.filter(without_fault)
            res = scheduler.replay(cand_trace, oracle)
            executions += 1
            history.append({
                "step": executions,
                "phase": "fault_necessity_test",
                "tested_fault": fe.event_id,
                "size": len(without_fault),
                "verdict": res.verdict.value,
            })
            if res.is_reproduced:
                # Fault was unnecessary; discard it!
                active_set = without_fault

        # 4. Phase 3: Causal Delta Debugging (Topological Partition Pruning)
        # Prioritize pruning non-critical path events first.
        critical_path = set(causal_graph.critical_path_to_failure())
        non_critical = [eid for eid in active_set if eid not in critical_path and eid != fail_id]

        # Try removing blocks of non-critical events
        chunk_size = max(1, len(non_critical) // 2)
        while chunk_size >= 1 and executions < self.max_executions and non_critical:
            improved = False
            for i in range(0, len(non_critical), chunk_size):
                if executions >= self.max_executions:
                    break
                block = set(non_critical[i : i + chunk_size])
                cand = active_set - block
                if fail_id:
                    cand.add(fail_id)
                cand_trace = original_trace.filter(cand)
                res = scheduler.replay(cand_trace, oracle)
                executions += 1
                history.append({
                    "step": executions,
                    "phase": "topological_pruning",
                    "pruned_block_size": len(block),
                    "size": len(cand),
                    "verdict": res.verdict.value,
                })
                if res.is_reproduced:
                    active_set = cand
                    non_critical = [eid for eid in non_critical if eid not in block]
                    improved = True
                    break
            if not improved:
                chunk_size //= 2

        # 5. Phase 4: Individual Causal Necessity Ledger
        # For each remaining event, test whether it is strictly necessary.
        necessary_events: List[str] = []
        irrelevant_events: List[str] = []
        removable_candidates = [eid for eid in list(active_set) if eid != fail_id]

        for eid in removable_candidates:
            if executions >= self.max_executions:
                break
            without_e = active_set - {eid}
            cand_trace = original_trace.filter(without_e)
            res = scheduler.replay(cand_trace, oracle)
            executions += 1
            history.append({
                "step": executions,
                "phase": "necessity_ledger",
                "tested_event": eid,
                "size": len(without_e),
                "verdict": res.verdict.value,
            })
            if res.is_reproduced:
                # Event is irrelevant for reproducing the failure
                active_set = without_e
                irrelevant_events.append(eid)
            else:
                # Removing it prevented the failure -> event is CAUSALLY NECESSARY!
                necessary_events.append(eid)

        if fail_id:
            active_set.add(fail_id)
            if fail_id not in necessary_events:
                necessary_events.append(fail_id)

        minimized_trace = original_trace.filter(active_set)
        elapsed = time.perf_counter() - start_time

        # 6. Phase 5: Stability and Confidence Verification
        stability = self.verify_stability(minimized_trace, oracle, scheduler, repeat_count=10)
        # Statistical confidence: probability of true reproduction
        confidence = 1.0 - ((1.0 - max(0.01, stability)) ** 5) if stability > 0 else 0.0

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
            retained_event_ids=sorted(list(active_set)),
            necessary_events=necessary_events,
            irrelevant_events=irrelevant_events,
            confidence_score=confidence,
            search_history=history,
        )
