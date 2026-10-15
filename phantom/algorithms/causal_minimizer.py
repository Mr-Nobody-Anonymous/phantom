"""
PHANTOM Causal Minimizer: Trace-guided causal minimization engine.
Combines partial-order event graphs, backward causal slicing, counterfactual
perturbation testing, topological delta debugging, and individual necessity ledger.
Enhanced with search budgets, ablation flags, checkpointing, and minimality level classification.
"""

from __future__ import annotations
import itertools
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from phantom.algorithms.base import (
    AblationConfig,
    BaseMinimizer,
    MinimalityLevel,
    MinimizerResult,
    SearchBudget,
)
from phantom.core.event import Event, EventType
from phantom.core.graph import CausalGraph, CausalInconsistencyError
from phantom.core.oracle import FailureOracle, OracleResult
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class PhantomCausalMinimizer(BaseMinimizer):
    """
    Automated failure-reconstruction engine that uses trace-guided causal analysis
    to find the minimal reproducible failure schedule with minimal test executions.
    """

    def __init__(
        self,
        max_executions: int = 200,
        seed: Optional[int] = 42,
        time_budget_seconds: Optional[float] = None,
        checkpoint_file: Optional[str | Path] = None,
        resume: bool = False,
        ablation: Optional[AblationConfig] = None,
        strict_graph: bool = False,
    ):
        super().__init__(
            name="PHANTOM_CausalMinimizer",
            max_executions=max_executions,
            seed=seed,
            time_budget_seconds=time_budget_seconds,
            checkpoint_file=checkpoint_file,
            resume=resume,
            ablation=ablation,
            strict_graph=strict_graph,
        )

    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        start_time = time.perf_counter()
        history: List[Dict[str, Any]] = []
        executions = 0
        termination_reason = "CONVERGED"
        divergences: List[Dict[str, Any]] = []
        trace_hash = original_trace.compute_hash() if hasattr(original_trace, "compute_hash") else ""

        all_ids = [e.event_id for e in original_trace.events]
        total_events = len(all_ids)
        fail_id = original_trace.failure_event_id

        # 0. Initial baseline check
        initial_check = scheduler.replay(original_trace, oracle)
        executions += 1
        divergences.extend(getattr(initial_check, "divergences", []))
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
                minimality_level=MinimalityLevel.UNMINIMIZED,
                termination_reason="INITIAL_REPLAY_FAILED",
                search_history=history,
                source_trace_hash=trace_hash,
            )

        # 1. Build Causal Dependency Graph
        causal_graph = CausalGraph(original_trace, strict_mode=self.strict_graph)
        uncertain_causality = causal_graph.is_uncertain
        uncertainty_reasons = list(causal_graph.uncertainty_reasons)

        # Check for resume checkpoint
        resumed = False
        loaded_checkpoint = self.load_checkpoint()
        if loaded_checkpoint and "active_set" in loaded_checkpoint:
            active_set = set(loaded_checkpoint["active_set"])
            executions = loaded_checkpoint.get("step", executions)
            history = loaded_checkpoint.get("history", history)
            resumed = True
        else:
            active_set = set(all_ids)

        # 2. Phase 1: Backward Causal Cone Extraction (Causal Slicing)
        if not resumed and self.ablation.enable_causal_slicing:
            if fail_id:
                causal_cone = causal_graph.causal_cone(fail_id)
            else:
                causal_cone = set(all_ids)

            if len(causal_cone) < len(active_set):
                exhausted, reason = self.budget.is_exhausted(executions, time.perf_counter() - start_time)
                if not exhausted:
                    cand_trace = original_trace.filter(causal_cone)
                    res = scheduler.replay(cand_trace, oracle)
                    executions += 1
                    divergences.extend(getattr(res, "divergences", []))
                    history.append({
                        "step": executions,
                        "phase": "causal_slicing",
                        "size": len(causal_cone),
                        "verdict": res.verdict.value,
                    })
                    if res.is_reproduced:
                        active_set = set(causal_cone)
                        self.save_checkpoint(executions, active_set, history, "causal_slicing")

        # 3. Phase 2: Counterfactual Fault Pruning
        fault_pruned = False
        if self.ablation.enable_fault_pruning:
            fault_events = [e for e in original_trace.events if e.is_fault() and e.event_id in active_set]
            for fe in fault_events:
                exhausted, reason = self.budget.is_exhausted(executions, time.perf_counter() - start_time)
                if exhausted:
                    termination_reason = reason
                    break
                without_fault = active_set - {fe.event_id}
                cand_trace = original_trace.filter(without_fault)
                res = scheduler.replay(cand_trace, oracle)
                executions += 1
                divergences.extend(getattr(res, "divergences", []))
                history.append({
                    "step": executions,
                    "phase": "fault_necessity_test",
                    "tested_fault": fe.event_id,
                    "size": len(without_fault),
                    "verdict": res.verdict.value,
                })
                if res.is_reproduced:
                    active_set = without_fault
                    self.save_checkpoint(executions, active_set, history, "fault_necessity_test")
            fault_pruned = True

        # 4. Phase 3: Causal Delta Debugging (Topological Partition Pruning)
        if self.ablation.enable_delta_debugging:
            critical_path = set(causal_graph.critical_path_to_failure())
            non_critical = [eid for eid in active_set if eid not in critical_path and eid != fail_id]
            chunk_size = max(1, len(non_critical) // 2)

            while chunk_size >= 1 and non_critical:
                exhausted, reason = self.budget.is_exhausted(executions, time.perf_counter() - start_time)
                if exhausted:
                    termination_reason = reason
                    break
                improved = False
                for i in range(0, len(non_critical), chunk_size):
                    exhausted, reason = self.budget.is_exhausted(executions, time.perf_counter() - start_time)
                    if exhausted:
                        termination_reason = reason
                        break
                    block = set(non_critical[i : i + chunk_size])
                    cand = active_set - block
                    if fail_id:
                        cand.add(fail_id)
                    cand_trace = original_trace.filter(cand)
                    res = scheduler.replay(cand_trace, oracle)
                    executions += 1
                    divergences.extend(getattr(res, "divergences", []))
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
                        self.save_checkpoint(executions, active_set, history, "topological_pruning")
                        break
                if not improved:
                    chunk_size //= 2

        # 5. Phase 4: Individual Causal Necessity Ledger
        necessary_events: List[str] = []
        irrelevant_events: List[str] = []
        ledger_completed = False

        if self.ablation.enable_necessity_ledger:
            removable_candidates = [eid for eid in list(active_set) if eid != fail_id]
            for eid in removable_candidates:
                exhausted, reason = self.budget.is_exhausted(executions, time.perf_counter() - start_time)
                if exhausted:
                    termination_reason = reason
                    break
                without_e = active_set - {eid}
                cand_trace = original_trace.filter(without_e)
                res = scheduler.replay(cand_trace, oracle)
                executions += 1
                divergences.extend(getattr(res, "divergences", []))
                history.append({
                    "step": executions,
                    "phase": "necessity_ledger",
                    "tested_event": eid,
                    "size": len(without_e),
                    "verdict": res.verdict.value,
                })
                if res.is_reproduced:
                    active_set = without_e
                    irrelevant_events.append(eid)
                    self.save_checkpoint(executions, active_set, history, "necessity_ledger")
                else:
                    necessary_events.append(eid)
            else:
                ledger_completed = True

        if fail_id:
            active_set.add(fail_id)
            if fail_id not in necessary_events:
                necessary_events.append(fail_id)

        minimized_trace = original_trace.filter(active_set)
        elapsed = time.perf_counter() - start_time

        # 6. Minimality Level Classification
        minimality = MinimalityLevel.UNMINIMIZED
        if ledger_completed:
            minimality = MinimalityLevel.ONE_EVENT_MINIMAL
            # Check bounded global minimality if trace is sufficiently reduced (<= 5 events)
            if len(minimized_trace.events) <= 5 and not self.budget.is_exhausted(executions, elapsed)[0]:
                is_globally_minimal = True
                removable = [e.event_id for e in minimized_trace.events if e.event_id != fail_id]
                for r in range(2, len(removable)):
                    for subset in itertools.combinations(removable, r):
                        exhausted, _ = self.budget.is_exhausted(executions, time.perf_counter() - start_time)
                        if exhausted:
                            break
                        cand = active_set - set(subset)
                        if fail_id:
                            cand.add(fail_id)
                        res = scheduler.replay(original_trace.filter(cand), oracle)
                        executions += 1
                        if res.is_reproduced:
                            is_globally_minimal = False
                            active_set = cand
                            minimized_trace = original_trace.filter(active_set)
                            break
                    if not is_globally_minimal:
                        break
                if is_globally_minimal:
                    minimality = MinimalityLevel.GLOBALLY_MINIMAL

        # 7. Phase 5: Stability and Confidence Verification
        stability = self.verify_stability(minimized_trace, oracle, scheduler, repeat_count=10)
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
            minimality_level=minimality,
            termination_reason=termination_reason,
            search_history=history,
            initial_state=dict(scheduler.initial_state),
            divergences_detected=divergences,
            source_trace_hash=trace_hash,
            resumed_from_checkpoint=resumed,
            ablation_config=self.ablation.to_dict(),
            uncertain_causality=uncertain_causality,
            uncertainty_reasons=uncertainty_reasons,
        )
