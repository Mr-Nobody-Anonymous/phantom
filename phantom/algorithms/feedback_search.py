"""
Feedback-Guided Causal Search Engine for Project PHANTOM.
Component 3: Prioritizes unexplored and conflict-heavy event orderings based on
prior execution outcomes, causal happens-before constraints, and UCB uncertainty.
"""

from __future__ import annotations
import math
import random
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from phantom.algorithms.base import BaseMinimizer, MinimizerResult
from phantom.core.event import Event, EventType
from phantom.core.graph import CausalGraph
from phantom.core.oracle import FailureOracle, OracleResult
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class FeedbackGuidedSearchMinimizer(BaseMinimizer):
    """
    Search and minimization engine that prioritizes unexplored event orderings
    and fault timings based on causal dependency constraints and feedback from prior runs.
    """

    def __init__(
        self,
        max_executions: int = 150,
        seed: Optional[int] = 42,
        exploration_constant: float = 1.414,
        conflict_bonus: float = 2.0,
    ):
        super().__init__(name="PHANTOM_FeedbackGuidedSearch", max_executions=max_executions, seed=seed)
        self.c = exploration_constant
        self.conflict_bonus = conflict_bonus

    def minimize(
        self,
        original_trace: Trace,
        oracle: FailureOracle,
        scheduler: EventScheduler,
    ) -> MinimizerResult:
        rng = random.Random(self.seed)
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

        # 2. Extract Causal Cone (Phase 1 Causal Slicing)
        if fail_id:
            cone = causal_graph.causal_cone(fail_id)
        else:
            cone = set(all_ids)
        active_ids = list(cone)

        # Fast verify causal slice
        cand_trace = original_trace.filter(set(active_ids))
        res = scheduler.replay(cand_trace, oracle)
        executions += 1
        history.append({
            "step": executions,
            "phase": "causal_slicing",
            "size": len(active_ids),
            "verdict": res.verdict.value,
        })
        if not res.is_reproduced:
            active_ids = list(all_ids)

        best_reproducing_trace = original_trace.filter(set(active_ids))
        best_size = len(best_reproducing_trace.events)

        # 3. Identify Concurrent Event Pairs and Potential Conflict Races
        conflicts = causal_graph.find_conflicting_pairs()
        conflict_pairs: Set[Tuple[str, str]] = set()
        for e1, e2 in conflicts:
            conflict_pairs.add((e1.event_id, e2.event_id))
            conflict_pairs.add((e2.event_id, e1.event_id))

        # 4. Initialize Multi-Armed Bandit / Feedback Model for Ordering Preferences
        # Pairwise counts and success frequencies for orderings (u, v) where u precedes v
        order_counts: Dict[Tuple[str, str], int] = {}
        order_successes: Dict[Tuple[str, str], int] = {}
        total_evaluations = 0

        # Subgraph for active events
        active_subgraph = causal_graph.graph.subgraph(active_ids).copy()

        def compute_pairwise_score(u: str, v: str) -> float:
            pair = (u, v)
            n_uv = order_counts.get(pair, 0)
            s_uv = order_successes.get(pair, 0)

            # Prior expectation + UCB exploration term
            mean_reward = (s_uv + 0.1) / (n_uv + 0.2)
            exploration = self.c * math.sqrt(math.log(total_evaluations + 2) / (n_uv + 1))
            bonus = self.conflict_bonus if pair in conflict_pairs else 0.0

            return mean_reward + exploration + bonus

        # 5. Feedback-Guided Search Loop over Interleavings and Subsets
        # Reserve execution budget for the final causal necessity pruning pass
        search_budget = max(5, self.max_executions - len(active_ids) - 2)
        while executions < search_budget:
            total_evaluations += 1

            # Candidate schedule generation using Kahn's algorithm guided by feedback scores
            in_degree = {n: active_subgraph.in_degree(n) for n in active_subgraph.nodes}
            ready_queue = [n for n, deg in in_degree.items() if deg == 0]
            candidate_order: List[str] = []

            # Track in-flight graph edges for topological validity
            temp_graph = active_subgraph.copy()

            while ready_queue:
                if len(ready_queue) == 1:
                    chosen = ready_queue.pop(0)
                else:
                    # Score each ready candidate against other ready candidates
                    scores = []
                    for cand in ready_queue:
                        cand_score = sum(compute_pairwise_score(cand, other) for other in ready_queue if other != cand)
                        scores.append(cand_score)

                    # Softmax / Epsilon-Greedy selection for feedback exploration
                    if rng.random() < 0.15:
                        # Exploration jitter
                        idx = rng.randrange(len(ready_queue))
                    else:
                        # Exploit highest UCB score
                        max_score = max(scores)
                        best_indices = [i for i, s in enumerate(scores) if s == max_score]
                        idx = rng.choice(best_indices)

                    chosen = ready_queue.pop(idx)

                candidate_order.append(chosen)

                # Remove chosen node and update in-degrees of successors
                for succ in list(temp_graph.successors(chosen)):
                    temp_graph.remove_edge(chosen, succ)
                    if temp_graph.in_degree(succ) == 0 and succ not in candidate_order and succ not in ready_queue:
                        ready_queue.append(succ)

            # Construct candidate trace with this interleaving
            cand_event_dict = {e.event_id: e for e in original_trace.events}
            cand_events = [cand_event_dict[eid] for eid in candidate_order if eid in cand_event_dict]
            
            # Optionally test dropping a non-critical event to combine search with reduction
            if len(candidate_events := cand_events) > 3 and rng.random() < 0.4:
                non_crit = [e for e in candidate_events if e.event_id != fail_id and e.event_id not in causal_graph.critical_path_to_failure()]
                if non_crit:
                    drop_event = rng.choice(non_crit)
                    candidate_events = [e for e in candidate_events if e.event_id != drop_event.event_id]

            candidate_trace = Trace(
                events=candidate_events,
                failure_event_id=fail_id,
                failure_signature=original_trace.failure_signature,
                metadata=dict(original_trace.metadata),
            )

            res = scheduler.replay(candidate_trace, oracle)
            executions += 1
            curr_size = len(candidate_trace.events)

            history.append({
                "step": executions,
                "phase": "feedback_search",
                "size": curr_size,
                "verdict": res.verdict.value,
            })

            # Update feedback model for observed relative orderings
            order_list = [e.event_id for e in candidate_trace.events]
            for i in range(len(order_list)):
                for j in range(i + 1, min(i + 5, len(order_list))):
                    pair = (order_list[i], order_list[j])
                    order_counts[pair] = order_counts.get(pair, 0) + 1
                    if res.is_reproduced:
                        order_successes[pair] = order_successes.get(pair, 0) + 1

            if res.is_reproduced:
                if curr_size < best_size:
                    best_size = curr_size
                    best_reproducing_trace = candidate_trace
                    # Update active subgraph to smaller reproducing set
                    active_ids = [e.event_id for e in candidate_trace.events]
                    active_subgraph = causal_graph.graph.subgraph(active_ids).copy()

            # Early break if optimal ground truth size reached or convergence achieved
            if best_size <= 3 and executions >= 20:
                break

        # 6. Final Pruning Pass on Best Discovered Schedule
        final_trace = best_reproducing_trace
        remaining_ids = [e.event_id for e in final_trace.events if e.event_id != fail_id]
        necessary: List[str] = []
        irrelevant: List[str] = []

        for eid in remaining_ids:
            if executions >= self.max_executions:
                break
            without_e = [e for e in final_trace.events if e.event_id != eid]
            test_tr = Trace(
                events=without_e,
                failure_event_id=fail_id,
                failure_signature=original_trace.failure_signature,
                metadata=dict(original_trace.metadata),
            )
            res = scheduler.replay(test_tr, oracle)
            executions += 1
            if res.is_reproduced:
                final_trace = test_tr
                irrelevant.append(eid)
            else:
                necessary.append(eid)

        if fail_id and fail_id not in necessary:
            necessary.append(fail_id)

        elapsed = time.perf_counter() - start_time
        stability = self.verify_stability(final_trace, oracle, scheduler, repeat_count=5)
        min_size = len(final_trace.events)
        reduction = 1.0 - (min_size / total_events) if total_events > 0 else 0.0
        confidence = 1.0 - ((1.0 - max(0.01, stability)) ** 5) if stability > 0 else 0.0

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
            minimized_trace=final_trace,
            retained_event_ids=[e.event_id for e in final_trace.events],
            necessary_events=necessary,
            irrelevant_events=irrelevant,
            confidence_score=confidence,
            search_history=history,
            initial_state=dict(scheduler.initial_state),
        )
