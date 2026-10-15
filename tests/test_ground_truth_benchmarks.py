"""
Tests for Independent Ground Truth Distributed System Benchmark Scenarios.
Evaluates PHANTOM against known distributed system bug patterns:
- Raft Split-Brain Stale Leader Commit
- Two-Phase Commit Coordinator Crash Recovery
- Asynchronous Replication Failover Lost Write
- Message Queue Consumer Rebalance Duplicate Processing
- Snapshot Isolation Write Skew
"""

import pytest
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.benchmarks.benchmark_ground_truth import get_ground_truth_cases


@pytest.mark.parametrize("case", get_ground_truth_cases(), ids=lambda c: c.case_id)
def test_ground_truth_case_initial_reproduction(case):
    """Initial execution of ground truth case must violate expected invariant."""
    initial = case.scheduler.replay(case.trace, case.oracle)
    assert initial.is_reproduced is True
    assert initial.failure_signature == case.trace.failure_signature


@pytest.mark.parametrize("case", get_ground_truth_cases(), ids=lambda c: c.case_id)
def test_ground_truth_causal_minimization(case):
    """PHANTOM must minimize each ground truth trace while deterministically preserving failure."""
    minimizer = PhantomCausalMinimizer(max_executions=150)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is True
    assert res.failure_preserved is True
    assert res.minimized_event_count <= res.original_event_count
    assert res.reproduction_rate >= 0.80

    # Ground truth essential events must be preserved in minimized trace
    for essential_eid in case.ground_truth_minimal_events:
        assert essential_eid in res.retained_event_ids
