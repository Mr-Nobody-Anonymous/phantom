import pytest
from phantom.algorithms.feedback_search import FeedbackGuidedSearchMinimizer
from phantom.benchmarks.benchmark_a import create_duplicate_order_case, create_double_spend_case
from phantom.benchmarks.benchmark_b import create_split_brain_partition_case
from phantom.core.oracle import OracleVerdict

def test_feedback_guided_search_on_duplicate_order():
    case = create_duplicate_order_case()
    minimizer = FeedbackGuidedSearchMinimizer(max_executions=50, seed=42)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is True
    assert res.failure_preserved is True
    assert res.minimized_event_count < res.original_event_count
    assert res.reduction_ratio > 0.5
    assert res.reproduction_rate == 1.0
    assert len(res.search_history) > 1

def test_feedback_guided_search_on_split_brain():
    case = create_split_brain_partition_case()
    minimizer = FeedbackGuidedSearchMinimizer(max_executions=50, seed=42)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is True
    assert res.failure_preserved is True
    assert res.reproduction_rate == 1.0

def test_feedback_search_stops_on_clean_trace():
    case = create_duplicate_order_case()
    # Modify oracle so target failure never triggers
    case.oracle.expected_signature = "NonExistentSignature"
    minimizer = FeedbackGuidedSearchMinimizer(max_executions=20)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is False
    assert res.executions_count == 1
