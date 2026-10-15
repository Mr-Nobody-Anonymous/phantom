import pytest
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.algorithms.delta_debugging import DeltaDebuggingMinimizer
from phantom.algorithms.random_search import RandomSearchMinimizer
from phantom.benchmarks.benchmark_a import create_duplicate_order_case

def test_causal_minimizer_on_duplicate_order():
    case = create_duplicate_order_case()
    minimizer = PhantomCausalMinimizer()
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is True
    assert res.failure_preserved is True
    assert res.reduction_ratio > 0.40  # Significant noise pruned
    assert res.reproduction_rate >= 0.80
    assert len(res.necessary_events) > 0
    assert "evt_assert_dup" in res.retained_event_ids
    # Background events should be pruned
    assert "bg_1" not in res.retained_event_ids
    assert "bg_4" not in res.retained_event_ids

def test_delta_debugging_on_duplicate_order():
    case = create_duplicate_order_case()
    minimizer = DeltaDebuggingMinimizer()
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is True
    assert res.failure_preserved is True
    assert res.minimized_event_count <= res.original_event_count

def test_random_search_on_duplicate_order():
    case = create_duplicate_order_case()
    minimizer = RandomSearchMinimizer(max_executions=50)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    assert res.reproduced is True
    assert res.failure_preserved is True

def test_causal_minimizer_efficiency_advantage():
    case = create_duplicate_order_case()
    c_res = PhantomCausalMinimizer().minimize(case.trace, case.oracle, case.scheduler)
    dd_res = DeltaDebuggingMinimizer().minimize(case.trace, case.oracle, case.scheduler)

    # PHANTOM uses backward causal cone to eliminate non-causal events without trial-and-error
    assert c_res.minimized_event_count <= dd_res.minimized_event_count
