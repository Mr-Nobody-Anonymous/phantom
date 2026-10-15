import pytest
from phantom.benchmarks.benchmark_a import get_benchmark_a_cases
from phantom.benchmarks.benchmark_b import get_benchmark_b_cases
from phantom.benchmarks.benchmark_c import get_benchmark_c_cases
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer

@pytest.mark.parametrize("case", get_benchmark_a_cases(), ids=lambda c: c.case_id)
def test_benchmark_a_cases_reproduce(case):
    # Initial replay must exhibit failure
    initial = case.scheduler.replay(case.trace, case.oracle)
    assert initial.is_reproduced

    # PHANTOM minimization must succeed
    minimizer = PhantomCausalMinimizer(max_executions=100)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)
    assert res.reproduced
    assert res.failure_preserved
    assert res.minimized_event_count <= res.original_event_count

@pytest.mark.parametrize("case", get_benchmark_b_cases(), ids=lambda c: c.case_id)
def test_benchmark_b_cases_reproduce(case):
    initial = case.scheduler.replay(case.trace, case.oracle)
    assert initial.is_reproduced

    minimizer = PhantomCausalMinimizer(max_executions=100)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)
    assert res.reproduced
    assert res.failure_preserved

@pytest.mark.parametrize("case", get_benchmark_c_cases(), ids=lambda c: c.case_id)
def test_benchmark_c_cases_reproduce(case):
    initial = case.scheduler.replay(case.trace, case.oracle)
    assert initial.is_reproduced

    minimizer = PhantomCausalMinimizer(max_executions=100)
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)
    assert res.reproduced
    assert res.failure_preserved
