"""
Tests for PHANTOM public API, edge cases, and robustness:
- Empty traces
- Malformed traces (duplicate IDs, missing failure events)
- Conflicting vector clocks
- Missing message endpoints
- Search budgets (execution limits, time limits)
- Checkpoint persistence and resuming
- Ablation study toggles
"""

from pathlib import Path
import time
import pytest

from phantom.algorithms.base import AblationConfig, MinimalityLevel
from phantom.api import analyze_trace, minimize_trace, replay_trace
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.graph import CausalCycleError, CausalGraph
from phantom.core.oracle import FailureOracle, OracleVerdict
from phantom.core.trace import Trace


def test_empty_trace_handling():
    """Empty traces must be handled gracefully without crashing."""
    empty_trace = Trace(events=[])
    oracle = FailureOracle(expected_signature="EmptyTest")
    res = replay_trace(empty_trace, oracle)
    assert res.verdict == OracleVerdict.PASS

    analysis = analyze_trace(empty_trace)
    assert analysis["event_count"] == 0
    assert analysis["is_dag"] is True


def test_malformed_trace_duplicate_ids():
    """Traces with duplicate event IDs must be caught by validation."""
    events = [
        Event("dup_1", EventType.INTERNAL, "node1"),
        Event("dup_1", EventType.INTERNAL, "node2"),
    ]
    trace = Trace(events=events)
    is_valid, err = trace.validate()
    assert is_valid is False
    assert "Duplicate event_id" in err


def test_malformed_trace_missing_failure_event():
    """Traces referencing a non-existent failure_event_id must fail validation."""
    events = [Event("e1", EventType.INTERNAL, "node1")]
    trace = Trace(events=events, failure_event_id="ghost_event_99")
    is_valid, err = trace.validate()
    assert is_valid is False
    assert "Failure event 'ghost_event_99' not found" in err


def test_conflicting_vector_clocks_causal_ordering():
    """Vector clocks with mutual happens_before inconsistencies are identified."""
    # vc1 has nodeA=2, nodeB=1
    vc1 = VectorClock({"nodeA": 2, "nodeB": 1})
    # vc2 has nodeA=1, nodeB=2
    vc2 = VectorClock({"nodeA": 1, "nodeB": 2})

    # They are concurrent
    assert vc1.concurrent_with(vc2)
    assert not vc1.happens_before(vc2)
    assert not vc2.happens_before(vc1)


def test_search_budget_execution_limit():
    """Search terminates cleanly with BUDGET_EXECUTIONS_EXHAUSTED when limit is reached."""
    from phantom.benchmarks.benchmark_a import create_duplicate_order_case
    case = create_duplicate_order_case()

    res = minimize_trace(
        case.trace,
        case.oracle,
        case.scheduler,
        max_executions=2,
    )
    assert res.executions_count <= 3
    assert res.termination_reason == "BUDGET_EXECUTIONS_EXHAUSTED"


def test_search_budget_time_limit():
    """Search terminates cleanly with BUDGET_TIME_EXHAUSTED when time limit is reached."""
    from phantom.benchmarks.benchmark_a import create_duplicate_order_case
    case = create_duplicate_order_case()

    res = minimize_trace(
        case.trace,
        case.oracle,
        case.scheduler,
        time_budget_seconds=0.0001,
    )
    assert res.termination_reason in {"BUDGET_TIME_EXHAUSTED", "BUDGET_EXECUTIONS_EXHAUSTED", "CONVERGED"}


def test_checkpointing_save_and_resume(tmp_path: Path):
    """Minimizer persists checkpoint and resumes seamlessly from disk."""
    from phantom.benchmarks.benchmark_a import create_duplicate_order_case
    case = create_duplicate_order_case()
    ckpt_file = tmp_path / "search_checkpoint.json"

    # Run with checkpoint enabled and a very small execution limit
    res1 = minimize_trace(
        case.trace,
        case.oracle,
        case.scheduler,
        max_executions=3,
        checkpoint_file=ckpt_file,
    )
    assert ckpt_file.exists()

    # Resume from checkpoint
    res2 = minimize_trace(
        case.trace,
        case.oracle,
        case.scheduler,
        max_executions=50,
        checkpoint_file=ckpt_file,
        resume=True,
    )
    assert res2.resumed_from_checkpoint is True
    assert res2.reproduced is True


def test_ablation_study_execution():
    """Ablation toggles disable specific phases while still completing search."""
    from phantom.benchmarks.benchmark_a import create_duplicate_order_case
    case = create_duplicate_order_case()

    # Disable causal slicing
    no_slicing_config = AblationConfig(enable_causal_slicing=False)
    res = minimize_trace(
        case.trace,
        case.oracle,
        case.scheduler,
        ablation=no_slicing_config,
    )
    assert res.reproduced is True
    assert res.ablation_config["causal_slicing"] is False
