import subprocess
import sys
from pathlib import Path
import pytest
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.benchmarks.benchmark_a import create_duplicate_order_case
from phantom.benchmarks.benchmark_b import create_delayed_retry_storm_case
from phantom.core.event import Event, EventType
from phantom.core.trace import Trace
from phantom.algorithms.base import MinimizerResult
from phantom.reproduction.generator import ReproducerGenerator

def test_reproducer_generator_and_execution(tmp_path: Path):
    case = create_duplicate_order_case()
    minimizer = PhantomCausalMinimizer()
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    script_path = tmp_path / "reproduce_test.py"
    ReproducerGenerator.generate_script(res, script_path)

    assert script_path.exists()
    content = script_path.read_text(encoding="utf-8")
    assert "PROJECT PHANTOM: REPRODUCING MINIMAL FAILURE SCHEDULE" in content

    # Run the script via python subprocess
    sub_res = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert sub_res.returncode == 0
    assert "RESULT: SUCCESSFUL DETERMINISTIC REPRODUCTION!" in sub_res.stdout

def test_reproducer_generator_with_message_passing(tmp_path: Path):
    case = create_delayed_retry_storm_case()
    minimizer = PhantomCausalMinimizer()
    res = minimizer.minimize(case.trace, case.oracle, case.scheduler)

    script_path = tmp_path / "reproduce_retry_storm.py"
    ReproducerGenerator.generate_script(res, script_path)

    assert script_path.exists()
    content = script_path.read_text(encoding="utf-8")
    assert "SEND" in content or "RECEIVE" in content or "WRITE" in content

    sub_res = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert sub_res.returncode == 0
    assert "RESULT: SUCCESSFUL DETERMINISTIC REPRODUCTION!" in sub_res.stdout

def test_reproducer_generator_detects_clean_passing_state(tmp_path: Path):
    # Construct a trace that does NOT trigger the invariant violation
    clean_events = [
        Event("w1", EventType.WRITE, "node1", {"key": "status", "val": "clean"}, logical_time=1),
        Event("assert_clean", EventType.ASSERTION, "node1", {"name": "NoBug", "key": "status", "expected": "clean", "signature": "SpuriousBug"}, logical_time=2),
    ]
    clean_trace = Trace(events=clean_events, failure_event_id="assert_clean", failure_signature="SpuriousBug")
    dummy_res = MinimizerResult(
        algorithm_name="test",
        original_event_count=2,
        minimized_event_count=2,
        reduction_ratio=0.0,
        executions_count=1,
        reproduction_rate=1.0,
        reproduced=True,
        failure_preserved=True,
        wall_clock_time=0.01,
        minimized_trace=clean_trace,
    )

    script_path = tmp_path / "reproduce_clean.py"
    ReproducerGenerator.generate_script(dummy_res, script_path)

    sub_res = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    # Since the state was clean and assertion was satisfied, it must exit 1 (FAILED TO REPRODUCE)
    assert sub_res.returncode == 1
    assert "RESULT: FAILED TO REPRODUCE (Invariant held)" in sub_res.stdout
