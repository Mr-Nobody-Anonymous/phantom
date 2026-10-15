"""
Tests for ReproducerGenerator bundle export and ReproducerSandbox execution:
- Standalone bundle generation (reproduce.py, test_reproduce.py, Dockerfile, docker-compose.yml, README.md)
- Subprocess execution and pytest verification of generated bundle
- Sandbox credential scrubbing and timeout enforcement
"""

import subprocess
import sys
from pathlib import Path
import pytest

from phantom.algorithms.base import MinimalityLevel, MinimizerResult
from phantom.core.event import Event, EventType
from phantom.core.trace import Trace
from phantom.reproduction.generator import ReproducerGenerator
from phantom.reproduction.sandbox import ReproducerSandbox


def test_reproducer_bundle_generation(tmp_path: Path):
    """Bundle generation produces all required files for independent reproduction."""
    events = [
        Event("w1", EventType.WRITE, "node1", {"key": "acc", "val": 100}, logical_time=1),
        Event("crash", EventType.FAULT_CRASH, "node1", {"target_node": "node1"}, logical_time=2),
        Event("restart", EventType.FAULT_RESTART, "node1", {"target_node": "node1"}, logical_time=3),
        Event("assert_loss", EventType.ASSERTION, "node1", {
            "name": "BalanceLoss",
            "key": "acc",
            "expected": 100,
            "signature": "DurabilityLossOnCrash",
        }, logical_time=4),
    ]
    trace = Trace(events=events, failure_event_id="assert_loss", failure_signature="DurabilityLossOnCrash")
    dummy_res = MinimizerResult(
        algorithm_name="PHANTOM_CausalMinimizer",
        original_event_count=4,
        minimized_event_count=4,
        reduction_ratio=0.0,
        executions_count=1,
        reproduction_rate=1.0,
        reproduced=True,
        failure_preserved=True,
        wall_clock_time=0.05,
        minimized_trace=trace,
        minimality_level=MinimalityLevel.ONE_EVENT_MINIMAL,
        necessary_events=["w1", "crash", "assert_loss"],
    )

    bundle_paths = ReproducerGenerator.generate_bundle(dummy_res, tmp_path)

    # Verify all expected files are present
    assert bundle_paths["script"].exists()
    assert bundle_paths["pytest"].exists()
    assert bundle_paths["dockerfile"].exists()
    assert bundle_paths["docker_compose"].exists()
    assert bundle_paths["requirements"].exists()
    assert bundle_paths["schedule_json"].exists()
    assert bundle_paths["readme"].exists()

    # Execute generated reproduce.py
    run_res = subprocess.run([sys.executable, str(bundle_paths["script"])], capture_output=True, text=True)
    assert run_res.returncode == 0
    assert "RESULT: SUCCESSFUL DETERMINISTIC REPRODUCTION!" in run_res.stdout


def test_sandbox_credential_scrubbing_and_execution(tmp_path: Path):
    """Sandbox strips sensitive cloud/API credentials from environment variables."""
    test_script = tmp_path / "inspect_env.py"
    test_script.write_text(
        """
import os, sys
# Check sensitive environment variables
assert "AWS_SECRET_ACCESS_KEY" not in os.environ
assert "DATABASE_URL" not in os.environ
assert "GITHUB_TOKEN" not in os.environ
print("Sandbox environment clean!")
sys.exit(0)
""",
        encoding="utf-8",
    )

    # Inject mock credentials into parent environment
    import os
    os.environ["AWS_SECRET_ACCESS_KEY"] = "mock_super_secret"
    os.environ["DATABASE_URL"] = "postgres://root:pass@localhost/db"
    os.environ["GITHUB_TOKEN"] = "ghp_mock_token"

    success, output, code = ReproducerSandbox.run_isolated(test_script, timeout=10.0)
    assert success is True
    assert code == 0
    assert "Sandbox environment clean!" in output


def test_sandbox_timeout_enforcement(tmp_path: Path):
    """Sandbox terminates long-running or deadlocked scripts upon reaching timeout."""
    infinite_script = tmp_path / "hang.py"
    infinite_script.write_text("import time\ntime.sleep(10)\n", encoding="utf-8")

    success, output, code = ReproducerSandbox.run_isolated(infinite_script, timeout=0.5)
    assert success is False
    assert "timed out" in output.lower()
