import subprocess
import sys
from pathlib import Path
import pytest
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.benchmarks.benchmark_a import create_duplicate_order_case
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
