from click.testing import CliRunner
from phantom.cli import main
from phantom.benchmarks.benchmark_a import create_duplicate_order_case

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Project PHANTOM" in result.output

def test_cli_info(tmp_path):
    case = create_duplicate_order_case()
    trace_path = tmp_path / "test_trace.json"
    case.trace.save(trace_path)

    runner = CliRunner()
    result = runner.invoke(main, ["info", str(trace_path)])
    assert result.exit_code == 0
    assert "Trace Inspection" in result.output
    assert "DuplicateOrderProcessing" in result.output

def test_cli_demo(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, ["demo", "--output-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "PROJECT PHANTOM LIVE DEMONSTRATION" in result.output
    assert "Reproducer verified successfully" in result.output
