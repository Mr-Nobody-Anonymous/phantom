import json
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

def test_cli_import_otel(tmp_path):
    otel_file = tmp_path / "otel_sample.json"
    otel_file.write_text(json.dumps({
        "resourceSpans": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "order-svc"}}]},
            "scopeSpans": [{"spans": [{"spanId": "1001", "name": "order_span", "status": {"code": 2, "message": "OrderFailure"}}]}]
        }]
    }), encoding="utf-8")

    out_trace = tmp_path / "imported.json"
    runner = CliRunner()
    result = runner.invoke(main, ["import-otel", str(otel_file), "-o", str(out_trace)])
    assert result.exit_code == 0
    assert out_trace.exists()
    assert "Successfully converted" in result.output

def test_cli_benchmark_command(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, ["benchmark", "-o", str(tmp_path), "-g", "a", "-a", "phantom", "--no-plot"])
    assert result.exit_code == 0
    assert (tmp_path / "benchmark_results.json").exists()
    assert (tmp_path / "benchmark_summary.csv").exists()
