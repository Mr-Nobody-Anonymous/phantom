"""
Command-line interface (CLI) for Project PHANTOM.
Supports benchmark suites, randomized multi-seed trials, ablation studies,
scale tests, trace minimization, OpenTelemetry ingestion, bundle export, and sandboxed verification.
"""

from __future__ import annotations
import json
from pathlib import Path
import subprocess
import sys
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from phantom.algorithms.base import AblationConfig, MinimalityLevel, SearchBudget
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.algorithms.feedback_search import FeedbackGuidedSearchMinimizer
from phantom.algorithms.delta_debugging import DeltaDebuggingMinimizer
from phantom.algorithms.random_search import RandomSearchMinimizer
from phantom.api import analyze_trace, export_reproducer_bundle, minimize_trace, verify_reproducer_script
from phantom.benchmarks.benchmark_a import get_benchmark_a_cases
from phantom.benchmarks.benchmark_b import get_benchmark_b_cases
from phantom.benchmarks.benchmark_c import get_benchmark_c_cases
from phantom.benchmarks.benchmark_ground_truth import get_ground_truth_cases
from phantom.benchmarks.runner import BenchmarkRunner
from phantom.core.graph import CausalGraph, CausalInconsistencyError
from phantom.core.oracle import FailureOracle
from phantom.core.otel import OTelTraceImporter
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace
from phantom.report.reporter import IncidentReporter
from phantom.reproduction.generator import ReproducerGenerator
from phantom.reproduction.sandbox import ReproducerSandbox


console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="phantom")
def main():
    """Project PHANTOM: Automated Failure Reconstruction and Causal Minimization Engine."""
    pass


@main.command("benchmark")
@click.option("--output-dir", "-o", default="benchmark_artifacts", help="Directory for benchmark artifacts.")
@click.option("--group", "-g", type=click.Choice(["a", "b", "c", "gt", "all"]), default="all", help="Benchmark group to run.")
@click.option("--algo", "-a", type=click.Choice(["phantom", "feedback", "ddmin", "random", "all"]), default="all", help="Algorithm to evaluate.")
@click.option("--plot/--no-plot", default=True, help="Generate comparison charts.")
@click.option("--randomized-trials", is_flag=True, help="Execute multi-seed randomized trials and publish variance.")
@click.option("--ablation", is_flag=True, help="Execute ablation study across subsystem configurations.")
@click.option("--scale", is_flag=True, help="Execute trace scale benchmarks (50 to 200+ events).")
def benchmark_cmd(output_dir: str, group: str, algo: str, plot: bool, randomized_trials: bool, ablation: bool, scale: bool):
    """Run the empirical comparative evaluation suite."""
    runner = BenchmarkRunner(output_dir=output_dir)

    cases = []
    if group in ["a", "all"]:
        cases.extend(get_benchmark_a_cases())
    if group in ["b", "all"]:
        cases.extend(get_benchmark_b_cases())
    if group in ["c", "all"]:
        cases.extend(get_benchmark_c_cases())
    if group in ["gt", "all"]:
        cases.extend(get_ground_truth_cases())

    if randomized_trials:
        runner.run_randomized_trials(cases=cases[:5])
        return

    if ablation:
        runner.run_ablation_study(cases=cases[:5])
        return

    if scale:
        runner.run_scale_benchmark()
        return

    algorithms = None
    if algo == "phantom":
        algorithms = [PhantomCausalMinimizer(max_executions=150)]
    elif algo == "feedback":
        algorithms = [FeedbackGuidedSearchMinimizer(max_executions=150)]
    elif algo == "ddmin":
        algorithms = [DeltaDebuggingMinimizer(max_executions=150)]
    elif algo == "random":
        algorithms = [RandomSearchMinimizer(max_executions=150)]

    records = runner.run_suite(cases=cases, algorithms=algorithms, verbose=True)

    if plot:
        chart_file = runner.generate_plots(records)
        console.print(f"[bold green][+][/bold green] Comparison chart generated at: [cyan]{chart_file}[/cyan]")

    console.print(f"[bold green][+][/bold green] Raw metrics saved to: [cyan]{output_dir}/benchmark_results.json[/cyan]")
    console.print(f"[bold green][+][/bold green] CSV summary saved to: [cyan]{output_dir}/benchmark_summary.csv[/cyan]")


@main.command("minimize")
@click.argument("trace_path", type=click.Path(exists=True))
@click.option("--algo", "-a", type=click.Choice(["phantom", "feedback", "ddmin", "random"]), default="phantom", help="Minimization algorithm.")
@click.option("--output", "-o", default=None, help="File to save minimized report/trace.")
@click.option("--bundle", "-b", default=None, help="Directory to export complete standalone reproducer bundle.")
@click.option("--time-budget", type=float, default=None, help="Maximum execution time budget in seconds.")
@click.option("--max-executions", type=int, default=200, help="Maximum search execution limit.")
@click.option("--checkpoint", default=None, help="Path for search checkpoint file.")
@click.option("--resume", is_flag=True, help="Resume search from checkpoint file if present.")
@click.option("--strict/--permissive", default=False, help="Strict causal DAG validation mode.")
@click.option("--json", "json_output", is_flag=True, help="Output results in machine-readable JSON format.")
def minimize_cmd(
    trace_path: str,
    algo: str,
    output: Optional[str],
    bundle: Optional[str],
    time_budget: Optional[float],
    max_executions: int,
    checkpoint: Optional[str],
    resume: bool,
    strict: bool,
    json_output: bool,
):
    """Minimize a distributed execution trace to its minimal causal failure schedule."""
    trace = Trace.load(trace_path)
    sig = trace.failure_signature or "DistributedInvariantFailure"
    oracle = FailureOracle(expected_signature=sig)
    scheduler = EventScheduler()

    try:
        result = minimize_trace(
            trace=trace,
            oracle=oracle,
            scheduler=scheduler,
            algorithm=algo,
            max_executions=max_executions,
            time_budget_seconds=time_budget,
            checkpoint_file=checkpoint,
            resume=resume,
            strict_graph=strict,
        )
    except CausalInconsistencyError as e:
        console.print(f"[bold red]Causal Graph Error:[/bold red] {str(e)}")
        sys.exit(1)

    if json_output:
        summary = result.summary()
        summary["retained_events"] = [e.to_dict() for e in result.minimized_trace.events]
        click.echo(json.dumps(summary, indent=2))
    else:
        IncidentReporter.print_terminal_report(result)

    if output:
        IncidentReporter.save_report(result, output)
        console.print(f"[bold green][+][/bold green] Report saved to: [cyan]{output}[/cyan]")

    if bundle:
        paths = export_reproducer_bundle(result, bundle)
        console.print(f"[bold green][+][/bold green] Complete reproducer bundle exported to: [cyan]{bundle}[/cyan]")
        for k, p in paths.items():
            console.print(f"  - {k}: {p.name}")


@main.command("import-otel")
@click.argument("otel_path", type=click.Path(exists=True))
@click.option("--output", "-o", default="imported_trace.json", help="Path to save imported PHANTOM trace JSON.")
def import_otel_cmd(otel_path: str, output: str):
    """Import an OpenTelemetry or Jaeger trace JSON into a PHANTOM Trace."""
    console.print(f"[bold]Importing OpenTelemetry spans from [cyan]{otel_path}[/cyan]...[/bold]")
    trace = OTelTraceImporter.from_otel_json(otel_path)
    trace.save(output)
    console.print(f"[bold green][+][/bold green] Successfully converted [yellow]{len(trace.events)} events[/yellow] into [cyan]{output}[/cyan]")
    if trace.failure_event_id:
        console.print(f"  Detected failure event: [red]{trace.failure_event_id}[/red] ({trace.failure_signature})")
    else:
        console.print(f"  [yellow]Notice:[/yellow] No explicit span exception or error status detected in input.")


@main.command("info")
@click.argument("trace_path", type=click.Path(exists=True))
@click.option("--strict/--permissive", default=False, help="Strict causal DAG validation.")
@click.option("--json", "json_output", is_flag=True, help="Output analysis as structured JSON.")
def info_cmd(trace_path: str, strict: bool, json_output: bool):
    """Inspect and analyze a distributed execution trace file."""
    trace = Trace.load(trace_path)
    data = analyze_trace(trace, strict_mode=strict)

    if json_output:
        click.echo(json.dumps(data, indent=2))
        return

    table = Table(title=f"Trace Inspection: {Path(trace_path).name}")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Events", str(data["event_count"]))
    table.add_row("Participating Nodes", ", ".join(data["participating_nodes"]))
    table.add_row("Injected Faults", str(data["fault_event_count"]))
    table.add_row("Failure Signature", data["failure_signature"] or "None")
    table.add_row("DAG Nodes / Edges", f"{data['num_nodes']} nodes, {data['num_edges']} edges")
    table.add_row("Is Valid DAG", str(data["is_dag"]))
    table.add_row("Graph Density", f"{data['density']:.4f}")
    table.add_row("Is Uncertain", str(data["is_uncertain"]))
    table.add_row("Trace Hash", data["trace_hash"][:12] if data["trace_hash"] else "N/A")

    console.print(table)

    if data["is_uncertain"]:
        console.print(Panel(
            "\n".join(f"• {r}" for r in data["uncertainty_reasons"]),
            title="[bold yellow]Causal Graph Uncertainty Notices[/bold yellow]",
            border_style="yellow",
        ))

    conflicts = data["conflicting_pairs"]
    if conflicts:
        console.print(f"\n[bold yellow]Identified {len(conflicts)} Concurrent State Conflict Pair(s) (Race Candidates):[/bold yellow]")
        for c in conflicts[:5]:
            console.print(f"  • {c['event_1']} ({c['node_1']}) <--> {c['event_2']} ({c['node_2']}) on key '{c['key']}'")


@main.command("demo")
@click.option("--output-dir", "-o", default="demo_output", help="Directory for demo artifacts.")
def demo_cmd(output_dir: str):
    """Run an instant demonstration of PHANTOM causal minimization on duplicate order processing."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    cases = get_benchmark_a_cases()
    demo_case = cases[0]

    console.print(Panel(
        f"[bold cyan]PROJECT PHANTOM LIVE DEMONSTRATION[/bold cyan]\n\n"
        f"Target Failure: [bold red]{demo_case.name}[/bold red]\n"
        f"Description: {demo_case.description}\n"
        f"Input Trace Size: [bold yellow]{len(demo_case.trace.events)} events[/bold yellow] (includes noise & telemetry)",
        border_style="cyan"
    ))

    # Run PHANTOM
    minimizer = PhantomCausalMinimizer()
    result = minimizer.minimize(demo_case.trace, demo_case.oracle, demo_case.scheduler)

    # Print report
    IncidentReporter.print_terminal_report(result)

    # Generate standalone reproducer bundle
    bundle_paths = ReproducerGenerator.generate_bundle(result, out)
    console.print(f"[bold green][+] Standalone Reproducer Bundle generated at:[/bold green] [cyan]{out}[/cyan]")

    # Run the reproducer
    console.print("\n[bold]Verifying generated reproduction script by direct execution...[/bold]")
    res = subprocess.run([sys.executable, str(bundle_paths["script"])], capture_output=True, text=True)
    console.print(res.stdout, markup=False)
    if res.returncode == 0:
        console.print("[bold green][+] Reproducer verified successfully! Deterministic failure achieved.[/bold green]")
    else:
        console.print(f"[bold red]X Verification failed (code {res.returncode}):[/bold red]\n{res.stderr}")


@main.command("verify")
@click.argument("script_path", type=click.Path(exists=True))
@click.option("--sandbox/--no-sandbox", default=True, help="Run in credential-scrubbed sandbox environment.")
@click.option("--timeout", type=float, default=30.0, help="Maximum execution timeout in seconds.")
def verify_cmd(script_path: str, sandbox: bool, timeout: float):
    """Execute and verify a standalone reproduction script."""
    console.print(f"[bold]Executing reproducer: [cyan]{script_path}[/cyan] (sandbox={sandbox})...[/bold]")
    if sandbox:
        success, output, code = ReproducerSandbox.run_isolated(script_path, timeout=timeout)
    else:
        success, output = verify_reproducer_script(script_path, timeout=timeout)
        code = 0 if success else 1

    console.print(output, markup=False)
    if success:
        console.print("[bold green][+] Verification PASSED: Target invariant violated as expected.[/bold green]")
    else:
        console.print(f"[bold red]X Verification FAILED (exit code {code})[/bold red]")
        sys.exit(code if code != 0 else 1)


if __name__ == "__main__":
    main()
