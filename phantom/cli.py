"""
Command-line interface (CLI) for Project PHANTOM.
"""

from __future__ import annotations
from pathlib import Path
import subprocess
import sys
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.algorithms.feedback_search import FeedbackGuidedSearchMinimizer
from phantom.algorithms.delta_debugging import DeltaDebuggingMinimizer
from phantom.algorithms.random_search import RandomSearchMinimizer
from phantom.benchmarks.benchmark_a import get_benchmark_a_cases
from phantom.benchmarks.benchmark_b import get_benchmark_b_cases
from phantom.benchmarks.benchmark_c import get_benchmark_c_cases
from phantom.benchmarks.runner import BenchmarkRunner
from phantom.core.graph import CausalGraph
from phantom.core.oracle import FailureOracle
from phantom.core.otel import OTelTraceImporter
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace
from phantom.report.reporter import IncidentReporter
from phantom.reproduction.generator import ReproducerGenerator


console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="phantom")
def main():
    """Project PHANTOM: Automated Failure Reconstruction and Causal Minimization Engine."""
    pass


@main.command("benchmark")
@click.option("--output-dir", "-o", default="benchmark_artifacts", help="Directory for benchmark artifacts.")
@click.option("--group", "-g", type=click.Choice(["a", "b", "c", "all"]), default="all", help="Benchmark group to run.")
@click.option("--algo", "-a", type=click.Choice(["phantom", "feedback", "ddmin", "random", "all"]), default="all", help="Algorithm to evaluate.")
@click.option("--plot/--no-plot", default=True, help="Generate comparison charts.")
def benchmark_cmd(output_dir: str, group: str, algo: str, plot: bool):
    """Run the empirical comparative evaluation suite."""
    runner = BenchmarkRunner(output_dir=output_dir)

    cases = []
    if group in ["a", "all"]:
        cases.extend(get_benchmark_a_cases())
    if group in ["b", "all"]:
        cases.extend(get_benchmark_b_cases())
    if group in ["c", "all"]:
        cases.extend(get_benchmark_c_cases())

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
def info_cmd(trace_path: str):
    """Inspect and analyze a distributed execution trace file."""
    trace = Trace.load(trace_path)
    graph = CausalGraph(trace)
    stats = graph.stats()

    table = Table(title=f"Trace Inspection: {Path(trace_path).name}")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Events", str(len(trace.events)))
    table.add_row("Participating Nodes", ", ".join(sorted(trace.nodes())))
    table.add_row("Injected Faults", str(len(trace.fault_events())))
    table.add_row("Failure Signature", trace.failure_signature or "None")
    table.add_row("DAG Nodes / Edges", f"{stats['num_nodes']} nodes, {stats['num_edges']} edges")
    table.add_row("Is Valid DAG", str(stats["is_dag"]))
    table.add_row("Graph Density", f"{stats['density']:.4f}")

    console.print(table)

    conflicts = graph.find_conflicting_pairs()
    if conflicts:
        console.print(f"\n[bold yellow]Identified {len(conflicts)} Concurrent State Conflict Pair(s) (Race Candidates):[/bold yellow]")
        for e1, e2 in conflicts[:5]:
            console.print(f"  • {e1.event_id} ({e1.node}) <--> {e2.event_id} ({e2.node}) on key '{e1.payload.get('key')}'")


@main.command("demo")
@click.option("--output-dir", "-o", default="demo_output", help="Directory for demo artifacts.")
def demo_cmd(output_dir: str):
    """Run an instant demonstration of PHANTOM causal minimization on duplicate order processing."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    cases = get_benchmark_a_cases()
    demo_case = cases[0]  # Duplicate order processing

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

    # Generate standalone reproducer script
    script_path = out / "reproduce_duplicate_order.py"
    ReproducerGenerator.generate_script(result, script_path)
    console.print(f"[bold green][+] Standalone Reproducer Script generated:[/bold green] [cyan]{script_path}[/cyan]")

    # Run the reproducer
    console.print("\n[bold]Verifying generated reproduction script by direct execution...[/bold]")
    res = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    console.print(res.stdout, markup=False)
    if res.returncode == 0:
        console.print("[bold green][+] Reproducer verified successfully! Deterministic failure achieved.[/bold green]")
    else:
        console.print(f"[bold red]X Verification failed (code {res.returncode}):[/bold red]\n{res.stderr}")


@main.command("verify")
@click.argument("script_path", type=click.Path(exists=True))
def verify_cmd(script_path: str):
    """Execute and verify a standalone reproduction script."""
    console.print(f"[bold]Executing reproducer: [cyan]{script_path}[/cyan]...[/bold]")
    res = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    console.print(res.stdout, markup=False)
    if res.returncode == 0:
        console.print("[bold green][+] Verification PASSED: Target invariant violated as expected.[/bold green]")
    else:
        console.print(f"[bold red]X Verification FAILED (exit code {res.returncode}):[/bold red]\n{res.stderr}")
        sys.exit(res.returncode)


if __name__ == "__main__":
    main()
