"""
Benchmark evaluation runner and statistical comparison engine.
"""

from __future__ import annotations
import csv
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
from rich.console import Console
from rich.table import Table

from phantom.algorithms.base import MinimizerResult
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.algorithms.delta_debugging import DeltaDebuggingMinimizer
from phantom.algorithms.random_search import RandomSearchMinimizer
from phantom.benchmarks.base import BenchmarkCase
from phantom.benchmarks.benchmark_a import get_benchmark_a_cases
from phantom.benchmarks.benchmark_b import get_benchmark_b_cases
from phantom.benchmarks.benchmark_c import get_benchmark_c_cases


class BenchmarkRunner:
    """
    Executes all benchmark cases across competing minimizers,
    computes statistical confidence metrics, and generates visual charts.
    """

    def __init__(self, output_dir: str = "benchmark_artifacts"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.console = Console()

    def get_all_cases(self) -> List[BenchmarkCase]:
        cases = []
        cases.extend(get_benchmark_a_cases())
        cases.extend(get_benchmark_b_cases())
        cases.extend(get_benchmark_c_cases())
        return cases

    def run_suite(
        self,
        cases: Optional[List[BenchmarkCase]] = None,
        algorithms: Optional[List[Any]] = None,
        verbose: bool = True,
    ) -> List[Dict[str, Any]]:
        """Run all cases against all algorithms and return detailed records."""
        if cases is None:
            cases = self.get_all_cases()

        if algorithms is None:
            algorithms = [
                PhantomCausalMinimizer(max_executions=150),
                DeltaDebuggingMinimizer(max_executions=150),
                RandomSearchMinimizer(max_executions=150),
            ]

        results: List[Dict[str, Any]] = []

        if verbose:
            self.console.print(
                f"[bold cyan]Running Project PHANTOM Benchmark Suite[/bold cyan] "
                f"({len(cases)} cases, {len(algorithms)} algorithms)...\n"
            )

        for case in cases:
            if verbose:
                self.console.print(
                    f"[yellow]Evaluating Case:[/yellow] [bold]{case.case_id}[/bold] "
                    f"({case.category} - {case.name}) | Orig Events: {len(case.trace.events)}"
                )

            for algo in algorithms:
                res: MinimizerResult = algo.minimize(case.trace, case.oracle, case.scheduler)
                record = {
                    "case_id": case.case_id,
                    "category": case.category,
                    "case_name": case.name,
                    "algorithm": algo.name,
                    "orig_events": res.original_event_count,
                    "min_events": res.minimized_event_count,
                    "reduction_ratio": res.reduction_ratio,
                    "reduction_pct": round(res.reduction_ratio * 100, 2),
                    "executions": res.executions_count,
                    "reproduced": res.reproduced,
                    "failure_preserved": res.failure_preserved,
                    "stability": res.reproduction_rate,
                    "confidence": res.confidence_score,
                    "time_sec": res.wall_clock_time,
                }
                results.append(record)

                if verbose:
                    self.console.print(
                        f"  -> {algo.name:<25}: "
                        f"Execs: {res.executions_count:<4} | "
                        f"Retained: {res.minimized_event_count}/{res.original_event_count} "
                        f"({record['reduction_pct']}%) | "
                        f"Repro Rate: {res.reproduction_rate:.2f} | "
                        f"Time: {res.wall_clock_time:.3f}s"
                    )

        # Save results
        self._save_results(results)
        if verbose:
            self.print_summary_table(results)

        return results

    def _save_results(self, records: List[Dict[str, Any]]) -> None:
        # JSON dump
        json_path = self.output_dir / "benchmark_results.json"
        json_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

        # CSV dump
        csv_path = self.output_dir / "benchmark_summary.csv"
        if records:
            keys = list(records[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(records)

    def print_summary_table(self, records: List[Dict[str, Any]]) -> None:
        """Render a formatted comparison table using Rich."""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            groups.setdefault(r["algorithm"], []).append(r)

        table = Table(title="Project PHANTOM: Comparative Research Benchmark Evaluation")

        table.add_column("Algorithm", style="bold cyan")
        table.add_column("Avg Executions", justify="right", style="green")
        table.add_column("Avg Reduction %", justify="right", style="magenta")
        table.add_column("Repro Rate", justify="right", style="yellow")
        table.add_column("Failure Preserved", justify="center", style="white")
        table.add_column("Avg Time (s)", justify="right", style="blue")

        for algo_name, group in sorted(groups.items()):
            avg_exec = f"{sum(g['executions'] for g in group) / len(group):.1f}"
            avg_red = f"{sum(g['reduction_pct'] for g in group) / len(group):.1f}%"
            repro_rate = f"{(sum(g['stability'] for g in group) / len(group)) * 100:.1f}%"
            pres = f"{int((sum(1 for g in group if g['failure_preserved']) / len(group)) * 100)}%"
            avg_time = f"{sum(g['time_sec'] for g in group) / len(group):.3f}s"

            table.add_row(algo_name, avg_exec, avg_red, repro_rate, pres, avg_time)

        self.console.print("\n")
        self.console.print(table)
        self.console.print("\n")

    def generate_plots(self, records: List[Dict[str, Any]]) -> str:
        """Generate publication-ready comparison bar chart saved as an image."""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            groups.setdefault(r["algorithm"], []).append(r)

        algorithms = sorted(groups.keys())
        avg_executions = [sum(g["executions"] for g in groups[a]) / len(groups[a]) for a in algorithms]
        avg_reduction = [sum(g["reduction_pct"] for g in groups[a]) / len(groups[a]) for a in algorithms]
        avg_stability = [(sum(g["stability"] for g in groups[a]) / len(groups[a])) * 100 for a in algorithms]

        fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
        colors = ["#2b5c8f", "#d95f02", "#7570b3"]

        labels = [
            a.replace("PHANTOM_CausalMinimizer", "PHANTOM\n(Causal)")
             .replace("DeltaDebugging_ddmin", "Delta Debugging\n(ddmin)")
             .replace("RandomSearch", "Random\nSearch")
            for a in algorithms
        ]

        # Plot 1: Executions to Reproduction (Lower is better)
        axes[0].bar(labels, avg_executions, color=colors[:len(algorithms)], alpha=0.85, edgecolor="black")
        axes[0].set_title("Search Efficiency: Executions\n(Lower is Better)", fontsize=11, fontweight="bold")
        axes[0].set_ylabel("Average Executions / Test Invocations")

        # Plot 2: Event Reduction % (Higher is better)
        axes[1].bar(labels, avg_reduction, color=colors[:len(algorithms)], alpha=0.85, edgecolor="black")
        axes[1].set_title("Trace Reduction Ratio\n(Higher is Better)", fontsize=11, fontweight="bold")
        axes[1].set_ylabel("Average Trace Reduction (%)")
        axes[1].set_ylim(0, 100)

        # Plot 3: Reproduction Stability % (Higher is better)
        axes[2].bar(labels, avg_stability, color=colors[:len(algorithms)], alpha=0.85, edgecolor="black")
        axes[2].set_title("Reproduction Stability Rate\n(Repeatability across runs)", fontsize=11, fontweight="bold")
        axes[2].set_ylabel("Deterministic Reproduction Rate (%)")
        axes[2].set_ylim(0, 105)

        plt.tight_layout()
        chart_path = self.output_dir / "benchmark_comparison.png"
        fig.savefig(chart_path, dpi=200)
        plt.close(fig)

        return str(chart_path)
