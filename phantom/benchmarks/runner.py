"""
Benchmark evaluation runner, statistical comparison, randomized multi-seed trials,
ablation study suite, and scale benchmarking for Project PHANTOM.
"""

from __future__ import annotations
import csv
import json
from pathlib import Path
import platform
import statistics
import time
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
from rich.console import Console
from rich.table import Table

from phantom.algorithms.base import AblationConfig, MinimizerResult
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.algorithms.feedback_search import FeedbackGuidedSearchMinimizer
from phantom.algorithms.delta_debugging import DeltaDebuggingMinimizer
from phantom.algorithms.random_search import RandomSearchMinimizer
from phantom.benchmarks.base import BenchmarkCase
from phantom.benchmarks.benchmark_a import get_benchmark_a_cases
from phantom.benchmarks.benchmark_b import get_benchmark_b_cases
from phantom.benchmarks.benchmark_c import get_benchmark_c_cases
from phantom.benchmarks.benchmark_ground_truth import get_ground_truth_cases
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


class BenchmarkRunner:
    """
    Executes benchmark cases across competing minimizers,
    computes statistical confidence metrics and variance across seeds,
    runs ablation studies, scale tests, and generates publication charts.
    """

    def __init__(self, output_dir: str = "benchmark_artifacts"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_runs_dir = self.output_dir / "raw_runs"
        self.raw_runs_dir.mkdir(parents=True, exist_ok=True)
        self.console = Console()

    def get_all_cases(self, include_ground_truth: bool = True) -> List[BenchmarkCase]:
        cases = []
        cases.extend(get_benchmark_a_cases())
        cases.extend(get_benchmark_b_cases())
        cases.extend(get_benchmark_c_cases())
        if include_ground_truth:
            cases.extend(get_ground_truth_cases())
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
                FeedbackGuidedSearchMinimizer(max_executions=150),
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
                    "minimality_level": res.minimality_level.value if hasattr(res.minimality_level, "value") else str(res.minimality_level),
                    "time_sec": res.wall_clock_time,
                }
                results.append(record)

                # Persist raw run artifact
                self._save_raw_run_artifact(case, algo.name, res)

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

    def run_randomized_trials(
        self,
        cases: Optional[List[BenchmarkCase]] = None,
        seeds: Optional[List[int]] = None,
        algorithms: Optional[List[str]] = None,
        verbose: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Run repeated randomized trials across multiple random seeds,
        measuring mean and variance for reduction ratio and test invocations.
        """
        if cases is None:
            cases = self.get_all_cases()[:5]  # Default to representative subset
        seeds = seeds or [42, 1337, 2024, 7, 99]
        algo_names = algorithms or ["phantom", "feedback", "random"]

        aggregated: List[Dict[str, Any]] = []

        if verbose:
            self.console.print(
                f"[bold magenta]Running Multi-Seed Randomized Trials[/bold magenta] "
                f"({len(cases)} cases, {len(seeds)} seeds: {seeds})...\n"
            )

        for case in cases:
            for algo_key in algo_names:
                exec_counts = []
                red_pcts = []
                stabilities = []

                for seed in seeds:
                    if algo_key == "phantom":
                        algo = PhantomCausalMinimizer(max_executions=150, seed=seed)
                    elif algo_key == "feedback":
                        algo = FeedbackGuidedSearchMinimizer(max_executions=150, seed=seed)
                    elif algo_key == "random":
                        algo = RandomSearchMinimizer(max_executions=150, seed=seed)
                    else:
                        algo = DeltaDebuggingMinimizer(max_executions=150, seed=seed)

                    res = algo.minimize(case.trace, case.oracle, case.scheduler)
                    exec_counts.append(res.executions_count)
                    red_pcts.append(res.reduction_ratio * 100)
                    stabilities.append(res.reproduction_rate)

                mean_exec = statistics.mean(exec_counts)
                var_exec = statistics.variance(exec_counts) if len(exec_counts) > 1 else 0.0
                mean_red = statistics.mean(red_pcts)
                var_red = statistics.variance(red_pcts) if len(red_pcts) > 1 else 0.0

                record = {
                    "case_id": case.case_id,
                    "case_name": case.name,
                    "algorithm": algo.name,
                    "seeds_tested": len(seeds),
                    "mean_executions": round(mean_exec, 2),
                    "var_executions": round(var_exec, 2),
                    "std_executions": round(var_exec ** 0.5, 2),
                    "mean_reduction_pct": round(mean_red, 2),
                    "var_reduction": round(var_red, 2),
                    "std_reduction": round(var_red ** 0.5, 2),
                    "mean_stability": round(statistics.mean(stabilities), 2),
                }
                aggregated.append(record)

        # Save randomized trial metrics
        trial_path = self.output_dir / "randomized_trials.json"
        trial_path.write_text(json.dumps(aggregated, indent=2), encoding="utf-8")

        if verbose:
            table = Table(title="Multi-Seed Randomized Trials (Mean ± Std Dev)")
            table.add_column("Case", style="cyan")
            table.add_column("Algorithm", style="bold")
            table.add_column("Executions (Mean ± Std)", justify="right", style="green")
            table.add_column("Reduction % (Mean ± Std)", justify="right", style="magenta")
            for r in aggregated:
                table.add_row(
                    r["case_id"],
                    r["algorithm"],
                    f"{r['mean_executions']} ± {r['std_executions']}",
                    f"{r['mean_reduction_pct']}% ± {r['std_reduction']}",
                )
            self.console.print(table)

        return aggregated

    def run_ablation_study(
        self,
        cases: Optional[List[BenchmarkCase]] = None,
        verbose: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Evaluate PHANTOM with individual subsystems toggled off to isolate
        the exact contribution of causal slicing, fault pruning, and delta debugging.
        """
        if cases is None:
            cases = self.get_all_cases()[:5]

        configurations = [
            ("Full PHANTOM", AblationConfig(enable_causal_slicing=True, enable_fault_pruning=True, enable_delta_debugging=True, enable_necessity_ledger=True)),
            ("No Causal Slicing", AblationConfig(enable_causal_slicing=False, enable_fault_pruning=True, enable_delta_debugging=True, enable_necessity_ledger=True)),
            ("No Fault Pruning", AblationConfig(enable_causal_slicing=True, enable_fault_pruning=False, enable_delta_debugging=True, enable_necessity_ledger=True)),
            ("No Delta Debugging", AblationConfig(enable_causal_slicing=True, enable_fault_pruning=True, enable_delta_debugging=False, enable_necessity_ledger=True)),
            ("No Necessity Ledger", AblationConfig(enable_causal_slicing=True, enable_fault_pruning=True, enable_delta_debugging=True, enable_necessity_ledger=False)),
        ]

        records: List[Dict[str, Any]] = []

        if verbose:
            self.console.print("[bold yellow]Running PHANTOM Ablation Study[/bold yellow]...\n")

        for case in cases:
            for config_name, config in configurations:
                minimizer = PhantomCausalMinimizer(max_executions=150, ablation=config)
                res = minimizer.minimize(case.trace, case.oracle, case.scheduler)
                records.append({
                    "case_id": case.case_id,
                    "case_name": case.name,
                    "configuration": config_name,
                    "executions": res.executions_count,
                    "min_events": res.minimized_event_count,
                    "reduction_pct": round(res.reduction_ratio * 100, 2),
                    "reproduced": res.reproduced,
                    "time_sec": round(res.wall_clock_time, 4),
                })

        ablation_path = self.output_dir / "ablation_results.json"
        ablation_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

        if verbose:
            table = Table(title="PHANTOM Ablation Study (Component Contributions)")
            table.add_column("Case", style="cyan")
            table.add_column("Configuration", style="bold yellow")
            table.add_column("Executions", justify="right", style="green")
            table.add_column("Reduction %", justify="right", style="magenta")
            table.add_column("Time (s)", justify="right", style="blue")
            for r in records:
                table.add_row(
                    r["case_id"],
                    r["configuration"],
                    str(r["executions"]),
                    f"{r['reduction_pct']}%",
                    f"{r['time_sec']:.3f}s",
                )
            self.console.print(table)

        return records

    def run_scale_benchmark(
        self,
        event_counts: Optional[List[int]] = None,
        verbose: bool = True,
    ) -> List[Dict[str, Any]]:
        """Evaluate algorithm runtime and memory scaling on larger traces (50 to 500+ events)."""
        counts = event_counts or [20, 50, 100, 200]
        records: List[Dict[str, Any]] = []

        if verbose:
            self.console.print("[bold green]Running Scale Benchmark[/bold green]...\n")

        for n in counts:
            # Generate synthetic distributed trace of size n
            events = []
            for i in range(1, n):
                node = f"node_{i % 3}"
                if i % 4 == 0:
                    events.append(Event(f"e_{i}", EventType.WRITE, node, {"key": f"k_{i%5}", "val": i}, logical_time=i))
                elif i % 4 == 1:
                    events.append(Event(f"e_{i}", EventType.SEND, node, {"msg_id": f"m_{i}"}, target=f"node_{(i+1)%3}", logical_time=i))
                elif i % 4 == 2:
                    events.append(Event(f"e_{i}", EventType.RECEIVE, node, {"msg_id": f"m_{i-1}"}, logical_time=i))
                else:
                    events.append(Event(f"e_{i}", EventType.INTERNAL, node, {"action": "step"}, logical_time=i))

            # Add critical bug sequence at the end
            events.append(Event(f"e_crit_w", EventType.WRITE, "node_0", {"key": "state_crit", "val": "bug"}, logical_time=n))
            events.append(Event(f"assert_scale", EventType.ASSERTION, "node_0", {
                "name": "ScaleInvariant",
                "key": "state_crit",
                "expected": "clean",
                "signature": "ScaleAnomaly",
            }, logical_time=n + 1, depends_on={f"e_crit_w"}))

            trace = Trace(events=events, failure_event_id="assert_scale", failure_signature="ScaleAnomaly")
            oracle = FailureOracle(expected_signature="ScaleAnomaly")
            oracle.register_invariant("ScaleInvariant", lambda s: s["kv_store"].get("state_crit") != "bug")
            scheduler = EventScheduler()

            start_t = time.perf_counter()
            minimizer = PhantomCausalMinimizer(max_executions=150)
            res = minimizer.minimize(trace, oracle, scheduler)
            elapsed = time.perf_counter() - start_t

            records.append({
                "trace_size": n,
                "executions": res.executions_count,
                "minimized_size": res.minimized_event_count,
                "reduction_pct": round(res.reduction_ratio * 100, 2),
                "wall_clock_time": round(elapsed, 4),
            })

            if verbose:
                self.console.print(f"  Size {n:>4} events -> Minimized to {res.minimized_event_count} in {elapsed:.3f}s ({res.executions_count} execs)")

        scale_path = self.output_dir / "scale_results.json"
        scale_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
        return records

    def _save_raw_run_artifact(self, case: BenchmarkCase, algo_name: str, result: MinimizerResult) -> None:
        """Persist complete raw run artifacts for reproducibility."""
        artifact_id = f"{case.case_id}_{algo_name.replace(' ', '_')}"
        artifact_path = self.raw_runs_dir / f"{artifact_id}.json"
        data = {
            "case_id": case.case_id,
            "case_name": case.name,
            "category": case.category,
            "algorithm": algo_name,
            "input_trace_size": result.original_event_count,
            "input_trace_hash": result.source_trace_hash,
            "failure_signature": result.minimized_trace.failure_signature,
            "executions_count": result.executions_count,
            "minimized_event_count": result.minimized_event_count,
            "reduction_ratio": result.reduction_ratio,
            "reproduction_rate": result.reproduction_rate,
            "confidence_score": result.confidence_score,
            "minimality_level": result.minimality_level.value if hasattr(result.minimality_level, "value") else str(result.minimality_level),
            "termination_reason": result.termination_reason,
            "wall_clock_seconds": result.wall_clock_time,
            "retained_event_ids": result.retained_event_ids,
            "search_history": result.search_history,
            "environment": {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "system": platform.system(),
                "timestamp": time.time(),
            },
        }
        artifact_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _save_results(self, records: List[Dict[str, Any]]) -> None:
        json_path = self.output_dir / "benchmark_results.json"
        json_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

        csv_path = self.output_dir / "benchmark_summary.csv"
        if records:
            keys = list(records[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(records)

    def print_summary_table(self, records: List[Dict[str, Any]]) -> None:
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
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            groups.setdefault(r["algorithm"], []).append(r)

        algorithms = sorted(groups.keys())
        avg_executions = [sum(g["executions"] for g in groups[a]) / len(groups[a]) for a in algorithms]
        avg_reduction = [sum(g["reduction_pct"] for g in groups[a]) / len(groups[a]) for a in algorithms]
        avg_stability = [(sum(g["stability"] for g in groups[a]) / len(groups[a])) * 100 for a in algorithms]

        fig, axes = plt.subplots(1, 3, figsize=(16, 5))
        colors = ["#2b5c8f", "#008080", "#d95f02", "#7570b3"]

        labels = [
            a.replace("PHANTOM_CausalMinimizer", "PHANTOM\n(Causal)")
             .replace("PHANTOM_FeedbackGuidedSearch", "PHANTOM\n(Feedback)")
             .replace("DeltaDebugging_ddmin", "Delta Debugging\n(ddmin)")
             .replace("RandomSearch", "Random\nSearch")
            for a in algorithms
        ]

        axes[0].bar(labels, avg_executions, color=colors[:len(algorithms)], alpha=0.85, edgecolor="black")
        axes[0].set_title("Search Efficiency: Executions\n(Lower is Better)", fontsize=11, fontweight="bold")
        axes[0].set_ylabel("Average Executions")

        axes[1].bar(labels, avg_reduction, color=colors[:len(algorithms)], alpha=0.85, edgecolor="black")
        axes[1].set_title("Trace Reduction Ratio\n(Higher is Better)", fontsize=11, fontweight="bold")
        axes[1].set_ylabel("Average Reduction (%)")
        axes[1].set_ylim(0, 100)

        axes[2].bar(labels, avg_stability, color=colors[:len(algorithms)], alpha=0.85, edgecolor="black")
        axes[2].set_title("Reproduction Stability Rate\n(Repeatability across runs)", fontsize=11, fontweight="bold")
        axes[2].set_ylabel("Reproduction Rate (%)")
        axes[2].set_ylim(0, 105)

        plt.tight_layout()
        chart_path = self.output_dir / "benchmark_comparison.png"
        fig.savefig(chart_path, dpi=200)
        plt.close(fig)

        return str(chart_path)
