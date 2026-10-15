"""
Stable machine-readable programmatic API for Project PHANTOM.
Provides clean entrypoints for CI pipelines, automated debuggers, and external tooling.
"""

from __future__ import annotations
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

from phantom.algorithms.base import AblationConfig, MinimizerResult, SearchBudget
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.algorithms.feedback_search import FeedbackGuidedSearchMinimizer
from phantom.algorithms.delta_debugging import DeltaDebuggingMinimizer
from phantom.algorithms.random_search import RandomSearchMinimizer
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.graph import CausalGraph, CausalInconsistencyError
from phantom.core.oracle import FailureOracle, OracleResult, OracleVerdict
from phantom.core.otel import OTelTraceImporter
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace
from phantom.report.reporter import IncidentReporter
from phantom.reproduction.generator import ReproducerGenerator


def minimize_trace(
    trace: Trace,
    oracle: FailureOracle,
    scheduler: Optional[EventScheduler] = None,
    algorithm: str = "phantom",
    max_executions: int = 200,
    time_budget_seconds: Optional[float] = None,
    checkpoint_file: Optional[str | Path] = None,
    resume: bool = False,
    strict_graph: bool = False,
    ablation: Optional[AblationConfig] = None,
    seed: Optional[int] = 42,
) -> MinimizerResult:
    """
    Programmatic entrypoint to minimize a distributed execution trace.
    Returns structured MinimizerResult with causal necessity, minimality level,
    and divergence records.
    """
    sched = scheduler or EventScheduler()

    algo_lower = algorithm.lower().strip()
    if algo_lower in {"phantom", "causal", "phantom_causal"}:
        minimizer = PhantomCausalMinimizer(
            max_executions=max_executions,
            time_budget_seconds=time_budget_seconds,
            checkpoint_file=checkpoint_file,
            resume=resume,
            strict_graph=strict_graph,
            ablation=ablation,
            seed=seed,
        )
    elif algo_lower in {"feedback", "feedback_guided"}:
        minimizer = FeedbackGuidedSearchMinimizer(
            max_executions=max_executions,
            seed=seed,
        )
    elif algo_lower in {"ddmin", "delta_debugging"}:
        minimizer = DeltaDebuggingMinimizer(
            max_executions=max_executions,
            seed=seed,
        )
    elif algo_lower in {"random", "random_search"}:
        minimizer = RandomSearchMinimizer(
            max_executions=max_executions,
            seed=seed,
        )
    else:
        raise ValueError(f"Unknown minimizer algorithm: '{algorithm}'. Choose from: phantom, feedback, ddmin, random.")

    return minimizer.minimize(trace, oracle, sched)


def analyze_trace(
    trace: Trace,
    strict_mode: bool = False,
) -> Dict[str, Any]:
    """
    Perform causal graph inspection, DAG verification, and race conflict detection.
    Returns structured JSON-serializable dictionary.
    """
    graph = CausalGraph(trace, strict_mode=strict_mode)
    stats = graph.stats()
    conflicts = graph.find_conflicting_pairs()

    return {
        "event_count": len(trace.events),
        "participating_nodes": sorted(list(trace.nodes())),
        "fault_event_count": len(trace.fault_events()),
        "failure_signature": trace.failure_signature,
        "is_dag": stats["is_dag"],
        "num_nodes": stats["num_nodes"],
        "num_edges": stats["num_edges"],
        "density": round(stats["density"], 4),
        "is_uncertain": stats["is_uncertain"],
        "uncertainty_reasons": stats["uncertainty_reasons"],
        "diagnostics": stats["diagnostics"],
        "conflicting_pairs_count": len(conflicts),
        "conflicting_pairs": [
            {
                "event_1": e1.event_id,
                "node_1": e1.node,
                "event_2": e2.event_id,
                "node_2": e2.node,
                "key": e1.payload.get("key"),
            }
            for e1, e2 in conflicts
        ],
        "trace_hash": trace.compute_hash() if hasattr(trace, "compute_hash") else "",
    }


def replay_trace(
    trace: Trace,
    oracle: FailureOracle,
    scheduler: Optional[EventScheduler] = None,
) -> OracleResult:
    """Execute a trace schedule and evaluate it with the given failure oracle."""
    sched = scheduler or EventScheduler()
    return sched.replay(trace, oracle)


def export_reproducer_bundle(
    result: MinimizerResult,
    output_dir: str | Path,
) -> Dict[str, Path]:
    """Export standalone reproducer script, pytest test, Dockerfile, and docker-compose."""
    return ReproducerGenerator.generate_bundle(result, output_dir)


def import_otel_trace(path_or_dict: Any) -> Trace:
    """Ingest OpenTelemetry or Jaeger JSON trace into a PHANTOM Trace."""
    return OTelTraceImporter.from_otel_json(path_or_dict)


def verify_reproducer_script(
    script_path: str | Path,
    timeout: float = 30.0,
) -> Tuple[bool, str]:
    """
    Execute a standalone reproducer script in an isolated subprocess with timeout.
    Returns (success, stdout/stderr output).
    """
    p = Path(script_path)
    if not p.exists():
        return False, f"Reproducer script not found at {script_path}"

    try:
        res = subprocess.run(
            [sys.executable, str(p)],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = res.stdout + ("\n" + res.stderr if res.stderr else "")
        return (res.returncode == 0, output)
    except subprocess.TimeoutExpired:
        return False, f"Execution timed out after {timeout} seconds."
    except Exception as e:
        return False, f"Execution error: {str(e)}"
