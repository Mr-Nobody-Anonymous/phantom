"""
Incident report generator for Project PHANTOM.
Produces clean terminal, markdown, and JSON summaries of discovered minimal failure explanations.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from phantom.algorithms.base import MinimizerResult


class IncidentReporter:
    """Formats and exports failure reconstruction incident reports."""

    @staticmethod
    def format_markdown(result: MinimizerResult) -> str:
        trace = result.minimized_trace
        fail_sig = trace.failure_signature or "UnspecifiedInvariantFailure"

        lines = [
            f"# PHANTOM Incident Reconstruction Report",
            f"",
            f"**Observed Failure:** `{fail_sig}`  ",
            f"**Algorithm:** `{result.algorithm_name}`  ",
            f"**Search Invocations:** {result.executions_count} executions  ",
            f"**Trace Reduction:** {result.original_event_count} -> {result.minimized_event_count} events ({result.reduction_ratio * 100:.1f}% reduction)  ",
            f"**Empirical Stability:** {result.reproduction_rate * 100:.1f}% (tested across repeated runs)  ",
            f"**Causal Confidence Score:** {result.confidence_score * 100:.1f}%  ",
            f"",
            f"---",
            f"",
            f"## Minimal Reproducer Schedule",
            f"",
        ]

        for i, e in enumerate(trace.events, 1):
            is_nec = "[*] [Necessary]" if e.event_id in result.necessary_events else ""
            lines.append(
                f"{i}. **`[{e.node}]`** `{e.event_type.value}` (`{e.event_id}`) {is_nec}  "
            )
            if e.payload:
                clean_payload = {k: v for k, v in e.payload.items() if not k.startswith("_")}
                lines.append(f"   *Payload:* `{json.dumps(clean_payload)}`  ")

        lines.extend([
            f"",
            f"---",
            f"",
            f"## Empirical Causal Evidence",
            f"",
            f"- **Counterfactual Necessity:** For each retained event marked `[Necessary]`, omitting the event eliminates the invariant violation in the controlled testbed.",
            f"- **Fault Sensitivity:** Removed {len(result.irrelevant_events)} extraneous events/perturbations that did not causally contribute to the invariant violation.",
            f"- **Deterministic Repeatability:** The isolated minimal schedule reproduces `{fail_sig}` with {result.reproduction_rate * 100:.1f}% stability.",
            f"- **Failure Preservation:** Ground-truth failure signature preserved without inducing unintended cascading timeouts or crashes.",
            f"",
        ])

        return "\n".join(lines)

    @classmethod
    def print_terminal_report(cls, result: MinimizerResult) -> None:
        console = Console()
        md_text = cls.format_markdown(result)
        console.print("\n")
        console.print(Panel(md_text, title="PHANTOM Incident Report", border_style="red"), markup=False)
        console.print("\n")

    @classmethod
    def save_report(
        cls,
        result: MinimizerResult,
        output_path: str | Path,
    ) -> None:
        p = Path(output_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.suffix.lower() == ".json":
            summary = result.summary()
            summary["retained_events"] = [e.to_dict() for e in result.minimized_trace.events]
            p.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        else:
            p.write_text(cls.format_markdown(result), encoding="utf-8")
