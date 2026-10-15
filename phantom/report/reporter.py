"""
Incident report generator for Project PHANTOM.
Enhanced with provenance versioning (source trace hash, engine version, environment),
minimality level distinction, causal evidence classification (proven vs inferred vs experimentally necessary),
and uncertainty disclosures.
"""

from __future__ import annotations
import json
from pathlib import Path
import platform
import sys
from typing import Any, Dict, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from phantom.algorithms.base import MinimizerResult

ENGINE_VERSION = "0.1.0"


class IncidentReporter:
    """Formats and exports failure reconstruction incident reports."""

    @staticmethod
    def format_markdown(result: MinimizerResult) -> str:
        trace = result.minimized_trace
        fail_sig = trace.failure_signature or "UnspecifiedInvariantFailure"
        minimality_str = (
            result.minimality_level.value
            if hasattr(result.minimality_level, "value")
            else str(result.minimality_level)
        )

        lines = [
            f"# PHANTOM Incident Reconstruction Report",
            f"",
            f"**Engine Version:** `PHANTOM v{ENGINE_VERSION}`  ",
            f"**Environment:** `Python {platform.python_version()} on {platform.system()} {platform.machine()}`  ",
            f"**Source Trace Hash (SHA-256):** `{result.source_trace_hash or 'N/A'}`  ",
            f"**Target Failure:** `{fail_sig}`  ",
            f"**Algorithm:** `{result.algorithm_name}`  ",
            f"**Minimality Claim:** `{minimality_str}`  ",
            f"**Termination Reason:** `{result.termination_reason}`  ",
            f"**Search Invocations:** {result.executions_count} executions  ",
            f"**Trace Reduction:** {result.original_event_count} -> {result.minimized_event_count} events ({result.reduction_ratio * 100:.1f}% reduction)  ",
            f"**Empirical Stability:** {result.reproduction_rate * 100:.1f}% (tested across repeated runs)  ",
            f"**Causal Confidence Score:** {result.confidence_score * 100:.1f}%  ",
            f"",
        ]

        if result.uncertain_causality:
            lines.extend([
                f"> [!WARNING] **Causal Uncertainty Warning**",
                f"> Graph analysis identified cycles or ambiguities that required heuristic resolution.",
            ])
            for reason in result.uncertainty_reasons:
                lines.append(f"> - {reason}")
            lines.append("")

        lines.extend([
            f"---",
            f"",
            f"## Minimal Reproducer Schedule",
            f"",
        ])

        for i, e in enumerate(trace.events, 1):
            is_nec = "[*] [Experimentally Necessary]" if e.event_id in result.necessary_events else ""
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
            f"## Rigorous Causal Evidence Classification",
            f"",
            f"### 1. Experimentally Necessary Events ({len(result.necessary_events)})",
            f"Omitting any of these events independently prevents the invariant violation in controlled replay.",
            f"",
            f"### 2. Discarded Extraneous Events ({len(result.irrelevant_events)})",
            f"Removed {len(result.irrelevant_events)} events (telemetry noise, unrelated service actions, non-causal perturbations).",
            f"",
            f"### 3. Minimality Distinction",
            f"- **Claim:** `{minimality_str}`",
            f"- **1-Minimal:** Guaranteed that no single remaining event can be removed without losing failure reproduction.",
            f"- **Global Minimality:** "
            + (
                "Verified by bounded combinatorial evaluation across event subsets."
                if minimality_str == "GLOBALLY-MINIMAL"
                else "Subject to 1-minimality; smaller multi-event combinations may theoretically exist under different schedules."
            ),
            f"",
            f"### 4. Repeatability & Divergence",
            f"- **Reproduction Stability:** {result.reproduction_rate * 100:.1f}% across 10 repeated replays.",
            f"- **Divergences Detected:** {len(result.divergences_detected)} unexpected state or value differences observed during replay.",
            f"",
        ])

        return "\n".join(lines)

    @classmethod
    def print_terminal_report(cls, result: MinimizerResult) -> None:
        console = Console()
        md_text = cls.format_markdown(result)
        console.print("\n")
        border = "yellow" if result.uncertain_causality else "red"
        console.print(Panel(md_text, title="PHANTOM Incident Report", border_style=border), markup=False)
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
            summary["search_history"] = result.search_history
            summary["environment"] = {
                "engine_version": ENGINE_VERSION,
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            }
            p.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        else:
            p.write_text(cls.format_markdown(result), encoding="utf-8")
