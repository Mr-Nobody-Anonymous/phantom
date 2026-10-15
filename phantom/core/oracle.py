"""
Failure oracle and invariant verification.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class OracleVerdict(str, Enum):
    """Evaluation verdict of a replayed execution."""
    REPRODUCED = "REPRODUCED"            # Invariant violated matching original failure
    DIFFERENT_FAILURE = "DIFFERENT_FAILURE"  # Unrelated invariant or crash violated
    PASS = "PASS"                        # Execution completed without any violation
    INVALID_SCHEDULE = "INVALID_SCHEDULE"  # Execution is causally broken or malformed


@dataclass
class OracleResult:
    """Outcome of evaluating a trace or system state."""
    verdict: OracleVerdict
    failure_signature: str = ""
    violated_invariant: Optional[str] = None
    observed_state: Dict[str, Any] = field(default_factory=dict)
    details: str = ""

    @property
    def is_reproduced(self) -> bool:
        return self.verdict == OracleVerdict.REPRODUCED

    @property
    def is_pass(self) -> bool:
        return self.verdict == OracleVerdict.PASS


class FailureOracle:
    """
    Validates whether an execution schedule reproduces the target failure.
    Checks invariant predicates and verifies that the failure signature
    matches ground truth rather than tripping unrelated faults.
    """

    def __init__(
        self,
        expected_signature: str,
        invariants: Optional[Dict[str, Callable[[Dict[str, Any]], bool]]] = None,
    ):
        self.expected_signature = expected_signature
        self.invariants = invariants or {}

    def register_invariant(
        self,
        name: str,
        predicate: Callable[[Dict[str, Any]], bool],
    ) -> None:
        """Register an invariant condition that must evaluate to True for safety."""
        self.invariants[name] = predicate

    def evaluate(
        self,
        final_state: Dict[str, Any],
        observed_errors: Optional[List[str]] = None,
    ) -> OracleResult:
        """
        Evaluate final state and observed runtime errors against invariants.
        Returns REPRODUCED if the expected signature matches the violation.
        """
        errors = list(observed_errors or [])

        # Check invariant violations
        for name, predicate in self.invariants.items():
            try:
                is_safe = predicate(final_state)
                if not is_safe:
                    errors.append(name)
            except Exception as e:
                errors.append(f"{name}_Exception: {str(e)}")

        if not errors:
            return OracleResult(
                verdict=OracleVerdict.PASS,
                failure_signature="",
                observed_state=final_state,
                details="All invariants hold; execution passed cleanly.",
            )

        # Check whether any error matches the expected signature
        matching_sig = None
        for err in errors:
            if self.expected_signature.lower() in err.lower() or err.lower() in self.expected_signature.lower():
                matching_sig = err
                break

        if matching_sig:
            return OracleResult(
                verdict=OracleVerdict.REPRODUCED,
                failure_signature=self.expected_signature,
                violated_invariant=matching_sig,
                observed_state=final_state,
                details=f"Target failure successfully reproduced: {matching_sig}",
            )
        else:
            return OracleResult(
                verdict=OracleVerdict.DIFFERENT_FAILURE,
                failure_signature=errors[0],
                violated_invariant=errors[0],
                observed_state=final_state,
                details=f"Different failure occurred (expected '{self.expected_signature}', got '{errors[0]}')",
            )
