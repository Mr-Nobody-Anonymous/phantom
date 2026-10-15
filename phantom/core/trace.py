"""
Execution trace representation, parsing, and serialization.
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from phantom.core.event import Event, EventType, VectorClock


@dataclass
class Trace:
    """
    A sequence of distributed events representing an execution,
    annotated with the observed failure signature.
    """
    events: List[Event] = field(default_factory=list)
    failure_event_id: Optional[str] = None
    failure_signature: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.events)

    def __iter__(self):
        return iter(self.events)

    def __getitem__(self, index: int) -> Event:
        return self.events[index]

    def get_event(self, event_id: str) -> Optional[Event]:
        for e in self.events:
            if e.event_id == event_id:
                return e
        return None

    def get_failure_event(self) -> Optional[Event]:
        if not self.failure_event_id:
            return None
        return self.get_event(self.failure_event_id)

    def nodes(self) -> Set[str]:
        return {e.node for e in self.events}

    def fault_events(self) -> List[Event]:
        return [e for e in self.events if e.is_fault()]

    def filter(self, retained_ids: Set[str]) -> Trace:
        """Return a sub-trace containing only the retained event IDs, preserving relative order."""
        filtered_events = [e for e in self.events if e.event_id in retained_ids]
        new_failure_id = self.failure_event_id if self.failure_event_id in retained_ids else None
        return Trace(
            events=filtered_events,
            failure_event_id=new_failure_id,
            failure_signature=self.failure_signature,
            metadata=dict(self.metadata),
        )

    def validate(self) -> Tuple[bool, Optional[str]]:
        """
        Validate causal sanity of the trace:
        1. Unique event IDs
        2. Non-empty if failure is set
        3. RECEIVE must refer to existing message or preceding SEND
        """
        seen_ids = set()
        for e in self.events:
            if e.event_id in seen_ids:
                return False, f"Duplicate event_id: {e.event_id}"
            seen_ids.add(e.event_id)

        if self.failure_event_id and self.failure_event_id not in seen_ids:
            return False, f"Failure event '{self.failure_event_id}' not found in events"

        return True, None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "events": [e.to_dict() for e in self.events],
            "failure_event_id": self.failure_event_id,
            "failure_signature": self.failure_signature,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Trace:
        events = [Event.from_dict(ed) for ed in data.get("events", [])]
        return cls(
            events=events,
            failure_event_id=data.get("failure_event_id"),
            failure_signature=data.get("failure_signature", ""),
            metadata=data.get("metadata", {}),
        )

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, text: str) -> Trace:
        return cls.from_dict(json.loads(text))

    def save(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> Trace:
        p = Path(path)
        return cls.from_json(p.read_text(encoding="utf-8"))
