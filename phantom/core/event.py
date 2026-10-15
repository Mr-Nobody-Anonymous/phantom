"""
Event and Vector Clock definitions for distributed execution traces.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class EventType(str, Enum):
    """Types of events observed in distributed executions."""
    SEND = "SEND"
    RECEIVE = "RECEIVE"
    READ = "READ"
    WRITE = "WRITE"
    FAULT_DELAY = "FAULT_DELAY"
    FAULT_DROP = "FAULT_DROP"
    FAULT_CRASH = "FAULT_CRASH"
    FAULT_RESTART = "FAULT_RESTART"
    ASSERTION = "ASSERTION"
    INTERNAL = "INTERNAL"


@dataclass
class VectorClock:
    """
    Vector clock representing distributed logical causality.
    Partial order defined by Mattern / Fidge vector timestamps.
    """
    clock: Dict[str, int] = field(default_factory=dict)

    def clone(self) -> VectorClock:
        return VectorClock(clock=dict(self.clock))

    def get(self, node: str) -> int:
        return self.clock.get(node, 0)

    def increment(self, node: str) -> None:
        self.clock[node] = self.get(node) + 1

    def update(self, other: VectorClock) -> None:
        """Merge by element-wise maximum."""
        all_nodes = set(self.clock.keys()).union(other.clock.keys())
        for n in all_nodes:
            self.clock[n] = max(self.get(n), other.get(n))

    def happens_before(self, other: VectorClock) -> bool:
        """
        Returns True if self causally precedes other (self < other):
        ∀ k: self[k] <= other[k] and ∃ k: self[k] < other[k].
        """
        all_nodes = set(self.clock.keys()).union(other.clock.keys())
        has_strictly_less = False
        for n in all_nodes:
            s_val = self.get(n)
            o_val = other.get(n)
            if s_val > o_val:
                return False
            if s_val < o_val:
                has_strictly_less = True
        return has_strictly_less

    def concurrent_with(self, other: VectorClock) -> bool:
        """Returns True if self and other are causally concurrent (self || other)."""
        return not (self.happens_before(other) or other.happens_before(self) or self == other)

    def to_dict(self) -> Dict[str, int]:
        return dict(self.clock)

    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> VectorClock:
        return cls(clock=dict(data or {}))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return False
        all_nodes = set(self.clock.keys()).union(other.clock.keys())
        return all(self.get(n) == other.get(n) for n in all_nodes)

    def __repr__(self) -> str:
        return f"VC({self.clock})"


@dataclass
class Event:
    """
    An observable atomic execution step in a distributed system.
    """
    event_id: str
    event_type: EventType
    node: str
    payload: Dict[str, Any] = field(default_factory=dict)
    vector_clock: VectorClock = field(default_factory=VectorClock)
    logical_time: int = 0
    target: Optional[str] = None
    depends_on: Set[str] = field(default_factory=set)

    def is_fault(self) -> bool:
        return self.event_type in {
            EventType.FAULT_DELAY,
            EventType.FAULT_DROP,
            EventType.FAULT_CRASH,
            EventType.FAULT_RESTART,
        }

    def is_message(self) -> bool:
        return self.event_type in {EventType.SEND, EventType.RECEIVE}

    def is_state_access(self) -> bool:
        return self.event_type in {EventType.READ, EventType.WRITE}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "node": self.node,
            "payload": self.payload,
            "vector_clock": self.vector_clock.to_dict(),
            "logical_time": self.logical_time,
            "target": self.target,
            "depends_on": sorted(list(self.depends_on)),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Event:
        vc_data = data.get("vector_clock", {})
        vc = VectorClock.from_dict(vc_data)
        return cls(
            event_id=data["event_id"],
            event_type=EventType(data["event_type"]),
            node=data["node"],
            payload=data.get("payload", {}),
            vector_clock=vc,
            logical_time=data.get("logical_time", 0),
            target=data.get("target"),
            depends_on=set(data.get("depends_on", [])),
        )

    def __repr__(self) -> str:
        return (
            f"Event({self.event_id}, {self.event_type.value}, node='{self.node}', "
            f"time={self.logical_time})"
        )
