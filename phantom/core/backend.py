"""
Pluggable execution backend architecture for Project PHANTOM.
Defines explicit execution capabilities, snapshot/restore contracts,
and backend implementations: SimulatedBackend, DistributedLabBackend, ContainerBackend.
"""

from __future__ import annotations
import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from phantom.core.event import Event, EventType


class ExecutionCapability(str, Enum):
    """Explicit capabilities provided by an execution backend."""
    SIMULATED = "SIMULATED"                    # In-memory deterministic simulation
    DURABLE_STORAGE = "DURABLE_STORAGE"        # Simulated disk / WAL durability semantics
    NETWORK_CONTROL = "NETWORK_CONTROL"        # Partitioning, packet drops, delays
    PROCESS_ISOLATION = "PROCESS_ISOLATION"    # Multi-process or thread isolation
    CONTAINER = "CONTAINER"                    # Docker / containerized service execution
    LIVE_SYSTEM = "LIVE_SYSTEM"                # Real distributed service / socket execution


@dataclass
class DivergenceRecord:
    """Records where a replayed execution diverged from the expected trace."""
    event_id: str
    divergence_type: str  # "VALUE_MISMATCH", "UNEXPECTED_DROP", "STATE_ANOMALY", "TIMING_SKEW"
    expected: Any
    observed: Any
    details: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "type": self.divergence_type,
            "expected": str(self.expected),
            "observed": str(self.observed),
            "details": self.details,
        }


class ExecutionBackend(ABC):
    """
    Abstract interface for executing distributed schedules.
    Provides explicit capabilities, snapshot/restore semantics,
    and divergence detection.
    """

    def __init__(self, name: str, capabilities: Set[ExecutionCapability]):
        self.name = name
        self.capabilities = capabilities
        self.divergences: List[DivergenceRecord] = []

    @abstractmethod
    def setup(self) -> None:
        """Initialize the execution environment."""
        pass

    @abstractmethod
    def reset(self, initial_state: Optional[Dict[str, Any]] = None) -> None:
        """Reset state cleanly before an experiment."""
        pass

    @abstractmethod
    def execute_event(self, event: Event) -> Dict[str, Any]:
        """Apply an event step to the underlying execution layer."""
        pass

    @abstractmethod
    def snapshot(self) -> Any:
        """Capture a point-in-time state snapshot."""
        pass

    @abstractmethod
    def restore(self, snapshot: Any) -> None:
        """Restore environment to a previously captured snapshot."""
        pass

    @abstractmethod
    def get_state(self) -> Dict[str, Any]:
        """Return the current observable state for oracle inspection."""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Clean up resources after execution."""
        pass


class SimulatedBackend(ExecutionBackend):
    """
    Enhanced deterministic in-memory distributed simulation backend.
    Models:
    - Multi-store state isolation (stores, databases, tenants)
    - Durability semantics: in-memory WAL vs durable disk storage, unflushed writes lost on crash
    - Delay semantics: node execution timestamps and postponement of subsequent events
    - Message routing: buffers, drops, network partitions (split-brain)
    - Read/write value validation & divergence detection
    """

    def __init__(self, initial_state: Optional[Dict[str, Any]] = None):
        super().__init__(
            name="SimulatedBackend",
            capabilities={
                ExecutionCapability.SIMULATED,
                ExecutionCapability.DURABLE_STORAGE,
                ExecutionCapability.NETWORK_CONTROL,
            },
        )
        self.initial_state = initial_state or {}
        # Core state
        self.kv_store: Dict[str, Any] = {}
        self.stores: Dict[str, Dict[str, Any]] = {}  # store_id -> {key: val}
        self.durable_stores: Dict[str, Dict[str, Any]] = {}
        self.unflushed_writes: Dict[str, List[Tuple[str, str, Any]]] = {}  # node -> [(store, key, val)]
        self.node_status: Dict[str, str] = {}  # node -> "UP" | "CRASHED"
        self.node_clocks: Dict[str, float] = {}  # node -> logical time offset
        self.delayed_nodes: Dict[str, float] = {}
        self.message_buffer: Dict[str, Dict[str, Any]] = {}
        self.dropped_messages: Set[str] = set()
        self.network_partitions: List[Set[str]] = []
        self.executed_event_ids: Set[str] = set()
        self.logs: List[str] = []
        self.errors: List[str] = []
        self.custom_state: Dict[str, Any] = {}
        self.divergences: List[DivergenceRecord] = []
        self.reset(self.initial_state)

    def setup(self) -> None:
        pass

    def reset(self, initial_state: Optional[Dict[str, Any]] = None) -> None:
        init = initial_state if initial_state is not None else self.initial_state
        self.kv_store = copy.deepcopy(init.get("kv_store", {}))
        self.stores = {"default": copy.deepcopy(self.kv_store)}
        if "stores" in init:
            for s_name, s_data in init["stores"].items():
                self.stores[s_name] = copy.deepcopy(s_data)
        self.durable_stores = copy.deepcopy(self.stores)
        self.unflushed_writes = {}
        self.node_status = copy.deepcopy(init.get("node_status", {}))
        self.node_clocks = {}
        self.delayed_nodes = {}
        self.message_buffer = {}
        self.dropped_messages = set(init.get("dropped_messages", []))
        self.network_partitions = []
        self.executed_event_ids = set()
        self.logs = []
        self.errors = []
        self.custom_state = copy.deepcopy(init.get("custom_state", {}))
        self.divergences = []

    def snapshot(self) -> Dict[str, Any]:
        return {
            "kv_store": copy.deepcopy(self.kv_store),
            "stores": copy.deepcopy(self.stores),
            "durable_stores": copy.deepcopy(self.durable_stores),
            "unflushed_writes": copy.deepcopy(self.unflushed_writes),
            "node_status": copy.deepcopy(self.node_status),
            "node_clocks": copy.deepcopy(self.node_clocks),
            "delayed_nodes": copy.deepcopy(self.delayed_nodes),
            "message_buffer": copy.deepcopy(self.message_buffer),
            "dropped_messages": set(self.dropped_messages),
            "network_partitions": [set(p) for p in self.network_partitions],
            "executed_event_ids": set(self.executed_event_ids),
            "logs": list(self.logs),
            "errors": list(self.errors),
            "custom_state": copy.deepcopy(self.custom_state),
            "divergences": list(self.divergences),
        }

    def restore(self, snapshot: Dict[str, Any]) -> None:
        self.kv_store = copy.deepcopy(snapshot["kv_store"])
        self.stores = copy.deepcopy(snapshot["stores"])
        self.durable_stores = copy.deepcopy(snapshot["durable_stores"])
        self.unflushed_writes = copy.deepcopy(snapshot["unflushed_writes"])
        self.node_status = copy.deepcopy(snapshot["node_status"])
        self.node_clocks = copy.deepcopy(snapshot["node_clocks"])
        self.delayed_nodes = copy.deepcopy(snapshot["delayed_nodes"])
        self.message_buffer = copy.deepcopy(snapshot["message_buffer"])
        self.dropped_messages = set(snapshot["dropped_messages"])
        self.network_partitions = [set(p) for p in snapshot["network_partitions"]]
        self.executed_event_ids = set(snapshot["executed_event_ids"])
        self.logs = list(snapshot["logs"])
        self.errors = list(snapshot["errors"])
        self.custom_state = copy.deepcopy(snapshot["custom_state"])
        self.divergences = list(snapshot["divergences"])

    def is_node_up(self, node: str) -> bool:
        return self.node_status.get(node, "UP") == "UP"

    def can_communicate(self, source: str, target: str) -> bool:
        """Check if source and target are in the same partition when partitions are active."""
        if not self.network_partitions or not source or not target:
            return True
        for part in self.network_partitions:
            if source in part and target in part:
                return True
            if source in part and target not in part:
                return False
        return True

    def execute_event(self, event: Event) -> Dict[str, Any]:
        self.executed_event_ids.add(event.event_id)
        node = event.node

        # Advance node clock if node had a delay injected
        node_delay = self.delayed_nodes.get(node, 0.0)
        self.node_clocks[node] = self.node_clocks.get(node, 0.0) + node_delay

        # Check crashed status
        if not self.is_node_up(node) and event.event_type != EventType.FAULT_RESTART:
            self.logs.append(f"Skipping event {event.event_id} on crashed node {node}")
            return {"status": "skipped_crashed"}

        payload = event.payload
        store_name = (
            payload.get("store")
            or payload.get("store_id")
            or payload.get("database")
            or payload.get("db")
            or payload.get("replica")
            or payload.get("tenant")
            or "default"
        )
        store = self.stores.setdefault(store_name, {})
        durable_store = self.durable_stores.setdefault(store_name, {})

        if event.event_type == EventType.WRITE:
            key = payload.get("key") if "key" in payload else payload.get("state_key")
            val = payload.get("val") if "val" in payload else payload.get("value")
            is_durable = payload.get("sync", False) or payload.get("durable", False) or payload.get("is_durable", False)

            if key is not None:
                k_str = str(key)
                store[k_str] = val
                self.kv_store[k_str] = val  # update default kv view
                if is_durable:
                    durable_store[k_str] = val
                else:
                    self.unflushed_writes.setdefault(node, []).append((store_name, k_str, val))
                self.logs.append(f"Node {node} wrote {store_name}:{key}={val} (durable={is_durable})")

        elif event.event_type == EventType.READ:
            key = payload.get("key") if "key" in payload else payload.get("state_key")
            if key is not None:
                k_str = str(key)
                if store_name != "default":
                    actual_val = store.get(k_str)
                else:
                    actual_val = self.kv_store.get(k_str)
                self.logs.append(f"Node {node} read {store_name}:{key}={actual_val}")
                self.custom_state[f"last_read_{key}_{node}"] = actual_val

                # Read validation & divergence check
                expected_val = payload.get("expected") or payload.get("val") or payload.get("value")
                if expected_val is not None and actual_val != expected_val:
                    div = DivergenceRecord(
                        event_id=event.event_id,
                        divergence_type="READ_VALUE_MISMATCH",
                        expected=expected_val,
                        observed=actual_val,
                        details=f"Read on {node} for {store_name}:{key} observed '{actual_val}', expected '{expected_val}'",
                    )
                    self.divergences.append(div)
                    self.logs.append(f"DIVERGENCE: {div.details}")

        elif event.event_type == EventType.SYNC:
            # Explicit sync/flush of node's WAL to durable disk
            flushed = self.unflushed_writes.pop(node, [])
            for s_name, k, v in flushed:
                self.durable_stores.setdefault(s_name, {})[k] = v
            self.logs.append(f"Node {node} flushed {len(flushed)} writes to durable storage")

        elif event.event_type == EventType.SEND:
            msg_id = payload.get("msg_id") or payload.get("message_id") or event.event_id
            target = event.target or payload.get("target")
            m_str = str(msg_id)

            if m_str in self.dropped_messages:
                self.logs.append(f"Send of {m_str} from {node} dropped due to prior fault")
            elif not self.can_communicate(node, str(target)):
                self.dropped_messages.add(m_str)
                self.logs.append(f"Send of {m_str} from {node} dropped due to network partition with {target}")
            else:
                self.message_buffer[m_str] = {
                    "source": node,
                    "target": target,
                    "payload": payload,
                    "send_time": self.node_clocks.get(node, 0.0),
                }
                self.logs.append(f"Node {node} sent message {m_str} to {target}")

        elif event.event_type == EventType.RECEIVE:
            msg_id = payload.get("msg_id") or payload.get("message_id")
            if msg_id:
                m_str = str(msg_id)
                if m_str in self.dropped_messages:
                    self.logs.append(f"Message {m_str} was dropped; receive skipped on {node}")
                elif m_str in self.message_buffer:
                    msg_data = self.message_buffer.pop(m_str)
                    self.logs.append(f"Node {node} received message {m_str}")
                    action = msg_data["payload"].get("action")
                    if action == "order_commit":
                        orders = self.custom_state.setdefault("orders", [])
                        order_id = msg_data["payload"].get("order_id")
                        orders.append(order_id)
                else:
                    self.logs.append(f"Receive event {event.event_id} on {node} with no in-flight message")

        elif event.event_type == EventType.FAULT_DROP:
            msg_id = payload.get("msg_id") or payload.get("message_id")
            if msg_id:
                m_str = str(msg_id)
                self.dropped_messages.add(m_str)
                self.message_buffer.pop(m_str, None)
                self.logs.append(f"FAULT: Dropped message {m_str}")

        elif event.event_type == EventType.FAULT_DELAY:
            duration = float(payload.get("duration", 1))
            target_node = payload.get("target_node", node)
            self.delayed_nodes[target_node] = duration
            self.node_clocks[target_node] = self.node_clocks.get(target_node, 0.0) + duration
            self.logs.append(f"FAULT: Injected delay of {duration} on node {target_node}")

        elif event.event_type == EventType.FAULT_CRASH:
            target_node = payload.get("target_node", node)
            self.node_status[target_node] = "CRASHED"
            # Durability loss: unflushed writes on target_node are lost!
            lost_writes = self.unflushed_writes.pop(target_node, [])
            for s_name, k, _ in lost_writes:
                # Revert store to durable_store state
                if s_name in self.durable_stores and k in self.durable_stores[s_name]:
                    self.stores[s_name][k] = self.durable_stores[s_name][k]
                    if s_name == "default":
                        self.kv_store[k] = self.durable_stores[s_name][k]
                else:
                    self.stores.setdefault(s_name, {}).pop(k, None)
                    if s_name == "default":
                        self.kv_store.pop(k, None)
            self.logs.append(
                f"FAULT: Node {target_node} crashed ({len(lost_writes)} unflushed writes lost)"
            )

        elif event.event_type == EventType.FAULT_RESTART:
            target_node = payload.get("target_node", node)
            self.node_status[target_node] = "UP"
            self.delayed_nodes.pop(target_node, None)
            self.logs.append(f"FAULT: Node {target_node} restarted; state restored from durable storage")

        elif event.event_type == EventType.FAULT_PARTITION:
            # payload can contain "partitions": [["nodeA", "nodeB"], ["nodeC"]]
            parts = payload.get("partitions", [])
            self.network_partitions = [set(p) for p in parts]
            self.logs.append(f"FAULT: Network partition activated: {self.network_partitions}")

        elif event.event_type == EventType.FAULT_HEAL:
            self.network_partitions = []
            self.logs.append(f"FAULT: Network partition healed; full connectivity restored")

        elif event.event_type == EventType.ASSERTION:
            assertion_name = payload.get("assertion_name") or payload.get("name")
            key = payload.get("key")
            if key is not None and "expected" in payload:
                k_str = str(key)
                if store_name != "default":
                    actual = store.get(k_str)
                else:
                    actual = self.kv_store.get(k_str)
                if actual != payload["expected"]:
                    sig = payload.get("signature") or assertion_name or "AssertionError"
                    self.errors.append(sig)
            self.logs.append(f"Assertion check on {node}: {assertion_name}")

        elif event.event_type == EventType.INTERNAL:
            action = payload.get("action")
            if action:
                self.logs.append(f"Internal action on {node}: {action}")

        return {"status": "ok"}

    def get_state(self) -> Dict[str, Any]:
        return {
            "kv_store": copy.deepcopy(self.kv_store),
            "stores": copy.deepcopy(self.stores),
            "durable_stores": copy.deepcopy(self.durable_stores),
            "custom_state": copy.deepcopy(self.custom_state),
            "node_status": copy.deepcopy(self.node_status),
            "node_clocks": copy.deepcopy(self.node_clocks),
            "delayed_nodes": copy.deepcopy(self.delayed_nodes),
            "dropped_messages": list(self.dropped_messages),
            "logs": list(self.logs),
            "errors": list(self.errors),
            "executed_event_ids": set(self.executed_event_ids),
            "divergences": [d.to_dict() for d in self.divergences],
        }

    def teardown(self) -> None:
        pass
