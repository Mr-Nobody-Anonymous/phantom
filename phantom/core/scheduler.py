"""
Deterministic event-driven interleaving scheduler and execution simulator.
Enhanced with pluggable backends, durability/WAL modeling, delay queues,
network partitioning, read/write validation, state snapshots, and divergence detection.
"""

from __future__ import annotations
import copy
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from phantom.core.backend import (
    DivergenceRecord,
    ExecutionBackend,
    ExecutionCapability,
    SimulatedBackend,
)
from phantom.core.event import Event, EventType
from phantom.core.oracle import FailureOracle, OracleResult, OracleVerdict
from phantom.core.trace import Trace


@dataclass
class SimulationState:
    """State of the simulated distributed environment during schedule replay."""
    kv_store: Dict[str, Any] = field(default_factory=dict)
    stores: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    durable_stores: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    unflushed_writes: Dict[str, List[Tuple[str, str, Any]]] = field(default_factory=dict)
    node_status: Dict[str, str] = field(default_factory=dict)  # "UP" or "CRASHED"
    node_clocks: Dict[str, float] = field(default_factory=dict)
    message_buffer: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # msg_id -> payload
    dropped_messages: Set[str] = field(default_factory=set)
    delayed_nodes: Dict[str, int] = field(default_factory=dict)
    network_partitions: List[Set[str]] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    custom_state: Dict[str, Any] = field(default_factory=dict)
    executed_event_ids: Set[str] = field(default_factory=set)
    divergences: List[Dict[str, Any]] = field(default_factory=list)

    def is_node_up(self, node: str) -> bool:
        return self.node_status.get(node, "UP") == "UP"

    def can_communicate(self, source: str, target: str) -> bool:
        if not self.network_partitions or not source or not target:
            return True
        for part in self.network_partitions:
            if source in part and target in part:
                return True
            if source in part and target not in part:
                return False
        return True

    def snapshot(self) -> Dict[str, Any]:
        return {
            "kv_store": copy.deepcopy(self.kv_store),
            "stores": copy.deepcopy(self.stores),
            "durable_stores": copy.deepcopy(self.durable_stores),
            "unflushed_writes": copy.deepcopy(self.unflushed_writes),
            "node_status": copy.deepcopy(self.node_status),
            "node_clocks": copy.deepcopy(self.node_clocks),
            "message_buffer": copy.deepcopy(self.message_buffer),
            "dropped_messages": set(self.dropped_messages),
            "delayed_nodes": copy.deepcopy(self.delayed_nodes),
            "network_partitions": [set(p) for p in self.network_partitions],
            "logs": list(self.logs),
            "errors": list(self.errors),
            "custom_state": copy.deepcopy(self.custom_state),
            "executed_event_ids": set(self.executed_event_ids),
            "divergences": list(self.divergences),
        }

    def restore(self, snap: Dict[str, Any]) -> None:
        self.kv_store = copy.deepcopy(snap["kv_store"])
        self.stores = copy.deepcopy(snap.get("stores", {"default": self.kv_store}))
        self.durable_stores = copy.deepcopy(snap.get("durable_stores", self.stores))
        self.unflushed_writes = copy.deepcopy(snap.get("unflushed_writes", {}))
        self.node_status = copy.deepcopy(snap["node_status"])
        self.node_clocks = copy.deepcopy(snap.get("node_clocks", {}))
        self.message_buffer = copy.deepcopy(snap["message_buffer"])
        self.dropped_messages = set(snap["dropped_messages"])
        self.delayed_nodes = copy.deepcopy(snap["delayed_nodes"])
        self.network_partitions = [set(p) for p in snap.get("network_partitions", [])]
        self.logs = list(snap["logs"])
        self.errors = list(snap["errors"])
        self.custom_state = copy.deepcopy(snap["custom_state"])
        self.executed_event_ids = set(snap["executed_event_ids"])
        self.divergences = list(snap.get("divergences", []))


class EventScheduler:
    """
    Replays a candidate trace or event schedule deterministically.
    Supports pluggable execution backends, fault injections (delays, packet drops,
    network partitions, node crashes/restarts with WAL durability), state read/writes,
    state snapshots, and divergence detection.
    """

    def __init__(
        self,
        custom_handlers: Optional[Dict[EventType, Callable[[Event, SimulationState], None]]] = None,
        initial_state: Optional[Dict[str, Any]] = None,
        backend: Optional[ExecutionBackend] = None,
    ):
        self.custom_handlers = custom_handlers or {}
        self.initial_state = initial_state or {}
        self.backend = backend or SimulatedBackend(initial_state=self.initial_state)

    def create_initial_state(self) -> SimulationState:
        state = SimulationState()
        state.kv_store = copy.deepcopy(self.initial_state.get("kv_store", {}))
        state.stores = {"default": copy.deepcopy(state.kv_store)}
        if "stores" in self.initial_state:
            for s_name, s_data in self.initial_state["stores"].items():
                state.stores[s_name] = copy.deepcopy(s_data)
        state.durable_stores = copy.deepcopy(state.stores)
        state.custom_state = copy.deepcopy(self.initial_state.get("custom_state", {}))
        state.node_status = copy.deepcopy(self.initial_state.get("node_status", {}))
        return state

    def execute_event(self, event: Event, state: SimulationState) -> None:
        """Apply an individual event step to the simulation state."""
        state.executed_event_ids.add(event.event_id)

        # Check custom handlers first
        if event.event_type in self.custom_handlers:
            self.custom_handlers[event.event_type](event, state)
            return

        node = event.node

        # Account for delay on node
        node_delay = state.delayed_nodes.get(node, 0)
        state.node_clocks[node] = state.node_clocks.get(node, 0.0) + node_delay

        # Handle node crash status
        if not state.is_node_up(node) and event.event_type not in {EventType.FAULT_RESTART}:
            state.logs.append(f"Skipping event {event.event_id} on crashed node {node}")
            return

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
        store = state.stores.setdefault(store_name, {})
        durable_store = state.durable_stores.setdefault(store_name, {})

        if event.event_type == EventType.WRITE:
            key = payload.get("key") if "key" in payload else payload.get("state_key")
            val = payload.get("val") if "val" in payload else payload.get("value")
            is_durable = payload.get("sync", False) or payload.get("durable", False) or payload.get("is_durable", False)

            if key is not None:
                k_str = str(key)
                store[k_str] = val
                state.kv_store[k_str] = val
                if is_durable:
                    durable_store[k_str] = val
                else:
                    state.unflushed_writes.setdefault(node, []).append((store_name, k_str, val))
                state.logs.append(f"Node {node} wrote {store_name}:{key}={val}")

        elif event.event_type == EventType.READ:
            key = payload.get("key") if "key" in payload else payload.get("state_key")
            if key is not None:
                k_str = str(key)
                if store_name != "default":
                    actual_val = store.get(k_str)
                else:
                    actual_val = state.kv_store.get(k_str)
                state.logs.append(f"Node {node} read {store_name}:{key}={actual_val}")
                state.custom_state[f"last_read_{key}_{node}"] = actual_val

                # Value divergence check against original trace expectation
                expected_val = payload.get("expected") or payload.get("val") or payload.get("value")
                if expected_val is not None and actual_val != expected_val:
                    div = {
                        "event_id": event.event_id,
                        "type": "READ_VALUE_MISMATCH",
                        "expected": str(expected_val),
                        "observed": str(actual_val),
                        "details": f"Node {node} read {actual_val}, expected {expected_val}",
                    }
                    state.divergences.append(div)
                    state.logs.append(f"DIVERGENCE: {div['details']}")

        elif event.event_type == EventType.SYNC:
            flushed = state.unflushed_writes.pop(node, [])
            for s_name, k, v in flushed:
                state.durable_stores.setdefault(s_name, {})[k] = v
            state.logs.append(f"Node {node} synced {len(flushed)} writes to disk")

        elif event.event_type == EventType.SEND:
            msg_id = payload.get("msg_id") or payload.get("message_id") or event.event_id
            target = event.target or payload.get("target")
            m_str = str(msg_id)

            if m_str in state.dropped_messages:
                state.logs.append(f"Send of {m_str} from {node} dropped due to prior fault")
            elif not state.can_communicate(node, str(target)):
                state.dropped_messages.add(m_str)
                state.logs.append(f"Send of {m_str} from {node} dropped due to network partition with {target}")
            else:
                state.message_buffer[m_str] = {
                    "source": node,
                    "target": target,
                    "payload": payload,
                    "send_time": state.node_clocks.get(node, 0.0),
                }
                state.logs.append(f"Node {node} sent message {m_str} to {target}")

        elif event.event_type == EventType.RECEIVE:
            msg_id = payload.get("msg_id") or payload.get("message_id")
            if msg_id:
                m_str = str(msg_id)
                if m_str in state.dropped_messages:
                    state.logs.append(f"Message {m_str} was dropped; receive skipped on {node}")
                elif m_str in state.message_buffer:
                    msg_data = state.message_buffer.pop(m_str)
                    state.logs.append(f"Node {node} received message {m_str}")
                    action = msg_data["payload"].get("action")
                    if action == "order_commit":
                        orders = state.custom_state.setdefault("orders", [])
                        order_id = msg_data["payload"].get("order_id")
                        orders.append(order_id)
                else:
                    state.logs.append(f"Receive event {event.event_id} on {node} with no in-flight message")

        elif event.event_type == EventType.FAULT_DROP:
            msg_id = payload.get("msg_id") or payload.get("message_id")
            if msg_id:
                m_str = str(msg_id)
                state.dropped_messages.add(m_str)
                state.message_buffer.pop(m_str, None)
                state.logs.append(f"FAULT: Dropped message {m_str}")

        elif event.event_type == EventType.FAULT_DELAY:
            duration = int(payload.get("duration", 1))
            target_node = payload.get("target_node", node)
            state.delayed_nodes[target_node] = duration
            state.node_clocks[target_node] = state.node_clocks.get(target_node, 0.0) + duration
            state.logs.append(f"FAULT: Injected delay of {duration} on node {target_node}")

        elif event.event_type == EventType.FAULT_CRASH:
            target_node = payload.get("target_node", node)
            state.node_status[target_node] = "CRASHED"
            # Durability loss: unflushed writes on target_node are lost
            lost_writes = state.unflushed_writes.pop(target_node, [])
            for s_name, k, _ in lost_writes:
                if s_name in state.durable_stores and k in state.durable_stores[s_name]:
                    state.stores[s_name][k] = state.durable_stores[s_name][k]
                    if s_name == "default":
                        state.kv_store[k] = state.durable_stores[s_name][k]
                else:
                    state.stores.setdefault(s_name, {}).pop(k, None)
                    if s_name == "default":
                        state.kv_store.pop(k, None)
            state.logs.append(f"FAULT: Node {target_node} crashed ({len(lost_writes)} unflushed writes lost)")

        elif event.event_type == EventType.FAULT_RESTART:
            target_node = payload.get("target_node", node)
            state.node_status[target_node] = "UP"
            state.delayed_nodes.pop(target_node, None)
            state.logs.append(f"FAULT: Node {target_node} restarted")

        elif event.event_type == EventType.FAULT_PARTITION:
            parts = payload.get("partitions", [])
            state.network_partitions = [set(p) for p in parts]
            state.logs.append(f"FAULT: Network partition activated: {state.network_partitions}")

        elif event.event_type == EventType.FAULT_HEAL:
            state.network_partitions = []
            state.logs.append(f"FAULT: Network partition healed")

        elif event.event_type == EventType.ASSERTION:
            assertion_name = payload.get("assertion_name") or payload.get("name")
            key = payload.get("key")
            if key is not None and "expected" in payload:
                k_str = str(key)
                if store_name != "default":
                    actual = store.get(k_str)
                else:
                    actual = state.kv_store.get(k_str)
                if actual != payload["expected"]:
                    sig = payload.get("signature") or assertion_name or "AssertionError"
                    state.errors.append(sig)
            state.logs.append(f"Assertion check on {node}: {assertion_name}")

        elif event.event_type == EventType.INTERNAL:
            action = payload.get("action")
            if action:
                state.logs.append(f"Internal action on {node}: {action}")

    def replay(self, trace: Trace, oracle: FailureOracle) -> OracleResult:
        """
        Replay all events in the trace in sequential schedule order,
        then evaluate the outcome with the failure oracle.
        """
        state = self.create_initial_state()

        for event in trace.events:
            self.execute_event(event, state)

        # Ensure failure assertion event was reached if one is specified
        if trace.failure_event_id and trace.failure_event_id not in state.executed_event_ids:
            return OracleResult(
                verdict=OracleVerdict.PASS,
                details=f"Failure assertion event '{trace.failure_event_id}' was not executed.",
            )

        # Merge state for oracle evaluation
        eval_state = {
            "kv_store": state.kv_store,
            "stores": state.stores,
            "durable_stores": state.durable_stores,
            "custom_state": state.custom_state,
            "node_status": state.node_status,
            "dropped_messages": list(state.dropped_messages),
            "logs": state.logs,
            "errors": state.errors,
            "executed_event_ids": set(state.executed_event_ids),
            "divergences": list(state.divergences),
        }

        return oracle.evaluate(eval_state, state.errors, divergences=state.divergences)
