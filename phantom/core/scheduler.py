"""
Deterministic event-driven interleaving scheduler and execution simulator.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from phantom.core.event import Event, EventType
from phantom.core.oracle import FailureOracle, OracleResult, OracleVerdict
from phantom.core.trace import Trace


@dataclass
class SimulationState:
    """State of the simulated distributed environment during schedule replay."""
    kv_store: Dict[str, Any] = field(default_factory=dict)
    node_status: Dict[str, str] = field(default_factory=dict)  # "UP" or "CRASHED"
    message_buffer: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # msg_id -> payload
    dropped_messages: Set[str] = field(default_factory=set)
    delayed_nodes: Dict[str, int] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    custom_state: Dict[str, Any] = field(default_factory=dict)
    executed_event_ids: Set[str] = field(default_factory=set)

    def is_node_up(self, node: str) -> bool:
        return self.node_status.get(node, "UP") == "UP"


class EventScheduler:
    """
    Replays a candidate trace or event schedule deterministically.
    Supports fault injections (delays, packet drops, node crashes/restarts),
    state read/writes, and custom node behavior handlers.
    """

    def __init__(
        self,
        custom_handlers: Optional[Dict[EventType, Callable[[Event, SimulationState], None]]] = None,
        initial_state: Optional[Dict[str, Any]] = None,
    ):
        self.custom_handlers = custom_handlers or {}
        self.initial_state = initial_state or {}

    def create_initial_state(self) -> SimulationState:
        state = SimulationState()
        state.kv_store = dict(self.initial_state.get("kv_store", {}))
        state.custom_state = dict(self.initial_state.get("custom_state", {}))
        return state

    def execute_event(self, event: Event, state: SimulationState) -> None:
        """Apply an individual event step to the simulation state."""
        state.executed_event_ids.add(event.event_id)

        # Check custom handlers first
        if event.event_type in self.custom_handlers:
            self.custom_handlers[event.event_type](event, state)
            return

        node = event.node

        # Handle node crash status
        if not state.is_node_up(node) and event.event_type not in {EventType.FAULT_RESTART}:
            # Operations on crashed nodes silently fail or log an error
            state.logs.append(f"Skipping event {event.event_id} on crashed node {node}")
            return

        if event.event_type == EventType.WRITE:
            key = event.payload.get("key") if "key" in event.payload else event.payload.get("state_key")
            val = event.payload.get("val") if "val" in event.payload else event.payload.get("value")
            if key is not None:
                state.kv_store[str(key)] = val
                state.logs.append(f"Node {node} wrote {key}={val}")

        elif event.event_type == EventType.READ:
            key = event.payload.get("key") if "key" in event.payload else event.payload.get("state_key")
            actual_val = state.kv_store.get(str(key))
            state.logs.append(f"Node {node} read {key}={actual_val}")
            state.custom_state[f"last_read_{key}_{node}"] = actual_val

        elif event.event_type == EventType.SEND:
            msg_id = event.payload.get("msg_id") or event.payload.get("message_id") or event.event_id
            target = event.target or event.payload.get("target")
            if msg_id not in state.dropped_messages:
                state.message_buffer[str(msg_id)] = {
                    "source": node,
                    "target": target,
                    "payload": event.payload,
                }
                state.logs.append(f"Node {node} sent message {msg_id} to {target}")

        elif event.event_type == EventType.RECEIVE:
            msg_id = event.payload.get("msg_id") or event.payload.get("message_id")
            if msg_id and str(msg_id) in state.dropped_messages:
                state.logs.append(f"Message {msg_id} was dropped; receive skipped on {node}")
            elif msg_id and str(msg_id) in state.message_buffer:
                msg_data = state.message_buffer.pop(str(msg_id))
                state.logs.append(f"Node {node} received message {msg_id}")
                # Execute message effect if defined in payload
                action = msg_data["payload"].get("action")
                if action == "order_commit":
                    orders = state.custom_state.setdefault("orders", [])
                    order_id = msg_data["payload"].get("order_id")
                    orders.append(order_id)
            else:
                state.logs.append(f"Receive event {event.event_id} on {node} with no in-flight message")

        elif event.event_type == EventType.FAULT_DROP:
            msg_id = event.payload.get("msg_id") or event.payload.get("message_id")
            if msg_id:
                state.dropped_messages.add(str(msg_id))
                state.message_buffer.pop(str(msg_id), None)
                state.logs.append(f"FAULT: Dropped message {msg_id}")

        elif event.event_type == EventType.FAULT_DELAY:
            duration = event.payload.get("duration", 1)
            target_node = event.payload.get("target_node", node)
            state.delayed_nodes[target_node] = duration
            state.logs.append(f"FAULT: Injected delay of {duration} on node {target_node}")

        elif event.event_type == EventType.FAULT_CRASH:
            target_node = event.payload.get("target_node", node)
            state.node_status[target_node] = "CRASHED"
            state.logs.append(f"FAULT: Node {target_node} crashed")

        elif event.event_type == EventType.FAULT_RESTART:
            target_node = event.payload.get("target_node", node)
            state.node_status[target_node] = "UP"
            state.logs.append(f"FAULT: Node {target_node} restarted")

        elif event.event_type == EventType.ASSERTION:
            assertion_name = event.payload.get("assertion_name") or event.payload.get("name")
            key = event.payload.get("key")
            if key is not None and "expected" in event.payload:
                actual = state.kv_store.get(key)
                if actual != event.payload["expected"]:
                    sig = event.payload.get("signature") or assertion_name or "AssertionError"
                    state.errors.append(sig)
            state.logs.append(f"Assertion check on {node}: {assertion_name}")

        elif event.event_type == EventType.INTERNAL:
            action = event.payload.get("action")
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
            "custom_state": state.custom_state,
            "node_status": state.node_status,
            "dropped_messages": list(state.dropped_messages),
            "logs": state.logs,
            "errors": state.errors,
            "executed_event_ids": set(state.executed_event_ids),
        }

        return oracle.evaluate(eval_state, state.errors)
