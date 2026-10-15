"""
Distributed Laboratory Adapter for Project PHANTOM.
Connects real distributed service executions to PHANTOM's causal minimizer,
enabling direct verification of generated reproducers against the real application.
"""

from __future__ import annotations
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from phantom.algorithms.base import MinimizerResult
from phantom.core.event import Event, EventType
from phantom.core.oracle import FailureOracle, OracleResult, OracleVerdict
from phantom.core.trace import Trace
from phantom.distributed_lab.service import DistributedLabCluster


class DistributedLabAdapter:
    """
    Adapter bridging real distributed cluster executions with PHANTOM analysis:
    - Runs real workloads and fault scenarios against the lab cluster
    - Captures distributed execution traces with happens-before vector clocks
    - Replays minimal schedules against the live application to verify real reproduction
    """

    def __init__(self, node_ids: Optional[List[str]] = None, storage_dir: Optional[Path] = None):
        self.node_ids = node_ids or ["nodeA", "nodeB", "nodeC"]
        self.cluster = DistributedLabCluster(self.node_ids, temp_dir=storage_dir)

    def start(self) -> None:
        self.cluster.start()

    def stop(self) -> None:
        self.cluster.stop()

    def reset(self) -> None:
        self.cluster.reset()

    def run_replicated_storage_workload(
        self,
        fault_type: str = "crash_before_sync",
    ) -> Trace:
        """
        Run a real replicated storage workload across nodes with fault injection:
        1. Node A receives write 'account_1' = 100
        2. Node A replicates write to Node B
        3. If fault == 'crash_before_sync': Node B crashes before flushing WAL to disk
        4. Client attempts to read from Node B after restart
        5. Assertion verifies whether data was lost
        """
        self.reset()
        node_a = self.cluster.nodes["nodeA"]
        node_b = self.cluster.nodes["nodeB"]

        # Step 1: Write to Node A
        node_a.write("account_1", 100, durable=True)

        # Step 2: Replicate to Node B
        if fault_type == "network_drop":
            self.cluster.interceptor.drop_message("msg_repl_1")
        elif fault_type == "network_delay":
            self.cluster.interceptor.set_delay("nodeA", 0.1)

        node_a.send_message(
            target_node="nodeB",
            msg_id="msg_repl_1",
            action="replicate_write",
            data={"key": "account_1", "value": 100, "durable": False},
        )

        time.sleep(0.05)

        if fault_type == "crash_before_sync":
            # Node B crashes while holding write only in unflushed memory
            node_b.crash()
            self.cluster.record_event(
                Event(
                    event_id="fault_crash_nodeB",
                    event_type=EventType.FAULT_CRASH,
                    node="nodeB",
                    payload={"target_node": "nodeB"},
                )
            )
            time.sleep(0.02)
            node_b.restart()
            self.cluster.record_event(
                Event(
                    event_id="fault_restart_nodeB",
                    event_type=EventType.FAULT_RESTART,
                    node="nodeB",
                    payload={"target_node": "nodeB"},
                )
            )

        time.sleep(0.05)

        # Step 3: Read back from Node B
        val = node_b.read("account_1")

        # Step 4: Invariant check (Account balance must be 100 on replicated node)
        assertion_event = Event(
            event_id="assert_replication_durability",
            event_type=EventType.ASSERTION,
            node="nodeB",
            payload={
                "name": "LostUncommittedWriteOnCrash",
                "key": "account_1",
                "store": "nodeB",
                "expected": 100,
                "actual": val,
                "signature": "LostUncommittedWriteOnCrash" if val != 100 else "Clean",
            },
        )
        self.cluster.record_event(assertion_event)

        trace = self.cluster.capture_trace(
            failure_signature="LostUncommittedWriteOnCrash" if val != 100 else "Clean"
        )
        trace.failure_event_id = "assert_replication_durability"
        return trace

    def verify_reproduction_against_application(
        self,
        trace_or_result: Trace | MinimizerResult,
        oracle: FailureOracle,
    ) -> OracleResult:
        """
        Execute the exact events from the minimized schedule against the REAL distributed cluster
        to verify that the failure reproduces against actual services.
        """
        trace = (
            trace_or_result.minimized_trace
            if isinstance(trace_or_result, MinimizerResult)
            else trace_or_result
        )

        self.reset()
        executed_events = set()
        divergences = []

        for event in trace.events:
            executed_events.add(event.event_id)
            node = self.cluster.nodes.get(event.node)

            if event.event_type == EventType.WRITE:
                if node:
                    k = event.payload.get("key", "k")
                    v = event.payload.get("val")
                    durable = event.payload.get("durable", False)
                    node.write(str(k), v, durable=durable)

            elif event.event_type == EventType.READ:
                if node:
                    k = event.payload.get("key", "k")
                    actual_val = node.read(str(k))
                    expected_val = event.payload.get("expected") or event.payload.get("val")
                    if expected_val is not None and actual_val != expected_val:
                        divergences.append({
                            "event_id": event.event_id,
                            "type": "LIVE_READ_MISMATCH",
                            "expected": str(expected_val),
                            "observed": str(actual_val),
                        })

            elif event.event_type == EventType.SYNC:
                if node:
                    node.flush_wal()

            elif event.event_type == EventType.SEND:
                if node:
                    msg_id = event.payload.get("msg_id", event.event_id)
                    action = event.payload.get("action", "msg")
                    data = event.payload.get("data", {})
                    target = event.target or "nodeB"
                    node.send_message(target, msg_id, action, data)

            elif event.event_type == EventType.FAULT_DROP:
                msg_id = event.payload.get("msg_id", "")
                self.cluster.interceptor.drop_message(msg_id)

            elif event.event_type == EventType.FAULT_DELAY:
                dur = float(event.payload.get("duration", 0.05))
                self.cluster.interceptor.set_delay(event.node, dur)

            elif event.event_type == EventType.FAULT_CRASH:
                target_node = event.payload.get("target_node", event.node)
                if target_node in self.cluster.nodes:
                    self.cluster.nodes[target_node].crash()

            elif event.event_type == EventType.FAULT_RESTART:
                target_node = event.payload.get("target_node", event.node)
                if target_node in self.cluster.nodes:
                    self.cluster.nodes[target_node].restart()

        # Allow in-flight messages to settle
        time.sleep(0.05)

        # Check final state against oracle
        final_state = {
            "node_states": {nid: dict(n.memory_kv) for nid, n in self.cluster.nodes.items()},
            "stores": {nid: dict(n.memory_kv) for nid, n in self.cluster.nodes.items()},
            "kv_store": {k: v for n in self.cluster.nodes.values() for k, v in n.memory_kv.items()},
            "divergences": divergences,
        }

        # Check if failure assertion was triggered
        fail_evt = trace.get_failure_event()
        observed_errors = []
        if fail_evt:
            key = fail_evt.payload.get("key")
            node_target = self.cluster.nodes.get(fail_evt.node)
            if node_target and key:
                actual = node_target.read(key)
                if actual != fail_evt.payload.get("expected"):
                    observed_errors.append(trace.failure_signature or "AssertionError")

        return oracle.evaluate(final_state, observed_errors=observed_errors, divergences=divergences)
