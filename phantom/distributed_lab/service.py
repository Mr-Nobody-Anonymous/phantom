"""
Real distributed service implementation for the PHANTOM Distributed Laboratory.
Includes concurrent node workers, network fault interceptor (delays, drops, partitions),
disk-backed WAL durability, and live event tracing.
"""

from __future__ import annotations
import copy
import json
from pathlib import Path
import queue
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from phantom.core.event import Event, EventType, VectorClock
from phantom.core.trace import Trace


class NetworkFaultInterceptor:
    """
    Controls real message delivery between distributed nodes:
    - Packet drops by message ID or pattern
    - Injected latency / delays
    - Network partitions (split-brain)
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.dropped_msg_ids: Set[str] = set()
        self.node_delays: Dict[str, float] = {}  # node -> delay in seconds
        self.partitions: List[Set[str]] = []  # isolated network partitions

    def drop_message(self, msg_id: str) -> None:
        with self.lock:
            self.dropped_msg_ids.add(str(msg_id))

    def set_delay(self, node: str, delay_seconds: float) -> None:
        with self.lock:
            self.node_delays[node] = delay_seconds

    def clear_delay(self, node: str) -> None:
        with self.lock:
            self.node_delays.pop(node, None)

    def partition(self, partitions: List[Set[str]]) -> None:
        with self.lock:
            self.partitions = [set(p) for p in partitions]

    def heal(self) -> None:
        with self.lock:
            self.partitions = []
            self.dropped_msg_ids.clear()
            self.node_delays.clear()

    def can_deliver(self, source: str, target: str, msg_id: str) -> Tuple[bool, float]:
        """Returns (can_deliver, delay_seconds)."""
        with self.lock:
            if str(msg_id) in self.dropped_msg_ids:
                return False, 0.0

            if self.partitions:
                for part in self.partitions:
                    if source in part and target in part:
                        break
                    if source in part and target not in part:
                        return False, 0.0

            delay = self.node_delays.get(source, 0.0) + self.node_delays.get(target, 0.0)
            return True, delay


class DistributedNode:
    """
    A real stateful distributed service node.
    Maintains an in-memory buffer, a disk-backed SQLite WAL, and an inbox queue.
    """

    def __init__(
        self,
        node_id: str,
        cluster: DistributedLabCluster,
        storage_dir: Optional[Path] = None,
    ):
        self.node_id = node_id
        self.cluster = cluster
        self.storage_dir = storage_dir or Path(f".lab_storage_{node_id}")
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_dir / f"{node_id}_store.db"

        self.lock = threading.Lock()
        self.inbox: queue.Queue = queue.Queue()
        self.is_running = False
        self.is_crashed = False
        self.worker_thread: Optional[threading.Thread] = None

        # Memory state & uncommitted writes
        self.memory_kv: Dict[str, Any] = {}
        self.unflushed_writes: List[Tuple[str, Any]] = []

        # Vector clock
        self.vector_clock = VectorClock({node_id: 0})
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS durable_kv (key TEXT PRIMARY KEY, value TEXT, version INTEGER)"
            )
            conn.commit()

    def start(self) -> None:
        with self.lock:
            if self.is_running:
                return
            self.is_running = True
            self.is_crashed = False
            self.worker_thread = threading.Thread(target=self._run_loop, daemon=True)
            self.worker_thread.start()

    def stop(self) -> None:
        with self.lock:
            self.is_running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=1.0)

    def crash(self) -> None:
        """Simulate a sudden crash: kill worker, discard unflushed in-memory writes."""
        with self.lock:
            self.is_crashed = True
            self.is_running = False
            # In-memory unflushed writes are lost!
            self.unflushed_writes.clear()
            # Clear memory KV to force reload from disk WAL
            self.memory_kv.clear()
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=0.5)

    def restart(self) -> None:
        """Restart node: recover state strictly from durable SQLite WAL on disk."""
        with self.lock:
            self.is_crashed = False
            # Recover durable state from disk
            self._recover_from_wal()
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._run_loop, daemon=True)
            self.worker_thread.start()

    def _recover_from_wal(self) -> None:
        self.memory_kv.clear()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT key, value FROM durable_kv")
            for k, v_json in cur.fetchall():
                try:
                    self.memory_kv[k] = json.loads(v_json)
                except Exception:
                    self.memory_kv[k] = v_json

    def write(self, key: str, value: Any, durable: bool = False, version: int = 1) -> None:
        with self.lock:
            if self.is_crashed:
                return
            self.vector_clock.increment(self.node_id)
            self.memory_kv[key] = value

            if durable:
                # Synchronous fsync to disk WAL
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO durable_kv (key, value, version) VALUES (?, ?, ?)",
                        (key, json.dumps(value), version),
                    )
                    conn.commit()
            else:
                self.unflushed_writes.append((key, value))

        # Record event in cluster trace
        self.cluster.record_event(
            Event(
                event_id=f"write_{self.node_id}_{key}_{int(time.time()*1000)%100000}",
                event_type=EventType.WRITE,
                node=self.node_id,
                payload={"key": key, "val": value, "durable": durable, "version": version, "store": self.node_id},
                vector_clock=self.vector_clock.clone(),
                logical_time=self.vector_clock.get(self.node_id),
            )
        )

    def flush_wal(self) -> None:
        """Sync unflushed memory writes to disk."""
        with self.lock:
            if self.is_crashed:
                return
            with sqlite3.connect(self.db_path) as conn:
                for k, v in self.unflushed_writes:
                    conn.execute(
                        "INSERT OR REPLACE INTO durable_kv (key, value, version) VALUES (?, ?, ?)",
                        (k, json.dumps(v), 1),
                    )
                conn.commit()
            self.unflushed_writes.clear()
            self.vector_clock.increment(self.node_id)

        self.cluster.record_event(
            Event(
                event_id=f"sync_{self.node_id}_{int(time.time()*1000)%100000}",
                event_type=EventType.SYNC,
                node=self.node_id,
                payload={"store": self.node_id},
                vector_clock=self.vector_clock.clone(),
                logical_time=self.vector_clock.get(self.node_id),
            )
        )

    def read(self, key: str) -> Any:
        with self.lock:
            if self.is_crashed:
                return None
            self.vector_clock.increment(self.node_id)
            val = self.memory_kv.get(key)

        self.cluster.record_event(
            Event(
                event_id=f"read_{self.node_id}_{key}_{int(time.time()*1000)%100000}",
                event_type=EventType.READ,
                node=self.node_id,
                payload={"key": key, "val": val, "store": self.node_id},
                vector_clock=self.vector_clock.clone(),
                logical_time=self.vector_clock.get(self.node_id),
            )
        )
        return val

    def send_message(self, target_node: str, msg_id: str, action: str, data: Dict[str, Any]) -> None:
        with self.lock:
            if self.is_crashed:
                return
            self.vector_clock.increment(self.node_id)
            vc = self.vector_clock.clone()

        self.cluster.record_event(
            Event(
                event_id=f"send_{msg_id}",
                event_type=EventType.SEND,
                node=self.node_id,
                target=target_node,
                payload={"msg_id": msg_id, "action": action, "data": data},
                vector_clock=vc,
                logical_time=vc.get(self.node_id),
            )
        )

        # Dispatch via cluster interceptor
        self.cluster.dispatch_message(self.node_id, target_node, msg_id, action, data, vc)

    def deliver_message(self, msg: Dict[str, Any]) -> None:
        """Receive message into inbox."""
        if not self.is_crashed:
            self.inbox.put(msg)

    def _run_loop(self) -> None:
        while self.is_running and not self.is_crashed:
            try:
                msg = self.inbox.get(timeout=0.1)
            except queue.Empty:
                continue

            with self.lock:
                if not self.is_running or self.is_crashed:
                    break
                # Update vector clock from incoming message
                msg_vc: VectorClock = msg.get("vector_clock", VectorClock())
                self.vector_clock.update(msg_vc)
                self.vector_clock.increment(self.node_id)
                curr_vc = self.vector_clock.clone()

            # Record RECEIVE event
            msg_id = msg.get("msg_id", "")
            self.cluster.record_event(
                Event(
                    event_id=f"recv_{msg_id}_{self.node_id}",
                    event_type=EventType.RECEIVE,
                    node=self.node_id,
                    target=msg.get("source"),
                    payload={"msg_id": msg_id, "action": msg.get("action"), "data": msg.get("data")},
                    vector_clock=curr_vc,
                    logical_time=curr_vc.get(self.node_id),
                )
            )

            # Process action
            action = msg.get("action")
            data = msg.get("data", {})
            if action == "replicate_write":
                k = data.get("key")
                v = data.get("value")
                durable = data.get("durable", False)
                self.write(k, v, durable=durable)
            elif action == "order_commit":
                order_id = data.get("order_id")
                orders = self.cluster.shared_state.setdefault("committed_orders", [])
                orders.append(order_id)


class ClusterState:
    """Snapshot representation of distributed laboratory cluster state."""
    def __init__(self, node_states: Dict[str, Dict[str, Any]], shared_state: Dict[str, Any]):
        self.node_states = copy.deepcopy(node_states)
        self.shared_state = copy.deepcopy(shared_state)


class DistributedLabCluster:
    """
    A controllable cluster of real distributed service nodes for testing and reproducing bugs.
    """

    def __init__(self, node_ids: List[str], temp_dir: Optional[Path] = None):
        self.node_ids = node_ids
        self.temp_dir = temp_dir or Path(".lab_temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.interceptor = NetworkFaultInterceptor()
        self.lock = threading.Lock()
        self.events: List[Event] = []
        self.shared_state: Dict[str, Any] = {}
        self.nodes: Dict[str, DistributedNode] = {}

        for nid in node_ids:
            self.nodes[nid] = DistributedNode(
                node_id=nid,
                cluster=self,
                storage_dir=self.temp_dir / nid,
            )

    def start(self) -> None:
        for node in self.nodes.values():
            node.start()

    def stop(self) -> None:
        for node in self.nodes.values():
            node.stop()

    def reset(self) -> None:
        """Reset all nodes, clean databases, and clear intercepted events."""
        self.stop()
        self.interceptor.heal()
        with self.lock:
            self.events.clear()
            self.shared_state.clear()
        for node in self.nodes.values():
            if node.db_path.exists():
                node.db_path.unlink()
            node._init_db()
            node.memory_kv.clear()
            node.unflushed_writes.clear()
            node.vector_clock = VectorClock({node.node_id: 0})
        self.start()

    def record_event(self, event: Event) -> None:
        with self.lock:
            self.events.append(event)

    def dispatch_message(
        self,
        source: str,
        target: str,
        msg_id: str,
        action: str,
        data: Dict[str, Any],
        vc: VectorClock,
    ) -> None:
        can_send, delay = self.interceptor.can_deliver(source, target, msg_id)
        if not can_send:
            return

        def _deliver():
            if delay > 0:
                time.sleep(delay)
            if target in self.nodes:
                self.nodes[target].deliver_message({
                    "source": source,
                    "target": target,
                    "msg_id": msg_id,
                    "action": action,
                    "data": data,
                    "vector_clock": vc,
                })

        threading.Thread(target=_deliver, daemon=True).start()

    def capture_trace(self, failure_signature: str = "") -> Trace:
        with self.lock:
            evts = list(self.events)
        fail_id = None
        for e in evts:
            if e.event_type == EventType.ASSERTION:
                fail_id = e.event_id
                break
        return Trace(
            events=evts,
            failure_event_id=fail_id,
            failure_signature=failure_signature,
            metadata={"source": "DistributedLabCluster", "nodes": list(self.node_ids)},
        )

    def snapshot(self) -> ClusterState:
        node_states = {}
        for nid, node in self.nodes.items():
            with node.lock:
                node_states[nid] = {
                    "memory": dict(node.memory_kv),
                    "crashed": node.is_crashed,
                    "unflushed": list(node.unflushed_writes),
                }
        return ClusterState(node_states, self.shared_state)

    def restore(self, snap: ClusterState) -> None:
        self.shared_state = copy.deepcopy(snap.shared_state)
        for nid, nstate in snap.node_states.items():
            if nid in self.nodes:
                node = self.nodes[nid]
                with node.lock:
                    node.memory_kv = dict(nstate["memory"])
                    node.is_crashed = nstate["crashed"]
                    node.unflushed_writes = list(nstate["unflushed"])
