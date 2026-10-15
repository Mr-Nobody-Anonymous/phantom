"""
Tests for EventScheduler high-fidelity distributed modeling:
- Durability / WAL semantics (unflushed writes lost on crash, preserved on sync)
- Delay semantics and node clocks
- Network partitions and healing
- Read validation and divergence detection
- State snapshot and restore
"""

import pytest
from phantom.core.event import Event, EventType
from phantom.core.oracle import FailureOracle, OracleVerdict
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


def test_durability_unflushed_writes_lost_on_crash():
    """Unflushed writes in memory must be lost when a node crashes without syncing."""
    events = [
        # Durable write: persisted to disk
        Event("w_dur", EventType.WRITE, "db_node", {"key": "durable_k", "val": "saved", "durable": True}),
        # Non-durable write: held only in volatile WAL buffer
        Event("w_vol", EventType.WRITE, "db_node", {"key": "volatile_k", "val": "unsaved", "durable": False}),
        # Crash db_node
        Event("crash", EventType.FAULT_CRASH, "db_node", {"target_node": "db_node"}),
        # Restart db_node
        Event("restart", EventType.FAULT_RESTART, "db_node", {"target_node": "db_node"}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    state = res.observed_state
    # Durable write survived crash
    assert state["kv_store"].get("durable_k") == "saved"
    # Volatile unflushed write was lost!
    assert "volatile_k" not in state["kv_store"] or state["kv_store"]["volatile_k"] is None


def test_durability_sync_persists_unflushed_writes():
    """Explicit SYNC event flushes volatile writes to durable storage before crash."""
    events = [
        Event("w_vol", EventType.WRITE, "db_node", {"key": "synced_k", "val": "persisted", "durable": False}),
        # Flush WAL to disk
        Event("sync", EventType.SYNC, "db_node"),
        # Crash and restart
        Event("crash", EventType.FAULT_CRASH, "db_node", {"target_node": "db_node"}),
        Event("restart", EventType.FAULT_RESTART, "db_node", {"target_node": "db_node"}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    assert res.observed_state["kv_store"].get("synced_k") == "persisted"


def test_delay_semantics_advances_node_clocks():
    """FAULT_DELAY advances virtual node clock and records delay."""
    events = [
        Event("delay1", EventType.FAULT_DELAY, "worker_1", {"duration": 5, "target_node": "worker_1"}),
        Event("w1", EventType.WRITE, "worker_1", {"key": "work_done", "val": True}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    assert "FAULT: Injected delay of 5 on node worker_1" in res.observed_state["logs"]


def test_network_partition_and_healing():
    """Network partitions block cross-partition message delivery; healing restores it."""
    events = [
        # Activate partition isolating nodeA from nodeB
        Event("part", EventType.FAULT_PARTITION, "net", {"partitions": [["nodeA"], ["nodeB"]]}),
        # Send message from nodeA to nodeB -> should be dropped/blocked
        Event("send_blocked", EventType.SEND, "nodeA", {"msg_id": "blocked_1"}, target="nodeB"),
        # Heal partition
        Event("heal", EventType.FAULT_HEAL, "net"),
        # Send message after heal -> should succeed
        Event("send_healed", EventType.SEND, "nodeA", {"msg_id": "healed_1"}, target="nodeB"),
        Event("recv_healed", EventType.RECEIVE, "nodeB", {"msg_id": "healed_1"}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    logs = res.observed_state["logs"]
    assert any("dropped due to network partition" in log for log in logs)
    assert any("Node nodeB received message healed_1" in log for log in logs)


def test_read_value_divergence_detection():
    """Reading a value different from the original trace expectation must record a divergence."""
    events = [
        # Store has "balance" = 100
        Event("w1", EventType.WRITE, "node1", {"key": "balance", "val": 100}),
        # Read expects "balance" == 500 (e.g. from an alternate un-reproduced execution)
        Event("r1", EventType.READ, "node1", {"key": "balance", "expected": 500}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    assert res.is_divergent is True
    assert len(res.divergences) == 1
    assert res.divergences[0]["type"] == "READ_VALUE_MISMATCH"
    assert res.divergences[0]["expected"] == "500"
    assert res.divergences[0]["observed"] == "100"


def test_state_snapshot_and_restore():
    """State snapshots capture complete environment state and restore cleanly."""
    scheduler = EventScheduler(initial_state={"kv_store": {"k1": "init"}})
    state = scheduler.create_initial_state()

    # Apply mutation
    scheduler.execute_event(Event("w1", EventType.WRITE, "node1", {"key": "k1", "val": "modified"}), state)
    assert state.kv_store["k1"] == "modified"

    # Take snapshot
    snap = state.snapshot()

    # Mutate further
    scheduler.execute_event(Event("w2", EventType.WRITE, "node1", {"key": "k1", "val": "corrupted"}), state)
    assert state.kv_store["k1"] == "corrupted"

    # Restore snapshot
    state.restore(snap)
    assert state.kv_store["k1"] == "modified"
