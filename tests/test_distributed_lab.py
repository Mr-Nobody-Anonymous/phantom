"""
Tests for DistributedLabCluster and DistributedLabAdapter:
- Real concurrent node execution
- Real disk-backed SQLite WAL persistence and crash recovery
- Real network interceptor (packet delays and drops)
- Full end-to-end bug reproduction against the live distributed application
"""

from pathlib import Path
import time
import pytest

from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.core.oracle import FailureOracle
from phantom.distributed_lab.service import DistributedLabCluster
from phantom.distributed_lab.adapter import DistributedLabAdapter


def test_distributed_lab_node_durability_and_crash_recovery(tmp_path: Path):
    """Real node persists durable writes to SQLite on disk, recovering them after crash."""
    cluster = DistributedLabCluster(node_ids=["node1"], temp_dir=tmp_path)
    cluster.start()

    node = cluster.nodes["node1"]
    # Write durable key
    node.write("durable_setting", "enabled", durable=True)
    # Write volatile key
    node.write("volatile_setting", "cached", durable=False)

    # Crash the node
    node.crash()
    assert node.is_crashed is True

    # Restart the node
    node.restart()
    assert node.is_crashed is False

    # Durable write was recovered from disk
    assert node.read("durable_setting") == "enabled"
    # Volatile unflushed write was lost on crash
    assert node.read("volatile_setting") is None

    cluster.stop()


def test_distributed_lab_network_fault_interception(tmp_path: Path):
    """Network interceptor controls real message delivery between concurrent nodes."""
    cluster = DistributedLabCluster(node_ids=["nodeA", "nodeB"], temp_dir=tmp_path)
    cluster.start()

    # Drop message
    cluster.interceptor.drop_message("msg_to_drop")
    cluster.nodes["nodeA"].send_message("nodeB", "msg_to_drop", "replicate_write", {"key": "test_k", "value": "val1"})
    time.sleep(0.05)

    # Message was dropped; nodeB did not receive it
    assert cluster.nodes["nodeB"].read("test_k") is None

    # Normal message delivery without drop
    cluster.nodes["nodeA"].send_message("nodeB", "msg_ok", "replicate_write", {"key": "test_k", "value": "val1"})
    time.sleep(0.1)

    assert cluster.nodes["nodeB"].read("test_k") == "val1"
    cluster.stop()


def test_distributed_lab_adapter_end_to_end_reproduction(tmp_path: Path):
    """
    Run real replicated storage workload with crash fault, capture trace,
    minimize with PHANTOM, and verify reproduction against the real application.
    """
    adapter = DistributedLabAdapter(storage_dir=tmp_path)
    adapter.start()

    # Run real workload with crash before fsync
    trace = adapter.run_replicated_storage_workload(fault_type="crash_before_sync")
    assert trace.failure_event_id is not None
    assert trace.failure_signature == "LostUncommittedWriteOnCrash"

    # Minimize trace with PHANTOM
    oracle = FailureOracle(expected_signature="LostUncommittedWriteOnCrash")
    oracle.register_invariant(
        "LostUncommittedWriteOnCrash",
        lambda s: (s.get("stores", {}).get("nodeB") or s.get("node_states", {}).get("nodeB", {})).get("account_1") == 100,
    )

    from phantom.core.scheduler import EventScheduler
    minimizer = PhantomCausalMinimizer()
    res = minimizer.minimize(trace, oracle, EventScheduler())

    assert res.reproduced is True
    assert res.minimized_event_count <= res.original_event_count

    # Verify that the minimized schedule triggers the same bug in the LIVE distributed cluster!
    live_result = adapter.verify_reproduction_against_application(res, oracle)
    assert live_result.is_reproduced is True

    adapter.stop()
