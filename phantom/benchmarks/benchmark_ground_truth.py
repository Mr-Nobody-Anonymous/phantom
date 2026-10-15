"""
Independent Ground Truth Benchmark Scenarios for Project PHANTOM.
Real-world distributed bug reproductions modeled after known distributed system bugs:
1. Raft Split-Brain Stale Leader Commit (etcd/TiKV style lease expiration race)
2. Two-Phase Commit Coordinator Crash Recovery (PostgreSQL/CockroachDB style 2PC desync)
3. Asynchronous Replication Failover Lost Write (Redis Sentinel style data loss)
4. Message Queue Consumer Rebalance Duplicate Charge (Kafka/RabbitMQ style at-least-once race)
5. Snapshot Isolation Doctor On-Call Write Skew (ANSI SQL isolation anomaly)
"""

from __future__ import annotations
from typing import List

from phantom.benchmarks.base import BenchmarkCase
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace


def create_raft_split_brain_case() -> BenchmarkCase:
    """Raft split-brain stale leader accepts write during partition."""
    events = [
        Event("init_leader", EventType.WRITE, "node1", {"key": "leader_id", "val": "node1", "store": "raft"}, logical_time=1),
        Event("init_term", EventType.WRITE, "node1", {"key": "current_term", "val": 1, "store": "raft"}, logical_time=2),
        # Partition isolates node1
        Event("fault_partition", EventType.FAULT_PARTITION, "network", {"partitions": [["node1"], ["node2", "node3"]]}, logical_time=3),
        # node2 and node3 elect node2
        Event("elect_node2", EventType.WRITE, "node2", {"key": "leader_id", "val": "node2", "store": "raft"}, logical_time=4),
        Event("inc_term", EventType.WRITE, "node2", {"key": "current_term", "val": 2, "store": "raft"}, logical_time=5),
        # Background noise
        Event("bg_heartbeat", EventType.INTERNAL, "node3", {"action": "heartbeat_ack"}, logical_time=6),
        # Stale leader node1 still accepts client write before discovering partition
        Event("stale_write", EventType.WRITE, "node1", {"key": "node1_val", "val": "stale_data"}, logical_time=7),
        # New leader node2 accepts write
        Event("new_write", EventType.WRITE, "node2", {"key": "node2_val", "val": "new_data"}, logical_time=8),
        # Invariant check: split-brain dual leader write observed
        Event("assert_split_brain", EventType.ASSERTION, "node1", {
            "name": "RaftSplitBrainStaleLeaderCommit",
            "signature": "RaftSplitBrainStaleLeaderCommit",
        }, logical_time=9, depends_on={"stale_write", "new_write"}),
    ]
    trace = Trace(
        events=events,
        failure_event_id="assert_split_brain",
        failure_signature="RaftSplitBrainStaleLeaderCommit",
        metadata={"category": "Ground Truth", "system": "Raft Consensus"},
    )
    oracle = FailureOracle(expected_signature="RaftSplitBrainStaleLeaderCommit")
    # Invariant: two separate nodes must not both believe they committed concurrent leader writes
    oracle.register_invariant(
        "RaftSplitBrainStaleLeaderCommit",
        lambda s: not (s["kv_store"].get("node1_val") == "stale_data" and s["kv_store"].get("node2_val") == "new_data"),
    )
    scheduler = EventScheduler()
    return BenchmarkCase(
        case_id="GT-01",
        category="Ground Truth",
        name="Raft Split-Brain Stale Leader Commit",
        description="Stale isolated Raft leader accepts client write after network partition without quorum renewal.",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events={"stale_write", "new_write", "assert_split_brain"},
    )


def create_2pc_coordinator_crash_case() -> BenchmarkCase:
    """Two-phase commit coordinator crashes after committing shard 1 before shard 2."""
    events = [
        Event("init_p1", EventType.WRITE, "shard1", {"key": "acc_1", "val": 100}, logical_time=1),
        Event("init_p2", EventType.WRITE, "shard2", {"key": "acc_2", "val": 100}, logical_time=2),
        Event("bg_telemetry", EventType.INTERNAL, "coordinator", {"action": "emit_metrics"}, logical_time=3),
        # Coordinator prepares
        Event("prep_s1", EventType.SEND, "coordinator", {"msg_id": "prep_1", "action": "prepare"}, target="shard1", logical_time=4),
        Event("recv_prep_s1", EventType.RECEIVE, "shard1", {"msg_id": "prep_1"}, logical_time=5),
        # Coordinator commits shard1: deducts 50 from acc_1
        Event("commit_s1", EventType.WRITE, "shard1", {"key": "acc_1", "val": 50}, logical_time=6, depends_on={"init_p1"}),
        # Coordinator crashes before committing shard2 (adding 50 to acc_2)!
        Event("fault_crash_coord", EventType.FAULT_CRASH, "coordinator", {"target_node": "coordinator"}, logical_time=7),
        # Invariant check: total money in system must remain 200 (acc_1 + acc_2 == 200)
        Event("assert_2pc_atomicity", EventType.ASSERTION, "shard1", {
            "name": "TwoPhaseCommitDesyncAtomicLoss",
            "signature": "TwoPhaseCommitDesyncAtomicLoss",
        }, logical_time=8, depends_on={"commit_s1", "init_p2", "fault_crash_coord"}),
    ]
    trace = Trace(
        events=events,
        failure_event_id="assert_2pc_atomicity",
        failure_signature="TwoPhaseCommitDesyncAtomicLoss",
        metadata={"category": "Ground Truth", "system": "Distributed 2PC Transactions"},
    )
    oracle = FailureOracle(expected_signature="TwoPhaseCommitDesyncAtomicLoss")
    oracle.register_invariant(
        "TwoPhaseCommitDesyncAtomicLoss",
        lambda s: not (s["kv_store"].get("acc_1") is not None and s["kv_store"].get("acc_2") is not None and (s["kv_store"].get("acc_1") + s["kv_store"].get("acc_2")) != 200),
    )
    scheduler = EventScheduler()
    return BenchmarkCase(
        case_id="GT-02",
        category="Ground Truth",
        name="Two-Phase Commit Coordinator Crash Atomicity Loss",
        description="2PC coordinator commits Shard 1 then crashes before committing Shard 2, violating cross-shard conservation.",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events={"init_p2", "commit_s1", "assert_2pc_atomicity"},
    )


def create_redis_replication_lost_write_case() -> BenchmarkCase:
    """Asynchronous replication failover causes un-replicated write to be lost on master crash."""
    events = [
        Event("master_data", EventType.WRITE, "master", {"key": "tx_receipt", "val": "confirmed", "durable": False}, logical_time=1),
        Event("client_write", EventType.WRITE, "client", {"key": "client_tx_confirmed", "val": True, "durable": True}, logical_time=2, depends_on={"master_data"}),
        Event("send_repl", EventType.SEND, "master", {"msg_id": "repl_tx_1"}, target="replica", logical_time=3),
        Event("fault_drop_repl", EventType.FAULT_DROP, "network", {"msg_id": "repl_tx_1"}, logical_time=4),
        # Master crashes
        Event("fault_crash_master", EventType.FAULT_CRASH, "master", {"target_node": "master"}, logical_time=5),
        # Replica promoted to new master
        Event("promote_replica", EventType.WRITE, "replica", {"key": "replica_promoted", "val": True, "durable": True}, logical_time=6),
        # Check that acknowledged write exists on promoted replica
        Event("assert_lost_write", EventType.ASSERTION, "replica", {
            "name": "AsyncReplicationLostAcknowledgedWrite",
            "signature": "AsyncReplicationLostAcknowledgedWrite",
        }, logical_time=7, depends_on={"promote_replica", "client_write"}),
    ]
    trace = Trace(
        events=events,
        failure_event_id="assert_lost_write",
        failure_signature="AsyncReplicationLostAcknowledgedWrite",
        metadata={"category": "Ground Truth", "system": "Replicated KV Cache"},
    )
    oracle = FailureOracle(expected_signature="AsyncReplicationLostAcknowledgedWrite")
    # If client received confirmation and replica was promoted, replica must have tx_receipt
    oracle.register_invariant(
        "AsyncReplicationLostAcknowledgedWrite",
        lambda s: not (s["kv_store"].get("client_tx_confirmed") is True and s["kv_store"].get("replica_promoted") is True and s["kv_store"].get("tx_receipt") != "confirmed"),
    )
    scheduler = EventScheduler()
    return BenchmarkCase(
        case_id="GT-03",
        category="Ground Truth",
        name="Async Replication Failover Lost Acknowledged Write",
        description="Primary confirms write before async replication; primary crashes and replica failover loses the write.",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events={"client_write", "promote_replica", "assert_lost_write"},
    )


def create_kafka_rebalance_duplicate_case() -> BenchmarkCase:
    """Consumer rebalance causes duplicate message processing."""
    events = [
        Event("pub_msg", EventType.SEND, "producer", {"msg_id": "order_msg_101", "action": "order_commit", "order_id": "ord_101"}, target="broker", logical_time=1),
        Event("c1_recv", EventType.RECEIVE, "consumer_1", {"msg_id": "order_msg_101"}, logical_time=2),
        Event("c1_charge", EventType.WRITE, "consumer_1", {"key": "charged_ord_101", "val": 1}, logical_time=3),
        # Rebalance triggered before offset commit
        Event("fault_rebalance", EventType.INTERNAL, "broker", {"action": "group_rebalance"}, logical_time=4),
        Event("c2_reassigned", EventType.SEND, "broker", {"msg_id": "order_msg_101_replay", "action": "order_commit", "order_id": "ord_101"}, target="consumer_2", logical_time=5),
        Event("c2_recv", EventType.RECEIVE, "consumer_2", {"msg_id": "order_msg_101_replay"}, logical_time=6),
        Event("c2_charge", EventType.WRITE, "consumer_2", {"key": "charged_ord_101_dup", "val": 1}, logical_time=7),
        Event("assert_duplicate_charge", EventType.ASSERTION, "consumer_2", {
            "name": "ConsumerRebalanceDuplicateExecution",
            "signature": "ConsumerRebalanceDuplicateExecution",
        }, logical_time=8, depends_on={"c1_charge", "c2_charge"}),
    ]
    trace = Trace(
        events=events,
        failure_event_id="assert_duplicate_charge",
        failure_signature="ConsumerRebalanceDuplicateExecution",
        metadata={"category": "Ground Truth", "system": "Message Queue / Event Stream"},
    )
    oracle = FailureOracle(expected_signature="ConsumerRebalanceDuplicateExecution")
    oracle.register_invariant(
        "ConsumerRebalanceDuplicateExecution",
        lambda s: not (s["kv_store"].get("charged_ord_101") == 1 and s["kv_store"].get("charged_ord_101_dup") == 1),
    )
    scheduler = EventScheduler()
    return BenchmarkCase(
        case_id="GT-04",
        category="Ground Truth",
        name="Consumer Rebalance Duplicate Message Execution",
        description="At-least-once message reprocessing after consumer group rebalance interrupts commit.",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events={"c1_charge", "c2_charge", "assert_duplicate_charge"},
    )


def create_write_skew_case() -> BenchmarkCase:
    """Snapshot isolation write skew anomaly."""
    events = [
        Event("init_doc1", EventType.WRITE, "db", {"key": "doc1_on_call", "val": True}, logical_time=1),
        Event("init_doc2", EventType.WRITE, "db", {"key": "doc2_on_call", "val": True}, logical_time=2),
        # Doc 1 checks both on call
        Event("doc1_read", EventType.READ, "tx1", {"key": "doc2_on_call"}, logical_time=3),
        # Doc 2 checks both on call concurrently
        Event("doc2_read", EventType.READ, "tx2", {"key": "doc1_on_call"}, logical_time=4),
        # Both take themselves off call
        Event("doc1_off", EventType.WRITE, "tx1", {"key": "doc1_on_call", "val": False}, logical_time=5, depends_on={"init_doc1"}),
        Event("doc2_off", EventType.WRITE, "tx2", {"key": "doc2_on_call", "val": False}, logical_time=6, depends_on={"init_doc2"}),
        # Invariant: at least one doctor must remain on call
        Event("assert_doctor_on_call", EventType.ASSERTION, "db", {
            "name": "SnapshotIsolationWriteSkewAnomaly",
            "signature": "SnapshotIsolationWriteSkewAnomaly",
        }, logical_time=7, depends_on={"doc1_off", "doc2_off"}),
    ]
    trace = Trace(
        events=events,
        failure_event_id="assert_doctor_on_call",
        failure_signature="SnapshotIsolationWriteSkewAnomaly",
        metadata={"category": "Ground Truth", "system": "Snapshot Isolated Database"},
    )
    oracle = FailureOracle(expected_signature="SnapshotIsolationWriteSkewAnomaly")
    oracle.register_invariant(
        "SnapshotIsolationWriteSkewAnomaly",
        lambda s: not (s["kv_store"].get("doc1_on_call") is False and s["kv_store"].get("doc2_on_call") is False),
    )
    scheduler = EventScheduler()
    return BenchmarkCase(
        case_id="GT-05",
        category="Ground Truth",
        name="Snapshot Isolation Doctor On-Call Write Skew",
        description="Concurrent transactions under Snapshot Isolation violate global predicate constraint via disjoint writes.",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events={"doc1_off", "doc2_off", "assert_doctor_on_call"},
    )


def get_ground_truth_cases() -> List[BenchmarkCase]:
    return [
        create_raft_split_brain_case(),
        create_2pc_coordinator_crash_case(),
        create_redis_replication_lost_write_case(),
        create_kafka_rebalance_duplicate_case(),
        create_write_skew_case(),
    ]
