"""
Benchmark Group B: Distributed Fault Scenarios.
Covers network delays, dropped messages, retries, service restarts, and replication lag.
"""

from __future__ import annotations
from typing import Dict, List, Set

from phantom.benchmarks.base import BenchmarkCase
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler, SimulationState
from phantom.core.trace import Trace


def create_delayed_retry_storm_case() -> BenchmarkCase:
    """
    Case B1: Delayed response triggers retry storm.
    1. Client sends Request 1 to Server A.
    2. Server A is delayed by FAULT_DELAY.
    3. Client times out and retries with Request 2.
    4. Server A eventually completes Request 1.
    5. Server A also completes Request 2 without deduplication.
    Both charge the customer.
    Trace has 15 events, including irrelevant faults (e.g. noise delay on Server B).
    """
    events = [
        Event("noise_ping", EventType.INTERNAL, "monitor", {"action": "heartbeat"}, logical_time=1),
        Event("req_1_send", EventType.SEND, "client", {"msg_id": "req_1", "action": "charge_order", "order_id": "ord_77"}, target="server_a", logical_time=2),
        Event("fault_delay_server_a", EventType.FAULT_DELAY, "server_a", {"duration": 5, "target_node": "server_a"}, logical_time=3),
        Event("fault_irrelevant_delay_b", EventType.FAULT_DELAY, "server_b", {"duration": 2, "target_node": "server_b"}, logical_time=4),
        Event("client_timeout", EventType.INTERNAL, "client", {"action": "timeout_expired"}, logical_time=5),
        Event("req_2_retry_send", EventType.SEND, "client", {"msg_id": "req_2", "action": "charge_order", "order_id": "ord_77"}, target="server_a", logical_time=6),
        Event("noise_log", EventType.INTERNAL, "logger", {"action": "log_rotation"}, logical_time=7),
        Event("server_a_recv_1", EventType.RECEIVE, "server_a", {"msg_id": "req_1"}, logical_time=8),
        Event("server_a_exec_1", EventType.WRITE, "server_a", {"key": "charge_ord_77_cnt", "val": 1}, logical_time=9),
        Event("server_a_recv_2", EventType.RECEIVE, "server_a", {"msg_id": "req_2"}, logical_time=10),
        Event("server_a_exec_2", EventType.WRITE, "server_a", {"key": "charge_ord_77_cnt", "val": 2}, logical_time=11),
        Event(
            "assert_idempotent_retry",
            EventType.ASSERTION,
            "auditor",
            {"name": "IdempotentRetryInvariant", "expected": 1, "actual": 2, "signature": "RetryStormDuplicateExecution"},
            logical_time=12,
            depends_on={"server_a_exec_2"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_idempotent_retry",
        failure_signature="RetryStormDuplicateExecution",
        metadata={"description": "Delayed response induces duplicate processing across retry"},
    )

    oracle = FailureOracle(expected_signature="RetryStormDuplicateExecution")
    oracle.register_invariant(
        "RetryStormDuplicateExecution",
        lambda state: state.get("kv_store", {}).get("charge_ord_77_cnt", 0) <= 1,
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"charge_ord_77_cnt": 0}})
    ground_truth = {"req_1_send", "fault_delay_server_a", "req_2_retry_send", "server_a_exec_2", "assert_idempotent_retry"}

    return BenchmarkCase(
        case_id="BM_B1",
        category="Benchmark B",
        name="DelayedRetryStorm",
        description="Timing delay causes client timeout and non-idempotent retry duplicate execution",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_split_brain_partition_case() -> BenchmarkCase:
    """
    Case B2: Network partition induces Split-Brain dual leaders.
    Node 1 is leader. Network drops heartbeat from Node 1 to Node 2.
    Node 2 times out and promotes itself to leader while Node 1 is still active.
    Both accept writes simultaneously.
    """
    events = [
        Event("bg_sync", EventType.INTERNAL, "zookeeper-proxy", {"action": "sync"}, logical_time=1),
        Event("node1_heartbeat_send", EventType.SEND, "node_1", {"msg_id": "hb_101", "role": "leader"}, target="node_2", logical_time=2),
        Event("fault_drop_hb", EventType.FAULT_DROP, "network", {"msg_id": "hb_101"}, logical_time=3),
        Event("fault_irrelevant_drop", EventType.FAULT_DROP, "network", {"msg_id": "telemetry_pkg"}, logical_time=4),
        Event("node2_timeout", EventType.INTERNAL, "node_2", {"action": "lease_timeout"}, logical_time=5),
        Event("node2_elect_self", EventType.WRITE, "node_2", {"key": "node_2_role", "val": "LEADER"}, logical_time=6),
        Event("node1_write", EventType.WRITE, "node_1", {"key": "leader_write_epoch", "val": "node1_val"}, logical_time=7),
        Event("node2_write", EventType.WRITE, "node_2", {"key": "leader_write_epoch", "val": "node2_val"}, logical_time=8),
        Event("bg_log", EventType.INTERNAL, "cluster-monitor", {"action": "snapshot"}, logical_time=9),
        Event(
            "assert_single_leader",
            EventType.ASSERTION,
            "cluster-monitor",
            {"name": "SingleActiveLeaderInvariant", "expected": 1, "actual": 2, "signature": "SplitBrainDualLeaderAnomaly"},
            logical_time=10,
            depends_on={"node2_elect_self", "node2_write"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_single_leader",
        failure_signature="SplitBrainDualLeaderAnomaly",
        metadata={"description": "Dropped heartbeat partition triggers concurrent dual leaders"},
    )

    oracle = FailureOracle(expected_signature="SplitBrainDualLeaderAnomaly")
    oracle.register_invariant(
        "SplitBrainDualLeaderAnomaly",
        lambda state: not (
            state.get("kv_store", {}).get("node_2_role") == "LEADER"
            and state.get("kv_store", {}).get("leader_write_epoch") == "node2_val"
            and "fault_drop_hb" in state.get("executed_event_ids", set())
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"node_1_role": "LEADER", "node_2_role": "FOLLOWER"}})
    ground_truth = {"fault_drop_hb", "node2_elect_self", "node2_write", "assert_single_leader"}

    return BenchmarkCase(
        case_id="BM_B2",
        category="Benchmark B",
        name="SplitBrainDualLeader",
        description="Dropped heartbeat partition triggers dual leader election and split-brain writes",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_dropped_ack_timeout_case() -> BenchmarkCase:
    """
    Case B3: Dropped ACK causes uncoordinated rollback.
    Service A sends order commit message to Service B.
    Service B processes commit and sends ACK.
    ACK is dropped by network fault.
    Service A times out and marks order as CANCELLED, leaving Service B COMMITTED.
    """
    events = [
        Event("bg_poll", EventType.INTERNAL, "discovery", {"action": "refresh"}, logical_time=1),
        Event("send_order", EventType.SEND, "service_a", {"msg_id": "ord_msg_55", "action": "commit"}, target="service_b", logical_time=2),
        Event("recv_order", EventType.RECEIVE, "service_b", {"msg_id": "ord_msg_55"}, logical_time=3),
        Event("b_commit", EventType.WRITE, "service_b", {"key": "service_b_order_55", "val": "COMMITTED"}, logical_time=4),
        Event("send_ack", EventType.SEND, "service_b", {"msg_id": "ack_msg_55", "status": "OK"}, target="service_a", logical_time=5),
        Event("fault_drop_ack", EventType.FAULT_DROP, "network", {"msg_id": "ack_msg_55"}, logical_time=6),
        Event("a_timeout", EventType.INTERNAL, "service_a", {"action": "timeout"}, logical_time=7),
        Event("a_cancel", EventType.WRITE, "service_a", {"key": "service_a_order_55", "val": "CANCELLED"}, logical_time=8),
        Event(
            "assert_state_agreement",
            EventType.ASSERTION,
            "auditor",
            {"name": "DistributedStateAgreement", "expected": "AGREED", "actual": "DESYNC", "signature": "DroppedAckDesyncAnomaly"},
            logical_time=9,
            depends_on={"b_commit", "a_cancel"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_state_agreement",
        failure_signature="DroppedAckDesyncAnomaly",
        metadata={"description": "Dropped acknowledgement produces cross-service state desync"},
    )

    oracle = FailureOracle(expected_signature="DroppedAckDesyncAnomaly")
    oracle.register_invariant(
        "DroppedAckDesyncAnomaly",
        lambda state: not (
            state.get("kv_store", {}).get("service_a_order_55") == "CANCELLED"
            and state.get("kv_store", {}).get("service_b_order_55") == "COMMITTED"
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {}})
    ground_truth = {"b_commit", "fault_drop_ack", "a_cancel", "assert_state_agreement"}

    return BenchmarkCase(
        case_id="BM_B3",
        category="Benchmark B",
        name="DroppedAckStateDesync",
        description="Dropped ACK leads sender to abort while recipient maintains committed state",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_crash_restart_state_loss_case() -> BenchmarkCase:
    """
    Case B4: Service crash before fsync / WAL flush.
    Primary processes transaction, responds OK, then FAULT_CRASH occurs before fsync.
    On FAULT_RESTART, state is rolled back to old checkpoint, creating ghost transactions.
    """
    events = [
        Event("bg_diag", EventType.INTERNAL, "diagnostics", {"action": "sample"}, logical_time=1),
        Event("tx_apply", EventType.WRITE, "db_node", {"key": "account_balance", "val": 500}, logical_time=2),
        Event("fault_crash_db", EventType.FAULT_CRASH, "db_node", {"target_node": "db_node"}, logical_time=3),
        Event("fault_restart_db", EventType.FAULT_RESTART, "db_node", {"target_node": "db_node"}, logical_time=4),
        # On restart from checkpoint, balance reverts to 0
        Event("restore_chkpt", EventType.WRITE, "db_node", {"key": "account_balance", "val": 0}, logical_time=5),
        Event(
            "assert_durability",
            EventType.ASSERTION,
            "auditor",
            {"name": "DurabilityInvariant", "expected": 500, "actual": 0, "signature": "CrashRestartDurabilityLoss"},
            logical_time=6,
            depends_on={"restore_chkpt"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_durability",
        failure_signature="CrashRestartDurabilityLoss",
        metadata={"description": "Crash before disk sync causes durability loss on restart"},
    )

    oracle = FailureOracle(expected_signature="CrashRestartDurabilityLoss")
    oracle.register_invariant(
        "CrashRestartDurabilityLoss",
        lambda state: not (
            state.get("kv_store", {}).get("account_balance") == 0
            and "fault_crash_db" in state.get("executed_event_ids", set())
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"account_balance": 500}})
    ground_truth = {"fault_crash_db", "restore_chkpt", "assert_durability"}

    return BenchmarkCase(
        case_id="BM_B4",
        category="Benchmark B",
        name="CrashRestartDurabilityLoss",
        description="Unsynced crash and restart causes committed write to vanish",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def get_benchmark_b_cases() -> List[BenchmarkCase]:
    """Retrieve all Benchmark B cases."""
    return [
        create_delayed_retry_storm_case(),
        create_split_brain_partition_case(),
        create_dropped_ack_timeout_case(),
        create_crash_restart_state_loss_case(),
    ]
