"""
Benchmark Group C: Real-World Distributed Failure Patterns.
Modeled after canonical failures from SOSP 2024 (Anduril), OSDI, and production incident archives.
"""

from __future__ import annotations
from typing import Dict, List, Set

from phantom.benchmarks.base import BenchmarkCase
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler, SimulationState
from phantom.core.trace import Trace


def create_two_phase_commit_desync_case() -> BenchmarkCase:
    """
    Case C1: Two-Phase Commit (2PC) Timeout Desynchronization.
    Coordinator sends PREPARE to P1 and P2.
    P1 votes COMMIT.
    P2's vote is delayed by FAULT_DELAY.
    Coordinator abort timeout expires -> Coordinator issues GLOBAL_ABORT to P1.
    P1 aborts.
    P2 delayed vote finishes and P2 commits unilaterally due to missed abort broadcast.
    Atomicity violated (P1=ABORTED, P2=COMMITTED).
    Trace has 18 events with multiple background telemetry and heartbeat noise.
    """
    events = [
        Event("noise_mesh_1", EventType.INTERNAL, "envoy_proxy", {"action": "route_sync"}, logical_time=1),
        Event("coord_init", EventType.WRITE, "coordinator", {"key": "tx_status", "val": "PREPARE"}, logical_time=2),
        Event("send_prep_p1", EventType.SEND, "coordinator", {"msg_id": "prep_p1", "tx_id": "tx_2pc_01"}, target="p1", logical_time=3),
        Event("send_prep_p2", EventType.SEND, "coordinator", {"msg_id": "prep_p2", "tx_id": "tx_2pc_01"}, target="p2", logical_time=4),
        
        # P1 votes commit promptly
        Event("recv_prep_p1", EventType.RECEIVE, "p1", {"msg_id": "prep_p1"}, logical_time=5),
        Event("p1_vote_commit", EventType.WRITE, "p1", {"key": "p1_tx_state", "val": "VOTED_COMMIT"}, logical_time=6),
        
        # P2 is hit with fault delay
        Event("fault_delay_p2", EventType.FAULT_DELAY, "p2", {"duration": 10, "target_node": "p2"}, logical_time=7),
        Event("noise_mesh_2", EventType.INTERNAL, "envoy_proxy", {"action": "stats_dump"}, logical_time=8),
        
        # Coordinator timeout
        Event("coord_timeout", EventType.INTERNAL, "coordinator", {"action": "prepare_timeout_expired"}, logical_time=9),
        Event("coord_abort", EventType.WRITE, "coordinator", {"key": "coord_tx_state", "val": "ABORTED"}, logical_time=10),
        Event("send_abort_p1", EventType.SEND, "coordinator", {"msg_id": "abort_p1"}, target="p1", logical_time=11),
        Event("recv_abort_p1", EventType.RECEIVE, "p1", {"msg_id": "abort_p1"}, logical_time=12),
        Event("p1_do_abort", EventType.WRITE, "p1", {"key": "p1_tx_state", "val": "ABORTED"}, logical_time=13),

        # Network fault drops abort to P2
        Event("fault_drop_abort_p2", EventType.FAULT_DROP, "network", {"msg_id": "abort_p2"}, logical_time=14),

        # P2 wakes up from delay and commits
        Event("p2_do_commit", EventType.WRITE, "p2", {"key": "p2_tx_state", "val": "COMMITTED"}, logical_time=15),
        Event("noise_metric", EventType.INTERNAL, "datadog", {"action": "push"}, logical_time=16),

        Event(
            "assert_2pc_atomicity",
            EventType.ASSERTION,
            "consistency_auditor",
            {"name": "AtomicCommitAgreement", "expected": "ALL_ABORTED", "actual": "MIXED", "signature": "TwoPhaseCommitDesyncAnomaly"},
            logical_time=17,
            depends_on={"p1_do_abort", "p2_do_commit"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_2pc_atomicity",
        failure_signature="TwoPhaseCommitDesyncAnomaly",
        metadata={"description": "2PC coordinator timeout and dropped abort results in split commit/abort"},
    )

    oracle = FailureOracle(expected_signature="TwoPhaseCommitDesyncAnomaly")
    oracle.register_invariant(
        "TwoPhaseCommitDesyncAnomaly",
        lambda state: not (
            state.get("kv_store", {}).get("p1_tx_state") == "ABORTED"
            and state.get("kv_store", {}).get("p2_tx_state") == "COMMITTED"
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {}})
    ground_truth = {"p1_do_abort", "p2_do_commit", "assert_2pc_atomicity"}

    return BenchmarkCase(
        case_id="BM_C1",
        category="Benchmark C",
        name="TwoPhaseCommitTimeoutDesync",
        description="Coordinator timeout and dropped participant notification breaks atomic commitment",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_distributed_lease_race_case() -> BenchmarkCase:
    """
    Case C2: Distributed lease expiration race (ZooKeeper / Raft style).
    Node 1 holds active lease until T=10.
    Node 1 experiences garbage-collection pause (FAULT_DELAY).
    T=10 expires on Node 2 -> Node 2 claims leadership and writes state.
    Node 1 resumes and performs stale leader write before checking lease validity.
    """
    events = [
        Event("noise_bg_1", EventType.INTERNAL, "zk_client", {"action": "noop"}, logical_time=1),
        Event("n1_acquire_lease", EventType.WRITE, "node_1", {"key": "lease_holder", "val": "node_1"}, logical_time=2),
        Event("fault_gc_pause_n1", EventType.FAULT_DELAY, "node_1", {"duration": 15, "target_node": "node_1"}, logical_time=3),
        Event("noise_bg_2", EventType.INTERNAL, "syslog", {"action": "gc_log"}, logical_time=4),
        Event("n2_lease_timeout", EventType.INTERNAL, "node_2", {"action": "lease_expired"}, logical_time=5),
        Event("n2_claim_lease", EventType.WRITE, "node_2", {"key": "lease_holder", "val": "node_2"}, logical_time=6),
        Event("n2_write_data", EventType.WRITE, "node_2", {"key": "cluster_config", "val": "version_2"}, logical_time=7),
        # Node 1 resumes and writes without re-checking lease!
        Event("n1_stale_write", EventType.WRITE, "node_1", {"key": "cluster_config", "val": "stale_version_1"}, logical_time=8),
        Event(
            "assert_linearizability",
            EventType.ASSERTION,
            "etcd_verifier",
            {"name": "StrictLinearizableOrder", "expected": "version_2", "actual": "stale_version_1", "signature": "StaleLeaderLeaseOverwrite"},
            logical_time=9,
            depends_on={"n2_write_data", "n1_stale_write"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_linearizability",
        failure_signature="StaleLeaderLeaseOverwrite",
        metadata={"description": "Stale leader write after lease expiration overwrites new leader config"},
    )

    oracle = FailureOracle(expected_signature="StaleLeaderLeaseOverwrite")
    oracle.register_invariant(
        "StaleLeaderLeaseOverwrite",
        lambda state: not (
            state.get("kv_store", {}).get("lease_holder") == "node_2"
            and state.get("kv_store", {}).get("cluster_config") == "stale_version_1"
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {}})
    ground_truth = {"n2_claim_lease", "n1_stale_write", "assert_linearizability"}

    return BenchmarkCase(
        case_id="BM_C2",
        category="Benchmark C",
        name="DistributedLeaseRenewalRace",
        description="GC pause past lease timeout allows old leader to overwrite new leader's committed write",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_saga_compensate_race_case() -> BenchmarkCase:
    """
    Case C3: Microservice Saga Compensation Race.
    Order cancellation compensation arrives at Payment Gateway before authorization message.
    1. Order Service dispatches Auth Payment.
    2. Inventory fails, Order Service immediately dispatches Cancel Payment.
    3. Cancel Payment arrives first; rejected because payment not yet in DB.
    4. Delayed Auth Payment arrives and charges customer anyway.
    """
    events = [
        Event("noise_trace", EventType.INTERNAL, "jaeger", {"action": "span_export"}, logical_time=1),
        Event("send_auth", EventType.SEND, "order_service", {"msg_id": "auth_tx_9", "action": "auth"}, target="payment_gw", logical_time=2),
        Event("fault_delay_auth", EventType.FAULT_DELAY, "payment_gw", {"duration": 5, "target_node": "payment_gw"}, logical_time=3),
        Event("send_cancel", EventType.SEND, "order_service", {"msg_id": "cancel_tx_9", "action": "cancel"}, target="payment_gw", logical_time=4),
        Event("noise_kafka", EventType.INTERNAL, "kafka", {"action": "commit_offset"}, logical_time=5),
        # Cancel arrives first:
        Event("recv_cancel", EventType.RECEIVE, "payment_gw", {"msg_id": "cancel_tx_9"}, logical_time=6),
        Event("reject_cancel", EventType.WRITE, "payment_gw", {"key": "tx_9_cancel_status", "val": "REJECTED_UNKNOWN_TX"}, logical_time=7),
        # Delayed auth arrives second:
        Event("recv_auth", EventType.RECEIVE, "payment_gw", {"msg_id": "auth_tx_9"}, logical_time=8),
        Event("charge_customer", EventType.WRITE, "payment_gw", {"key": "tx_9_billed", "val": True}, logical_time=9),
        Event(
            "assert_no_ghost_charge",
            EventType.ASSERTION,
            "accounting",
            {"name": "NoGhostChargeInvariant", "expected": False, "actual": True, "signature": "SagaCompensationReorderGhostBilling"},
            logical_time=10,
            depends_on={"charge_customer", "reject_cancel"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_no_ghost_charge",
        failure_signature="SagaCompensationReorderGhostBilling",
        metadata={"description": "Compensation reordering causes ghost billing after order cancellation"},
    )

    oracle = FailureOracle(expected_signature="SagaCompensationReorderGhostBilling")
    oracle.register_invariant(
        "SagaCompensationReorderGhostBilling",
        lambda state: not (
            state.get("kv_store", {}).get("tx_9_cancel_status") == "REJECTED_UNKNOWN_TX"
            and state.get("kv_store", {}).get("tx_9_billed") is True
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {}})
    ground_truth = {"reject_cancel", "charge_customer", "assert_no_ghost_charge"}

    return BenchmarkCase(
        case_id="BM_C3",
        category="Benchmark C",
        name="SagaCompensationRace",
        description="Out-of-order compensation arrival causes ghost billing on cancelled transaction",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_raft_log_truncation_case() -> BenchmarkCase:
    """
    Case C4: Raft log truncation and uncommitted entry divergence.
    Old leader proposes entry at index 5 with Term 1, but is delayed by network fault.
    New leader elected in Term 2 overwrites index 5.
    Old leader resumes and observes state violation.
    """
    events = [
        Event("bg_heartbeat", EventType.INTERNAL, "raft_monitor", {"action": "hb_tick"}, logical_time=1),
        Event("n1_propose_idx5", EventType.WRITE, "node_1", {"key": "raft_idx_5", "val": "tx_term1"}, logical_time=2),
        Event("fault_delay_n1", EventType.FAULT_DELAY, "node_1", {"duration": 8, "target_node": "node_1"}, logical_time=3),
        Event("bg_raft_snap", EventType.INTERNAL, "node_2", {"action": "take_snapshot"}, logical_time=4),
        Event("n2_elect_term2", EventType.WRITE, "node_2", {"key": "current_term", "val": 2}, logical_time=5),
        Event("n2_overwrite_idx5", EventType.WRITE, "node_2", {"key": "raft_idx_5", "val": "tx_term2"}, logical_time=6),
        Event("n1_resume", EventType.INTERNAL, "node_1", {"action": "wake_from_delay"}, logical_time=7),
        Event(
            "assert_raft_log_agreement",
            EventType.ASSERTION,
            "auditor",
            {"name": "RaftLogAgreement", "expected": "tx_term2", "actual": "tx_term1", "signature": "RaftLogDivergenceAnomaly"},
            logical_time=8,
            depends_on={"n1_propose_idx5", "n2_overwrite_idx5"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_raft_log_agreement",
        failure_signature="RaftLogDivergenceAnomaly",
        metadata={"description": "Delayed leader entry overwritten by new leader term election"},
    )

    oracle = FailureOracle(expected_signature="RaftLogDivergenceAnomaly")
    oracle.register_invariant(
        "RaftLogDivergenceAnomaly",
        lambda state: not (
            state.get("kv_store", {}).get("raft_idx_5") == "tx_term2"
            and state.get("kv_store", {}).get("current_term") == 2
            and "fault_delay_n1" in state.get("executed_event_ids", set())
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"current_term": 1}})
    ground_truth = {"fault_delay_n1", "n2_elect_term2", "n2_overwrite_idx5", "assert_raft_log_agreement"}

    return BenchmarkCase(
        case_id="BM_C4",
        category="Benchmark C",
        name="RaftLogTruncationDivergence",
        description="Term change truncates uncommitted log entries, inducing state divergence",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_kafka_offset_rewind_case() -> BenchmarkCase:
    """
    Case C5: Message streaming crash before commit offset rewind.
    Consumer processes message and charges card, but crashes before commit offset.
    On restart, consumer rewinds and double-charges transaction.
    """
    events = [
        Event("bg_poll_stream", EventType.INTERNAL, "kafka-broker", {"action": "poll"}, logical_time=1),
        Event("consumer_read_msg", EventType.READ, "consumer_worker", {"key": "stream_offset", "val": 100}, logical_time=2),
        Event("consumer_exec_charge", EventType.WRITE, "consumer_worker", {"key": "charge_count_msg101", "val": 1}, logical_time=3),
        Event("fault_crash_consumer", EventType.FAULT_CRASH, "consumer_worker", {"target_node": "consumer_worker"}, logical_time=4),
        Event("fault_restart_consumer", EventType.FAULT_RESTART, "consumer_worker", {"target_node": "consumer_worker"}, logical_time=5),
        # Rewind to uncommitted offset 100
        Event("consumer_rewind_read", EventType.READ, "consumer_worker", {"key": "stream_offset", "val": 100}, logical_time=6),
        Event("consumer_re_execute", EventType.WRITE, "consumer_worker", {"key": "charge_count_msg101", "val": 2}, logical_time=7),
        Event(
            "assert_exactly_once",
            EventType.ASSERTION,
            "auditor",
            {"name": "ExactlyOnceProcessing", "expected": 1, "actual": 2, "signature": "KafkaOffsetRewindDuplicateAnomaly"},
            logical_time=8,
            depends_on={"consumer_re_execute"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_exactly_once",
        failure_signature="KafkaOffsetRewindDuplicateAnomaly",
        metadata={"description": "Consumer crash before offset commit causes rewind duplicate processing"},
    )

    oracle = FailureOracle(expected_signature="KafkaOffsetRewindDuplicateAnomaly")
    oracle.register_invariant(
        "KafkaOffsetRewindDuplicateAnomaly",
        lambda state: state.get("kv_store", {}).get("charge_count_msg101", 0) <= 1,
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"stream_offset": 100, "charge_count_msg101": 0}})
    ground_truth = {"fault_crash_consumer", "consumer_re_execute", "assert_exactly_once"}

    return BenchmarkCase(
        case_id="BM_C5",
        category="Benchmark C",
        name="KafkaConsumerOffsetRewind",
        description="Stream consumer crash before offset commit triggers rewind duplicate execution",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def get_benchmark_c_cases() -> List[BenchmarkCase]:
    """Retrieve all Benchmark C cases."""
    return [
        create_two_phase_commit_desync_case(),
        create_distributed_lease_race_case(),
        create_saga_compensate_race_case(),
        create_raft_log_truncation_case(),
        create_kafka_offset_rewind_case(),
    ]
