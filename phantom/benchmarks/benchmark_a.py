"""
Benchmark Group A: Controlled Concurrency Bugs.
Covers race conditions, duplicate operations, stale reads, lost updates, and ordering violations.
"""

from __future__ import annotations
from typing import Dict, List, Set

from phantom.benchmarks.base import BenchmarkCase
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler, SimulationState
from phantom.core.trace import Trace


def create_duplicate_order_case() -> BenchmarkCase:
    """
    Case A1: Duplicate order processing.
    Request A reads order status 'pending', Request B reads 'pending',
    both proceed to commit payment, resulting in duplicate payment.
    Trace contains 12 events (including 6 background telemetry/heartbeat events).
    """
    events = [
        # Background telemetry noise
        Event("bg_1", EventType.INTERNAL, "metrics-agent", {"action": "cpu_report"}, logical_time=1),
        Event("bg_2", EventType.SEND, "health-check", {"msg_id": "ping_1", "action": "ping"}, target="order-svc", logical_time=2),
        Event("bg_3", EventType.RECEIVE, "order-svc", {"msg_id": "ping_1", "action": "pong"}, logical_time=3),

        # Core bug events
        Event("evt_req_a_recv", EventType.RECEIVE, "order-svc", {"msg_id": "order_req_a", "order_id": "ord_99"}, logical_time=4),
        Event("evt_read_a", EventType.READ, "order-svc", {"key": "ord_99_status", "val": "pending"}, logical_time=5),
        
        # Concurrent Request B
        Event("evt_req_b_recv", EventType.RECEIVE, "order-svc", {"msg_id": "order_req_b", "order_id": "ord_99"}, logical_time=6),
        Event("evt_read_b", EventType.READ, "order-svc", {"key": "ord_99_status", "val": "pending"}, logical_time=7),

        # Noise
        Event("bg_4", EventType.INTERNAL, "log-daemon", {"action": "flush_buffer"}, logical_time=8),

        # Commits
        Event("evt_commit_b", EventType.WRITE, "payment-svc", {"key": "ord_99_charged", "val": True, "by": "req_b"}, logical_time=9),
        Event("evt_commit_a", EventType.WRITE, "payment-svc", {"key": "ord_99_charged_dup", "val": True, "by": "req_a"}, logical_time=10),

        # Noise
        Event("bg_5", EventType.INTERNAL, "metrics-agent", {"action": "mem_report"}, logical_time=11),

        # Failure assertion
        Event(
            "evt_assert_dup",
            EventType.ASSERTION,
            "order-svc",
            {"name": "NoDuplicateProcessing", "expected": False, "actual": True, "signature": "DuplicateOrderProcessing"},
            logical_time=12,
            depends_on={"evt_commit_b", "evt_commit_a"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="evt_assert_dup",
        failure_signature="DuplicateOrderProcessing",
        metadata={"description": "Duplicate order charge under concurrent requests"},
    )

    oracle = FailureOracle(expected_signature="DuplicateOrderProcessing")
    oracle.register_invariant(
        "DuplicateOrderProcessing",
        lambda state: not (
            state.get("kv_store", {}).get("ord_99_charged") is True
            and state.get("kv_store", {}).get("ord_99_charged_dup") is True
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"ord_99_status": "pending"}})

    ground_truth = {"evt_read_a", "evt_read_b", "evt_commit_b", "evt_commit_a", "evt_assert_dup"}

    return BenchmarkCase(
        case_id="BM_A1",
        category="Benchmark A",
        name="DuplicateOrderProcessing",
        description="Two concurrent requests both observe pending status and double-charge payment",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_double_spend_case() -> BenchmarkCase:
    """
    Case A2: Double spend race condition on shared wallet balance.
    Balance = 100. Tx1 reads 100. Tx2 reads 100.
    Tx1 deducts 70 (writes 30). Tx2 deducts 70 (writes -40).
    Trace contains 14 events with 7 noisy background events.
    """
    events = [
        Event("bg_0", EventType.INTERNAL, "wallet-svc", {"action": "warmup"}, logical_time=1),
        Event("bg_1", EventType.SEND, "cron", {"msg_id": "cron_sync"}, target="wallet-svc", logical_time=2),
        
        Event("tx1_read", EventType.READ, "tx-worker-1", {"key": "user_bal", "val": 100}, logical_time=3),
        Event("tx2_read", EventType.READ, "tx-worker-2", {"key": "user_bal", "val": 100}, logical_time=4),
        
        Event("bg_2", EventType.INTERNAL, "telemetry", {"action": "emit"}, logical_time=5),
        
        Event("tx1_deduct", EventType.WRITE, "tx-worker-1", {"key": "user_bal", "val": 30}, logical_time=6),
        Event("tx2_deduct", EventType.WRITE, "tx-worker-2", {"key": "user_bal", "val": -40}, logical_time=7),

        Event("bg_3", EventType.INTERNAL, "logger", {"action": "sync"}, logical_time=8),
        Event("bg_4", EventType.INTERNAL, "audit", {"action": "periodic_tick"}, logical_time=9),

        Event(
            "assert_bal",
            EventType.ASSERTION,
            "audit",
            {"name": "NonNegativeBalance", "expected": True, "actual": False, "signature": "NegativeBalanceDoubleSpend"},
            logical_time=10,
            depends_on={"tx2_deduct"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_bal",
        failure_signature="NegativeBalanceDoubleSpend",
        metadata={"description": "Concurrent debit without atomic lock results in negative balance"},
    )

    oracle = FailureOracle(expected_signature="NegativeBalanceDoubleSpend")
    oracle.register_invariant(
        "NegativeBalanceDoubleSpend",
        lambda state: state.get("kv_store", {}).get("user_bal", 100) >= 0,
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"user_bal": 100}})
    ground_truth = {"tx1_read", "tx2_read", "tx1_deduct", "tx2_deduct", "assert_bal"}

    return BenchmarkCase(
        case_id="BM_A2",
        category="Benchmark A",
        name="DoubleSpendRace",
        description="Unsynchronized concurrent debit transactions drain balance into negative amount",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_stale_cache_read_case() -> BenchmarkCase:
    """
    Case A3: Cache-aside stale read race.
    1. Worker 1 reads old value 'v1' from DB.
    2. Worker 2 updates DB to 'v2' and invalidates cache.
    3. Worker 1 populates cache with stale 'v1'.
    Result: Cache contains stale 'v1' while DB has 'v2'.
    """
    events = [
        Event("bg_1", EventType.INTERNAL, "cache-svc", {"action": "memory_gc"}, logical_time=1),
        Event("w1_read_db", EventType.READ, "worker-1", {"key": "db_user_email", "val": "old@test.com"}, logical_time=2),
        Event("w2_write_db", EventType.WRITE, "worker-2", {"key": "db_user_email", "val": "new@test.com"}, logical_time=3),
        Event("w2_inval_cache", EventType.WRITE, "worker-2", {"key": "cache_user_email", "val": None}, logical_time=4),
        Event("bg_2", EventType.INTERNAL, "worker-3", {"action": "heartbeat"}, logical_time=5),
        Event("w1_write_cache_stale", EventType.WRITE, "worker-1", {"key": "cache_user_email", "val": "old@test.com"}, logical_time=6),
        Event("bg_3", EventType.INTERNAL, "analytics", {"action": "flush"}, logical_time=7),
        Event(
            "assert_cache_consistency",
            EventType.ASSERTION,
            "verifier",
            {"name": "CacheDBConsistency", "expected": "new@test.com", "actual": "old@test.com", "signature": "StaleCacheInconsistency"},
            logical_time=8,
            depends_on={"w1_write_cache_stale", "w2_write_db"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_cache_consistency",
        failure_signature="StaleCacheInconsistency",
        metadata={"description": "Cache-aside invalidation race condition"},
    )

    oracle = FailureOracle(expected_signature="StaleCacheInconsistency")
    oracle.register_invariant(
        "StaleCacheInconsistency",
        lambda state: not (
            state.get("kv_store", {}).get("db_user_email") == "new@test.com"
            and state.get("kv_store", {}).get("cache_user_email") == "old@test.com"
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"db_user_email": "old@test.com", "cache_user_email": "old@test.com"}})
    ground_truth = {"w1_read_db", "w2_write_db", "w1_write_cache_stale", "assert_cache_consistency"}

    return BenchmarkCase(
        case_id="BM_A3",
        category="Benchmark A",
        name="StaleCacheInconsistency",
        description="Worker overwrites freshly invalidated cache with stale snapshot",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def create_lost_update_counter_case() -> BenchmarkCase:
    """
    Case A4: Lost update counter race.
    Two workers read counter=10, compute +1, and both write counter=11.
    Expected counter=12, actual counter=11.
    """
    events = [
        Event("bg_1", EventType.INTERNAL, "stat-collector", {"action": "poll"}, logical_time=1),
        Event("c1_read", EventType.READ, "worker-1", {"key": "click_count", "val": 10}, logical_time=2),
        Event("c2_read", EventType.READ, "worker-2", {"key": "click_count", "val": 10}, logical_time=3),
        Event("bg_2", EventType.INTERNAL, "stat-collector", {"action": "sample"}, logical_time=4),
        Event("c1_write", EventType.WRITE, "worker-1", {"key": "click_count", "val": 11}, logical_time=5),
        Event("c2_write", EventType.WRITE, "worker-2", {"key": "click_count", "val": 11}, logical_time=6),
        Event("bg_3", EventType.INTERNAL, "logger", {"action": "dump"}, logical_time=7),
        Event(
            "assert_counter",
            EventType.ASSERTION,
            "checker",
            {"name": "AtomicCounterUpdate", "expected": 12, "actual": 11, "signature": "LostUpdateCounterAnomaly"},
            logical_time=8,
            depends_on={"c1_write", "c2_write"},
        ),
    ]

    trace = Trace(
        events=events,
        failure_event_id="assert_counter",
        failure_signature="LostUpdateCounterAnomaly",
        metadata={"description": "Concurrent read-modify-write without version check"},
    )

    oracle = FailureOracle(expected_signature="LostUpdateCounterAnomaly")
    oracle.register_invariant(
        "LostUpdateCounterAnomaly",
        lambda state: not (
            "c1_write" in state.get("executed_event_ids", set())
            and "c2_write" in state.get("executed_event_ids", set())
            and state.get("kv_store", {}).get("click_count") == 11
        ),
    )

    scheduler = EventScheduler(initial_state={"kv_store": {"click_count": 10}})
    ground_truth = {"c1_read", "c2_read", "c1_write", "c2_write", "assert_counter"}

    return BenchmarkCase(
        case_id="BM_A4",
        category="Benchmark A",
        name="LostUpdateCounterAnomaly",
        description="Unversioned concurrent increments collide resulting in lost update",
        trace=trace,
        oracle=oracle,
        scheduler=scheduler,
        ground_truth_minimal_events=ground_truth,
    )


def get_benchmark_a_cases() -> List[BenchmarkCase]:
    """Retrieve all Benchmark A cases."""
    return [
        create_duplicate_order_case(),
        create_double_spend_case(),
        create_stale_cache_read_case(),
        create_lost_update_counter_case(),
    ]
