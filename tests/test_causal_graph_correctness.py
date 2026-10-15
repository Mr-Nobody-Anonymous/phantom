"""
Tests for CausalGraph correctness improvements:
- Multi-store state isolation
- Version/tx-aware data flow
- Unique message identity validation & duplicate detection
- Ambiguous message correlation
- Cycle diagnostics, root cause explanation, and strict vs permissive modes
- Reachability caching performance
"""

import pytest
from phantom.core.event import Event, EventType, VectorClock
from phantom.core.graph import (
    AmbiguousMessageError,
    CausalGraph,
    CausalCycleError,
    CausalInconsistencyError,
)
from phantom.core.trace import Trace


def test_multi_store_isolation():
    """Reads must connect only to writes within the same state store / replica."""
    events = [
        # Write to database A
        Event("w_dbA", EventType.WRITE, "node1", {"store": "dbA", "key": "balance", "val": 100}, logical_time=1),
        # Write to database B
        Event("w_dbB", EventType.WRITE, "node2", {"store": "dbB", "key": "balance", "val": 999}, logical_time=2),
        # Read from database A
        Event("r_dbA", EventType.READ, "node3", {"store": "dbA", "key": "balance", "val": 100}, logical_time=3),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    # Data-flow edge must connect w_dbA -> r_dbA, NOT w_dbB -> r_dbA
    assert graph.graph.has_edge("w_dbA", "r_dbA")
    assert not graph.graph.has_edge("w_dbB", "r_dbA")


def test_version_aware_data_flow():
    """Reads specifying observed version must connect to the specific write with that version."""
    events = [
        Event("w_v1", EventType.WRITE, "node1", {"key": "x", "val": 10, "version": "v1"}, logical_time=1),
        Event("w_v2", EventType.WRITE, "node1", {"key": "x", "val": 20, "version": "v2"}, logical_time=2),
        # Read specifically observed version v1 (e.g. from a replica lag snapshot)
        Event("r_v1", EventType.READ, "node2", {"key": "x", "read_version": "v1"}, logical_time=3),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    assert graph.graph.has_edge("w_v1", "r_v1")
    assert not graph.graph.has_edge("w_v2", "r_v1")


def test_duplicate_message_id_detection():
    """Duplicate sends sharing the same msg_id must be flagged in diagnostics."""
    events = [
        Event("send1", EventType.SEND, "nodeA", {"msg_id": "msg_dup_1"}, target="nodeB", logical_time=1),
        Event("send2", EventType.SEND, "nodeC", {"msg_id": "msg_dup_1"}, target="nodeB", logical_time=2),
        Event("recv1", EventType.RECEIVE, "nodeB", {"msg_id": "msg_dup_1"}, logical_time=3),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace, strict_mode=False)

    # Duplicate message must be recorded in diagnostics
    assert len(graph.diagnostics.duplicate_messages) > 0
    assert graph.diagnostics.duplicate_messages[0]["msg_id"] == "msg_dup_1"
    assert graph.is_uncertain is True


def test_unmatched_message_receive_reported():
    """Receive events referencing a non-existent send must be reported in diagnostics."""
    events = [
        Event("recv_orphan", EventType.RECEIVE, "nodeB", {"msg_id": "ghost_msg_404"}, logical_time=1),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    assert len(graph.diagnostics.unmatched_messages) == 1
    assert graph.diagnostics.unmatched_messages[0]["msg_id"] == "ghost_msg_404"


def test_cycle_detection_strict_mode_rejects_and_explains():
    """Strict mode must reject cyclic traces and diagnose the root cause."""
    # Create circular explicit dependency
    events = [
        Event("c1", EventType.INTERNAL, "nodeA", logical_time=1, depends_on={"c2"}),
        Event("c2", EventType.INTERNAL, "nodeA", logical_time=2, depends_on={"c1"}),
    ]
    trace = Trace(events=events)

    with pytest.raises(CausalCycleError) as exc_info:
        CausalGraph(trace, strict_mode=True)

    err_msg = str(exc_info.value)
    assert "Causal cycle detected" in err_msg
    assert "Likely cause" in err_msg


def test_cycle_detection_permissive_mode_repairs_and_marks_uncertain():
    """Permissive mode repairs cycles but marks graph as uncertain with reasons."""
    events = [
        Event("c1", EventType.INTERNAL, "nodeA", logical_time=1, depends_on={"c2"}),
        Event("c2", EventType.INTERNAL, "nodeA", logical_time=2, depends_on={"c1"}),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace, strict_mode=False)

    assert graph.stats()["is_dag"] is True
    assert graph.is_uncertain is True
    assert len(graph.uncertainty_reasons) > 0
    assert "Causal cycle detected" in graph.uncertainty_reasons[0]


def test_reachability_cache_and_conflicting_pairs():
    """Reachability caching accelerates conflict detection across concurrent writes."""
    events = [
        Event("w1", EventType.WRITE, "nodeA", {"key": "alpha", "val": 1}, logical_time=1),
        Event("w2", EventType.WRITE, "nodeB", {"key": "alpha", "val": 2}, logical_time=2),
        Event("w3", EventType.WRITE, "nodeC", {"key": "alpha", "val": 3}, logical_time=3),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace, enable_cache=True)

    conflicts = graph.find_conflicting_pairs()
    # 3 pairwise concurrent writes: (w1, w2), (w1, w3), (w2, w3) = 3 conflicts
    assert len(conflicts) == 3
