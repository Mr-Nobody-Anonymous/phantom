import pytest
from phantom.core.event import Event, EventType
from phantom.core.graph import CausalGraph
from phantom.core.trace import Trace

def test_causal_graph_construction_and_cone():
    events = [
        Event("e1", EventType.INTERNAL, "nodeA", logical_time=1),
        Event("e2", EventType.SEND, "nodeA", {"msg_id": "m1"}, target="nodeB", logical_time=2),
        Event("e3", EventType.INTERNAL, "nodeC", {"note": "unrelated"}, logical_time=3),
        Event("e4", EventType.RECEIVE, "nodeB", {"msg_id": "m1"}, logical_time=4),
        Event("e5", EventType.ASSERTION, "nodeB", {"name": "TestAssert"}, logical_time=5, depends_on={"e4"}),
    ]
    trace = Trace(events=events, failure_event_id="e5", failure_signature="TestAssert")
    graph = CausalGraph(trace)

    # Causal cone for e5 should include e1, e2, e4, e5
    # e3 is unrelated background event on nodeC and must NOT be in the causal cone!
    cone = graph.causal_cone("e5")
    assert "e5" in cone
    assert "e4" in cone
    assert "e2" in cone
    assert "e1" in cone
    assert "e3" not in cone

def test_causal_graph_concurrency_and_conflicts():
    events = [
        Event("r1", EventType.READ, "nodeA", {"key": "balance", "val": 100}, logical_time=1),
        Event("w1", EventType.WRITE, "nodeB", {"key": "balance", "val": 50}, logical_time=2),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    # r1 and w1 have no edges between them -> concurrent conflict!
    assert graph.are_concurrent("r1", "w1")
    conflicts = graph.find_conflicting_pairs()
    assert len(conflicts) == 1
    assert {conflicts[0][0].event_id, conflicts[0][1].event_id} == {"r1", "w1"}

def test_causal_topological_sort():
    events = [
        Event("e1", EventType.SEND, "nodeA", {"msg_id": "m1"}, target="nodeB", logical_time=1),
        Event("e2", EventType.RECEIVE, "nodeB", {"msg_id": "m1"}, logical_time=2),
        Event("e3", EventType.WRITE, "nodeB", {"key": "k", "val": "v"}, logical_time=3),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)
    order = graph.topological_order()
    assert order.index("e1") < order.index("e2")
    assert order.index("e2") < order.index("e3")
