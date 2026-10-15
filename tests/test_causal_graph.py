import pytest
from phantom.core.event import Event, EventType, VectorClock
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

def test_causal_graph_vector_clock_integration():
    vc1 = VectorClock(clock={"nodeA": 1})
    vc2 = VectorClock(clock={"nodeA": 1, "nodeB": 1})

    events = [
        Event("vc_e1", EventType.INTERNAL, "nodeA", vector_clock=vc1, logical_time=1),
        Event("vc_e2", EventType.INTERNAL, "nodeB", vector_clock=vc2, logical_time=2),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    # Vector clock precedence vc1 < vc2 should generate causal edge vc_e1 -> vc_e2
    assert graph.graph.has_edge("vc_e1", "vc_e2")
    assert not graph.are_concurrent("vc_e1", "vc_e2")

def test_causal_graph_cycle_resolution():
    # Construct corrupt events with circular explicit dependencies
    events = [
        Event("c_e1", EventType.INTERNAL, "nodeA", logical_time=1, depends_on={"c_e2"}),
        Event("c_e2", EventType.INTERNAL, "nodeA", logical_time=2, depends_on={"c_e1"}),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    # Graph construction should resolve cycles heuristically and remain a valid DAG
    assert graph.stats()["is_dag"] is True
    order = graph.topological_order()
    assert len(order) == 2

def test_causal_closure_check():
    events = [
        Event("p1", EventType.INTERNAL, "nodeA", logical_time=1),
        Event("p2", EventType.INTERNAL, "nodeA", logical_time=2, depends_on={"p1"}),
    ]
    trace = Trace(events=events)
    graph = CausalGraph(trace)

    # {p1, p2} is closed
    assert graph.is_causally_closed({"p1", "p2"}) is True
    # {p2} alone without predecessor p1 is not closed
    assert graph.is_causally_closed({"p2"}) is False

def test_trace_validation():
    valid_trace = Trace(events=[Event("e1", EventType.INTERNAL, "nodeA")])
    is_valid, err = valid_trace.validate()
    assert is_valid is True
    assert err is None

    dup_trace = Trace(events=[Event("e1", EventType.INTERNAL, "nodeA"), Event("e1", EventType.INTERNAL, "nodeB")])
    is_valid, err = dup_trace.validate()
    assert is_valid is False
    assert "Duplicate" in err
