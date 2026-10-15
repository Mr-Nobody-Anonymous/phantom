import pytest
from phantom.core.event import Event, EventType
from phantom.core.oracle import FailureOracle, OracleVerdict
from phantom.core.scheduler import EventScheduler
from phantom.core.trace import Trace

def test_scheduler_state_write_read():
    events = [
        Event("w1", EventType.WRITE, "node1", {"key": "alpha", "val": 42}),
        Event("r1", EventType.READ, "node2", {"key": "alpha"}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    assert res.verdict == OracleVerdict.PASS
    assert res.observed_state["kv_store"]["alpha"] == 42
    assert res.observed_state["custom_state"]["last_read_alpha_node2"] == 42

def test_scheduler_message_send_and_drop():
    events = [
        Event("send1", EventType.SEND, "client", {"msg_id": "m99"}, target="server"),
        Event("drop1", EventType.FAULT_DROP, "network", {"msg_id": "m99"}),
        Event("recv1", EventType.RECEIVE, "server", {"msg_id": "m99"}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    assert "m99" in res.observed_state["dropped_messages"]
    assert any("dropped" in log for log in res.observed_state["logs"])

def test_scheduler_node_crash_and_restart():
    events = [
        Event("w1", EventType.WRITE, "nodeA", {"key": "k1", "val": 100}),
        Event("crash1", EventType.FAULT_CRASH, "nodeA", {"target_node": "nodeA"}),
        # Event w2 occurs while nodeA is crashed -> should be skipped!
        Event("w2", EventType.WRITE, "nodeA", {"key": "k1", "val": 200}),
        Event("restart1", EventType.FAULT_RESTART, "nodeA", {"target_node": "nodeA"}),
        Event("w3", EventType.WRITE, "nodeA", {"key": "k1", "val": 300}),
    ]
    trace = Trace(events=events)
    scheduler = EventScheduler()
    oracle = FailureOracle(expected_signature="None")
    res = scheduler.replay(trace, oracle)

    assert res.observed_state["kv_store"]["k1"] == 300
    assert any("Skipping event w2" in log for log in res.observed_state["logs"])
