import pytest
from phantom.core.event import VectorClock

def test_vector_clock_basic_operations():
    vc1 = VectorClock()
    vc1.increment("nodeA")
    assert vc1.get("nodeA") == 1
    assert vc1.get("nodeB") == 0

    vc2 = vc1.clone()
    assert vc2 == vc1
    vc2.increment("nodeB")
    assert vc2.get("nodeB") == 1

    # vc1 < vc2
    assert vc1.happens_before(vc2)
    assert not vc2.happens_before(vc1)
    assert not vc1.concurrent_with(vc2)

def test_vector_clock_concurrency():
    vc1 = VectorClock({"nodeA": 2, "nodeB": 0})
    vc2 = VectorClock({"nodeA": 0, "nodeB": 2})

    assert not vc1.happens_before(vc2)
    assert not vc2.happens_before(vc1)
    assert vc1.concurrent_with(vc2)

def test_vector_clock_merge():
    vc1 = VectorClock({"nodeA": 2, "nodeB": 1})
    vc2 = VectorClock({"nodeA": 1, "nodeB": 3, "nodeC": 4})
    vc1.update(vc2)

    assert vc1.get("nodeA") == 2
    assert vc1.get("nodeB") == 3
    assert vc1.get("nodeC") == 4

def test_vector_clock_serialization():
    data = {"nodeA": 5, "nodeB": 10}
    vc = VectorClock.from_dict(data)
    assert vc.to_dict() == data
