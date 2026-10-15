import pytest
from phantom.core.oracle import FailureOracle, OracleVerdict

def test_oracle_pass_verdict():
    oracle = FailureOracle(expected_signature="DoubleSpend")
    oracle.register_invariant("PositiveBalance", lambda state: state.get("balance", 10) > 0)

    res = oracle.evaluate({"balance": 100})
    assert res.verdict == OracleVerdict.PASS
    assert res.is_pass

def test_oracle_reproduced_verdict():
    oracle = FailureOracle(expected_signature="DoubleSpend")
    oracle.register_invariant("DoubleSpendInvariant", lambda state: state.get("balance", 10) >= 0)

    res = oracle.evaluate({"balance": -50})
    assert res.verdict == OracleVerdict.REPRODUCED
    assert res.is_reproduced
    assert "DoubleSpend" in res.violated_invariant

def test_oracle_different_failure_verdict():
    oracle = FailureOracle(expected_signature="DoubleSpend")
    oracle.register_invariant("DiskFull", lambda state: state.get("disk_free", 100) > 10)

    res = oracle.evaluate({"disk_free": 0})
    assert res.verdict == OracleVerdict.DIFFERENT_FAILURE
    assert not res.is_reproduced
