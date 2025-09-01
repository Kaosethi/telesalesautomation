import os
from telesales import non_a, tier_a

def test_non_a_holiday(monkeypatch):
    monkeypatch.setenv("RUN_DATE", "2025-09-01")
    res = non_a.pipeline.run()
    assert res.row_count == 0

def test_tier_a_holiday(monkeypatch):
    monkeypatch.setenv("RUN_DATE", "2025-09-01")
    res = tier_a.pipeline.run()
    assert res.row_count == 0
