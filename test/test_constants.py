import pytest   
from telesales import constants as c


def test_is_tier_a_true_cases():
    assert c.is_tier_a("A-1")
    assert c.is_tier_a("A-2 Gold")
    assert c.is_tier_a("a-3")    # case insensitive


def test_is_tier_a_false_cases():
    assert not c.is_tier_a(None)
    assert not c.is_tier_a("")
    assert not c.is_tier_a("B-1")
    assert not c.is_tier_a("Tier A")   # must start with "A-"


def test_headers_schema_guard():
    # Tier A headers
    assert len(c.TIER_A_HEADERS) == 9, "Tier A headers count changed unexpectedly"
    assert len(set(c.TIER_A_HEADERS)) == len(c.TIER_A_HEADERS), "Tier A headers contain duplicates"

    # Non-A headers
    assert len(c.NON_A_HEADERS) == 13, "Non-A headers count changed unexpectedly"
    assert len(set(c.NON_A_HEADERS)) == len(c.NON_A_HEADERS), "Non-A headers contain duplicates"
