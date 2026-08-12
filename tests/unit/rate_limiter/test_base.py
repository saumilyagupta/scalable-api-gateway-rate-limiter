import pytest

from gateway.rate_limiter.base import Decision, RateLimiter


def test_decision_is_frozen_dataclass():
    d = Decision(allowed=True, remaining=5, reset_after_seconds=0.0)
    assert d.allowed is True
    assert d.remaining == 5
    with pytest.raises(AttributeError):
        d.remaining = 10


def test_rate_limiter_is_abstract():
    with pytest.raises(TypeError):
        RateLimiter()
