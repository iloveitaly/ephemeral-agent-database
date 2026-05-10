"""Unit tests for config. Focus on HTTP_BASIC_AUTH parsing."""

import pytest

from app.config import Settings


def _make(**overrides):
    defaults = dict(
        database_url="postgresql://x:y@z/db",
        redis_url="redis://r",
        http_basic_auth="admin:pw",
    )
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


def test_http_basic_auth_parses_simple():
    s = _make(http_basic_auth="admin:secret")
    assert s.auth_username == "admin"
    assert s.auth_password == "secret"


def test_http_basic_auth_preserves_colons_in_password():
    s = _make(http_basic_auth="admin:pa:ss:word")
    assert s.auth_username == "admin"
    assert s.auth_password == "pa:ss:word"


def test_http_basic_auth_rejects_missing_colon():
    with pytest.raises(ValueError):
        _make(http_basic_auth="adminsecret")


def test_http_basic_auth_rejects_empty_username():
    with pytest.raises(ValueError):
        _make(http_basic_auth=":secret")


def test_http_basic_auth_rejects_empty_password():
    with pytest.raises(ValueError):
        _make(http_basic_auth="admin:")
