"""Unit tests for URL manipulation helpers."""

from urllib.parse import urlparse

from app.urls import (
    postgres_url_with_credentials,
    postgres_url_with_db,
    redis_url_with_credentials,
)


def test_postgres_url_with_db_replaces_path():
    url = "postgresql://admin:pw@host:5432/original"
    new = postgres_url_with_db(url, "other")
    parsed = urlparse(new)
    assert parsed.path == "/other"
    assert parsed.hostname == "host"
    assert parsed.port == 5432
    assert parsed.username == "admin"
    assert parsed.password == "pw"


def test_postgres_url_with_credentials_swaps_user_pw_and_db():
    url = "postgresql://admin:adminpw@host:5432/postgres"
    new = postgres_url_with_credentials(url, "tenant_db", "tenant_user", "tenant_pw")
    parsed = urlparse(new)
    assert parsed.username == "tenant_user"
    assert parsed.password == "tenant_pw"
    assert parsed.hostname == "host"
    assert parsed.port == 5432
    assert parsed.path == "/tenant_db"


def test_postgres_url_with_credentials_escapes_special_chars():
    from urllib.parse import unquote
    url = "postgresql://admin:adminpw@host:5432/postgres"
    new = postgres_url_with_credentials(url, "db", "user", "p@ss:word/with?chars")
    parsed = urlparse(new)
    # password is percent-encoded in the URL; urlparse does not decode.
    assert unquote(parsed.password or "") == "p@ss:word/with?chars"
    # and the encoded form should not contain raw special chars that break parsing
    assert ":" not in (parsed.password or "").replace("%3A", "X")


def test_redis_url_with_credentials_sets_path_to_db_number():
    url = "redis://:adminpw@host:6379"
    new = redis_url_with_credentials(url, 7, "tenant", "tpw")
    parsed = urlparse(new)
    assert parsed.username == "tenant"
    assert parsed.password == "tpw"
    assert parsed.hostname == "host"
    assert parsed.port == 6379
    assert parsed.path == "/7"


def test_redis_url_with_credentials_handles_no_port():
    url = "redis://host"
    new = redis_url_with_credentials(url, 1, "u", "p")
    parsed = urlparse(new)
    assert parsed.username == "u"
    assert parsed.password == "p"
    assert parsed.hostname == "host"
    assert parsed.path == "/1"
