"""Unit tests for naming. Pure functions, no fixtures needed."""

import pytest

from ephemeral_agent_database.naming import (
    SHORT_ID_LENGTH,
    generate_short_id,
    pg_db_name,
    pg_role_name,
    validate_short_id,
)


def test_generate_short_id_has_correct_length_and_alphabet():
    for _ in range(100):
        s = generate_short_id()
        assert len(s) == SHORT_ID_LENGTH
        assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789" for c in s)


def test_generate_short_id_is_unique_enough():
    ids = {generate_short_id() for _ in range(1000)}
    assert len(ids) == 1000  # birthday collisions at this scale should be ~0


def test_validate_short_id_accepts_valid():
    validate_short_id("abc123xyz0")


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "short",
        "waytoolongforshortid",
        "ABC123xyz0",  # uppercase
        "abc-123xyz",  # hyphen
        "abc_123xyz",  # underscore
        "abc 123xyz",  # space
        "abc123xyz;",  # SQL metachar
        "abc123xyz'",  # quote
        'abc123xyz"',  # double quote
        "'; drop db--",  # classic
    ],
)
def test_validate_short_id_rejects_invalid(bad):
    with pytest.raises(ValueError):
        validate_short_id(bad)


def test_validate_short_id_rejects_non_string():
    with pytest.raises(ValueError):
        validate_short_id(12345)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        validate_short_id(None)  # type: ignore[arg-type]


def test_name_builders_use_prefix():
    sid = "abc123xyz0"
    assert pg_db_name("ephtest", sid) == "ephtest_abc123xyz0"
    assert pg_role_name("ephtest", sid) == "ephtest_user_abc123xyz0"


def test_name_builders_reject_bad_short_id():
    with pytest.raises(ValueError):
        pg_db_name("prefix", "BAD ID")
    with pytest.raises(ValueError):
        pg_role_name("prefix", "BAD ID")
