"""Test ephemeral-agent-database."""

import ephemeral_agent_database


def test_import() -> None:
    """Test that the  can be imported."""
    assert isinstance(ephemeral_agent_database.__name__, str)


def test_version() -> None:
    """Test that the version is available."""
    assert isinstance(ephemeral_agent_database.__version__, str)