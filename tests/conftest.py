"""Pytest fixtures.

Uses the locally running postgres + redis instances (no testcontainers — keeps
tests fast and avoids the docker dependency). Each test gets an isolated state
via:
  * A unique RESOURCE_PREFIX (so tests don't stomp each other's ephemeral DBs).
  * A fresh control DB dropped and recreated at teardown.
  * Redis ACLs cleaned up at teardown.

Run with:  uv run pytest
"""

import asyncio
import os
import secrets
from typing import AsyncIterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier

from ephemeral_agent_database import constants
from ephemeral_agent_database.control import Control
from ephemeral_agent_database.provisioners.postgres import PostgresProvisioner
from ephemeral_agent_database.provisioners.redis import RedisProvisioner

PG_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379")


@pytest.fixture
def pg_url() -> str:
    return PG_URL


@pytest.fixture
def redis_url() -> str:
    return REDIS_URL


@pytest.fixture(autouse=True)
def unique_prefix(monkeypatch):
    """Give each test its own resource prefix so parallel/sequential tests
    don't see each other's leftover resources. The prefix is short enough to
    keep role names under postgres's 63-char limit.
    """
    # 6 hex chars is enough to avoid collisions within a test run.
    suffix = secrets.token_hex(3)
    new_prefix = f"eph{suffix}"
    monkeypatch.setattr(constants, "RESOURCE_PREFIX", new_prefix)
    # Also override the constant as imported into submodules.
    from ephemeral_agent_database.provisioners import postgres as _pg
    from ephemeral_agent_database.provisioners import redis as _rd
    from ephemeral_agent_database import cleanup as _cl
    monkeypatch.setattr(_pg, "RESOURCE_PREFIX", new_prefix)
    monkeypatch.setattr(_rd, "RESOURCE_PREFIX", new_prefix)
    # cleanup.py does a local-scope import of RESOURCE_PREFIX inside a helper;
    # if it's been imported already at module load, patch it too. Safe if absent.
    if hasattr(_cl, "RESOURCE_PREFIX"):
        monkeypatch.setattr(_cl, "RESOURCE_PREFIX", new_prefix)
    yield new_prefix


@pytest.fixture(autouse=True)
def unique_control_db(monkeypatch, unique_prefix):
    """Each test gets its own control DB. Name derived from the prefix so it's
    easy to correlate state if something leaks.
    """
    name = f"{unique_prefix}_control"
    monkeypatch.setattr(constants, "CONTROL_DB_NAME", name)
    from ephemeral_agent_database import control as _ctrl
    monkeypatch.setattr(_ctrl, "CONTROL_DB_NAME", name)
    yield name


@pytest_asyncio.fixture
async def clean_environment(pg_url, redis_url, unique_prefix, unique_control_db):
    """Before AND after each test, drop any lingering resources matching the
    current prefix.
    """
    async def sweep():
        # Postgres: drop any ephemeral_* DB and role matching the prefix.
        async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                like = f"{unique_prefix}%"
                await cur.execute(
                    "SELECT datname FROM pg_database WHERE datname LIKE %s", (like,)
                )
                for (db,) in await cur.fetchall():
                    await cur.execute(
                        SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                            Identifier(db)
                        )
                    )
                await cur.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname LIKE %s", (like,)
                )
                for (role,) in await cur.fetchall():
                    await cur.execute(
                        SQL("DROP ROLE IF EXISTS {}").format(Identifier(role))
                    )
                # Control DB too (may have been recreated under new name).
                await cur.execute(
                    SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        Identifier(unique_control_db)
                    )
                )
        # Redis: delete ACL users with the prefix, flush all DBs.
        client = aioredis.from_url(redis_url, decode_responses=True)
        try:
            users = await client.execute_command("ACL", "USERS")
            for u in users:
                if isinstance(u, str) and u.startswith(unique_prefix):
                    await client.execute_command("ACL", "DELUSER", u)
            # Flush every DB we might've touched
            max_dbs = int((await client.config_get("databases"))["databases"])
            for n in range(max_dbs):
                db_client = aioredis.from_url(redis_url, db=n, decode_responses=True)
                try:
                    await db_client.flushdb()
                finally:
                    await db_client.aclose()
        finally:
            await client.aclose()

    await sweep()
    yield
    await sweep()


@pytest_asyncio.fixture
async def postgres_provisioner(pg_url, clean_environment) -> AsyncIterator[PostgresProvisioner]:
    p = PostgresProvisioner(pg_url)
    yield p


@pytest_asyncio.fixture
async def redis_provisioner(redis_url, clean_environment) -> AsyncIterator[RedisProvisioner]:
    p = RedisProvisioner(redis_url)
    await p.init()
    try:
        yield p
    finally:
        await p.close()


@pytest_asyncio.fixture
async def control(pg_url, clean_environment) -> AsyncIterator[Control]:
    c = Control(pg_url)
    await c.init()
    try:
        yield c
    finally:
        await c.close()
