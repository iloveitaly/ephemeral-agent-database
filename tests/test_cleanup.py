"""Integration tests for the Cleanup coordinator (loop + reconcile)."""

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from psycopg import AsyncConnection

from ephemeral_agent_database.cleanup import Cleanup
from ephemeral_agent_database.models import ProvisionStatus
from ephemeral_agent_database.naming import pg_db_name, pg_role_name, redis_user_name


@pytest_asyncio.fixture
async def cleanup(control, postgres_provisioner, redis_provisioner):
    c = Cleanup(control=control, postgres=postgres_provisioner, redis=redis_provisioner)
    yield c


def _future(hours: int = 1) -> datetime:
    return datetime.now(UTC) + timedelta(hours=hours)


def _past(seconds: int = 10) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)


async def _full_provision(
    postgres_provisioner, redis_provisioner, control, short_id, prefix, expires_at
):
    """Mirror the provision endpoint: insert row, create resources, mark active."""
    db = pg_db_name(prefix, short_id)
    role = pg_role_name(prefix, short_id)
    user = redis_user_name(prefix, short_id)
    row = await control.insert_pending(
        short_id=short_id,
        pg_db_name=db,
        pg_role_name=role,
        redis_user_name=user,
        redis_db_number=0,
        expires_at=expires_at,
    )
    await postgres_provisioner.provision(db, role)
    await redis_provisioner.provision(user)
    await control.mark_active(row.id)
    return row, db, role, user


@pytest.mark.asyncio
async def test_cleanup_releases_expired_active_row(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix, pg_url
):
    row, db, role, _ = await _full_provision(
        postgres_provisioner,
        redis_provisioner,
        control,
        "aaaaaaaaaa",
        unique_prefix,
        _past(),
    )

    processed = await cleanup.run_once()
    assert processed == 1

    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.CLEANED
    assert fresh.pg_dropped_at is not None
    assert fresh.redis_cleaned_at is not None

    async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            assert await cur.fetchone() is None
            await cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            assert await cur.fetchone() is None


@pytest.mark.asyncio
async def test_cleanup_is_idempotent_across_passes(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix
):
    row, *_ = await _full_provision(
        postgres_provisioner,
        redis_provisioner,
        control,
        "aaaaaaaaaa",
        unique_prefix,
        _past(),
    )

    assert await cleanup.run_once() == 1
    assert await cleanup.run_once() == 0

    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.CLEANED


@pytest.mark.asyncio
async def test_cleanup_leaves_fresh_active_rows_alone(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix
):
    row, *_ = await _full_provision(
        postgres_provisioner,
        redis_provisioner,
        control,
        "aaaaaaaaaa",
        unique_prefix,
        _future(hours=24),
    )

    assert await cleanup.run_once() == 0

    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.ACTIVE


@pytest.mark.asyncio
async def test_cleanup_processes_releasing_row(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix
):
    """Simulates the DELETE /provisions/{id} user-triggered release path."""
    row, *_ = await _full_provision(
        postgres_provisioner,
        redis_provisioner,
        control,
        "aaaaaaaaaa",
        unique_prefix,
        _future(hours=24),
    )
    await control.mark_releasing(row.id, released_at=datetime.now(UTC))

    assert await cleanup.run_once() == 1

    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.CLEANED


@pytest.mark.asyncio
async def test_cleanup_retries_partial_failure(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix
):
    """If the pg drop succeeds but redis fails, the row stays in RELEASING and
    a subsequent pass completes the redis side.
    """
    row, _, _, _ = await _full_provision(
        postgres_provisioner,
        redis_provisioner,
        control,
        "aaaaaaaaaa",
        unique_prefix,
        _past(),
    )

    original_release = redis_provisioner.release
    calls = {"n": 0}

    async def flaky_release(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated redis failure")
        return await original_release(*args, **kwargs)

    redis_provisioner.release = flaky_release

    await cleanup.run_once()
    fresh = await control.get(row.id)
    assert fresh.pg_dropped_at is not None
    assert fresh.redis_cleaned_at is None
    assert fresh.status == ProvisionStatus.RELEASING

    await cleanup.run_once()
    fresh = await control.get(row.id)
    assert fresh.redis_cleaned_at is not None
    assert fresh.status == ProvisionStatus.CLEANED


@pytest.mark.asyncio
async def test_reconcile_drops_pg_orphan(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix, pg_url
):
    """A postgres DB/role with the prefix but no control row should be torn down."""
    orphan_db = pg_db_name(unique_prefix, "orphanxxxx")
    orphan_role = pg_role_name(unique_prefix, "orphanxxxx")
    await postgres_provisioner.provision(orphan_db, orphan_role)

    await cleanup.reconcile()

    async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (orphan_db,)
            )
            assert await cur.fetchone() is None
            await cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (orphan_role,)
            )
            assert await cur.fetchone() is None


@pytest.mark.asyncio
async def test_reconcile_drops_redis_orphan(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix, redis_url
):
    """A redis ACL user with the prefix but no control row should be torn down."""
    orphan_user = redis_user_name(unique_prefix, "orphanxxxx")
    await redis_provisioner.provision(orphan_user)

    await cleanup.reconcile()

    admin = aioredis.from_url(redis_url, decode_responses=True)
    try:
        users = await admin.execute_command("ACL", "USERS")
        assert orphan_user not in users
    finally:
        await admin.aclose()


@pytest.mark.asyncio
async def test_reconcile_leaves_live_resources_alone(
    postgres_provisioner, redis_provisioner, control, cleanup, unique_prefix, pg_url
):
    row, db, role, _ = await _full_provision(
        postgres_provisioner,
        redis_provisioner,
        control,
        "aaaaaaaaaa",
        unique_prefix,
        _future(hours=24),
    )

    await cleanup.reconcile()

    async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            assert await cur.fetchone() is not None
            await cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            assert await cur.fetchone() is not None

    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.ACTIVE
