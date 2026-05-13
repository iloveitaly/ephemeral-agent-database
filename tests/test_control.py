"""Integration tests for Control. Exercises schema bootstrap and all CRUD paths."""

from datetime import UTC, datetime, timedelta

import pytest

from ephemeral_agent_database.models import ProvisionStatus


def _future(hours: int = 1) -> datetime:
    return datetime.now(UTC) + timedelta(hours=hours)


def _past(seconds: int = 60) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)


async def _insert_sample(control, short_id: str, expires_at: datetime):
    return await control.insert_pending(
        short_id=short_id,
        pg_db_name=f"eph_{short_id}",
        pg_role_name=f"eph_user_{short_id}",
        redis_user_name=f"eph_{short_id}",
        redis_db_number=0,
        expires_at=expires_at,
    )


@pytest.mark.asyncio
async def test_init_creates_control_db_and_schema(control):
    rows = await control.list_all()
    assert rows == []


@pytest.mark.asyncio
async def test_init_is_idempotent(pg_url, control):
    from ephemeral_agent_database.control import Control

    c2 = Control(pg_url)
    await c2.init()
    await c2.close()


@pytest.mark.asyncio
async def test_insert_pending_creates_row_with_status_pending(control):
    row = await _insert_sample(control, "aaaaaaaaaa", _future())
    assert row.short_id == "aaaaaaaaaa"
    assert row.status == ProvisionStatus.PENDING
    assert row.pg_dropped_at is None
    assert row.redis_cleaned_at is None
    assert row.released_at is None
    assert row.redis_db_number == 0


@pytest.mark.asyncio
async def test_insert_pending_rejects_duplicate_short_id(control):
    import psycopg

    await _insert_sample(control, "aaaaaaaaaa", _future())
    with pytest.raises(psycopg.errors.UniqueViolation):
        await _insert_sample(control, "aaaaaaaaaa", _future())


@pytest.mark.asyncio
async def test_status_transitions(control):
    row = await _insert_sample(control, "aaaaaaaaaa", _future())

    await control.mark_active(row.id)
    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.ACTIVE

    await control.mark_releasing(row.id, released_at=datetime.now(UTC))
    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.RELEASING
    assert fresh.released_at is not None

    first_released = fresh.released_at
    await control.mark_releasing(
        row.id, released_at=datetime.now(UTC) + timedelta(hours=1)
    )
    fresh = await control.get(row.id)
    assert fresh.released_at == first_released

    await control.mark_pg_dropped(row.id)
    await control.mark_redis_cleaned(row.id)
    await control.mark_cleaned(row.id)
    fresh = await control.get(row.id)
    assert fresh.status == ProvisionStatus.CLEANED
    assert fresh.pg_dropped_at is not None
    assert fresh.redis_cleaned_at is not None


@pytest.mark.asyncio
async def test_delete_hard_removes_row(control):
    row = await _insert_sample(control, "aaaaaaaaaa", _future())
    await control.delete(row.id)
    assert await control.get(row.id) is None


@pytest.mark.asyncio
async def test_list_all_and_count_active(control):
    r1 = await _insert_sample(control, "aaaaaaaaaa", _future())
    r2 = await _insert_sample(control, "bbbbbbbbbb", _future())
    await _insert_sample(control, "cccccccccc", _future())
    await control.mark_active(r1.id)
    await control.mark_active(r2.id)

    rows = await control.list_all()
    assert {r.short_id for r in rows} == {"aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"}
    assert await control.count_active() == 2


@pytest.mark.asyncio
async def test_find_rows_to_cleanup_picks_expired_active(control):
    r = await _insert_sample(control, "aaaaaaaaaa", _past(seconds=10))
    await control.mark_active(r.id)

    rows = await control.find_rows_to_cleanup(pending_timeout_seconds=300)
    assert [x.id for x in rows] == [r.id]


@pytest.mark.asyncio
async def test_find_rows_to_cleanup_picks_releasing(control):
    r = await _insert_sample(control, "aaaaaaaaaa", _future())
    await control.mark_active(r.id)
    await control.mark_releasing(r.id, released_at=datetime.now(UTC))

    rows = await control.find_rows_to_cleanup(pending_timeout_seconds=300)
    assert [x.id for x in rows] == [r.id]


@pytest.mark.asyncio
async def test_find_rows_to_cleanup_picks_stale_pending(control):
    r = await _insert_sample(control, "aaaaaaaaaa", _future(hours=24))
    async with control.pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE provisions SET created_at = now() - interval '10 minutes' WHERE id = %s",
                (r.id,),
            )

    rows = await control.find_rows_to_cleanup(pending_timeout_seconds=300)
    assert [x.id for x in rows] == [r.id]


@pytest.mark.asyncio
async def test_find_rows_to_cleanup_skips_fresh_active_and_cleaned(control):
    r1 = await _insert_sample(control, "aaaaaaaaaa", _future(hours=24))
    await control.mark_active(r1.id)

    r2 = await _insert_sample(control, "bbbbbbbbbb", _past())
    await control.mark_active(r2.id)
    await control.mark_releasing(r2.id)
    await control.mark_pg_dropped(r2.id)
    await control.mark_redis_cleaned(r2.id)
    await control.mark_cleaned(r2.id)

    rows = await control.find_rows_to_cleanup(pending_timeout_seconds=300)
    assert rows == []


@pytest.mark.asyncio
async def test_list_live_resource_names(control):
    r1 = await _insert_sample(control, "aaaaaaaaaa", _future())
    r2 = await _insert_sample(control, "bbbbbbbbbb", _future())
    await control.mark_active(r1.id)
    await control.mark_active(r2.id)

    await control.mark_pg_dropped(r1.id)

    pg_dbs, pg_roles, redis_users = await control.list_live_resource_names()
    assert "eph_aaaaaaaaaa" not in pg_dbs
    assert "eph_user_aaaaaaaaaa" not in pg_roles
    assert "eph_aaaaaaaaaa" in redis_users
    assert "eph_bbbbbbbbbb" in pg_dbs
    assert "eph_user_bbbbbbbbbb" in pg_roles
    assert "eph_bbbbbbbbbb" in redis_users


@pytest.mark.asyncio
async def test_ping(control):
    assert await control.ping() is True
