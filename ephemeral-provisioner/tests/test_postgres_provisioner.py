"""Integration tests for PostgresProvisioner. Uses the locally running pg."""

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier

from app.naming import pg_db_name, pg_role_name


@pytest.mark.asyncio
async def test_provision_creates_db_and_role(postgres_provisioner, pg_url, unique_prefix):
    db = pg_db_name(unique_prefix, "abc123xyz0")
    role = pg_role_name(unique_prefix, "abc123xyz0")

    creds = await postgres_provisioner.provision(db, role)

    assert creds.db_name == db
    assert creds.role_name == role
    assert len(creds.password) >= 20

    # Verify the DB and role both exist.
    async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (db,)
            )
            assert await cur.fetchone() is not None
            await cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)
            )
            assert await cur.fetchone() is not None


@pytest.mark.asyncio
async def test_returned_creds_can_connect_and_write(
    postgres_provisioner, pg_url, unique_prefix
):
    from urllib.parse import urlparse
    db = pg_db_name(unique_prefix, "abc123xyz0")
    role = pg_role_name(unique_prefix, "abc123xyz0")

    creds = await postgres_provisioner.provision(db, role)

    parsed = urlparse(pg_url)
    host = parsed.hostname
    port = parsed.port or 5432
    conninfo = f"postgresql://{creds.role_name}:{creds.password}@{host}:{port}/{creds.db_name}"

    async with await AsyncConnection.connect(conninfo, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute("CREATE TABLE t (x int)")
            await cur.execute("INSERT INTO t VALUES (42)")
            await cur.execute("SELECT x FROM t")
            row = await cur.fetchone()
            assert row == (42,)


@pytest.mark.asyncio
async def test_release_drops_db_and_role(postgres_provisioner, pg_url, unique_prefix):
    db = pg_db_name(unique_prefix, "abc123xyz0")
    role = pg_role_name(unique_prefix, "abc123xyz0")

    await postgres_provisioner.provision(db, role)
    await postgres_provisioner.release(db, role)

    async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            assert await cur.fetchone() is None
            await cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            assert await cur.fetchone() is None


@pytest.mark.asyncio
async def test_release_with_live_connection_still_succeeds(
    postgres_provisioner, pg_url, unique_prefix
):
    """Verifies DROP DATABASE ... WITH (FORCE) terminates live sessions."""
    from urllib.parse import urlparse
    db = pg_db_name(unique_prefix, "abc123xyz0")
    role = pg_role_name(unique_prefix, "abc123xyz0")
    creds = await postgres_provisioner.provision(db, role)

    parsed = urlparse(pg_url)
    host = parsed.hostname
    port = parsed.port or 5432
    conninfo = f"postgresql://{creds.role_name}:{creds.password}@{host}:{port}/{creds.db_name}"

    # Open a connection and hold it
    live = await AsyncConnection.connect(conninfo, autocommit=True)
    try:
        await postgres_provisioner.release(db, role)
    finally:
        # Connection is already terminated on the server side; closing client-side is best-effort.
        try:
            await live.close()
        except Exception:
            pass

    async with await AsyncConnection.connect(pg_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            assert await cur.fetchone() is None


@pytest.mark.asyncio
async def test_release_is_idempotent(postgres_provisioner, unique_prefix):
    db = pg_db_name(unique_prefix, "abc123xyz0")
    role = pg_role_name(unique_prefix, "abc123xyz0")
    await postgres_provisioner.provision(db, role)
    await postgres_provisioner.release(db, role)
    # Second release should not raise
    await postgres_provisioner.release(db, role)


@pytest.mark.asyncio
async def test_list_orphans_finds_resources_not_in_expected(
    postgres_provisioner, pg_url, unique_prefix
):
    db1 = pg_db_name(unique_prefix, "aaaaaaaaaa")
    role1 = pg_role_name(unique_prefix, "aaaaaaaaaa")
    db2 = pg_db_name(unique_prefix, "bbbbbbbbbb")
    role2 = pg_role_name(unique_prefix, "bbbbbbbbbb")

    # Patch the provisioner's view of RESOURCE_PREFIX so LIKE filter matches
    from app.provisioners import postgres as _pg
    original_prefix = _pg.RESOURCE_PREFIX
    _pg.RESOURCE_PREFIX = unique_prefix

    try:
        await postgres_provisioner.provision(db1, role1)
        await postgres_provisioner.provision(db2, role2)

        # Both are "expected" -> no orphans
        orphan_dbs, orphan_roles = await postgres_provisioner.list_orphans(
            {db1, db2}, {role1, role2}
        )
        assert orphan_dbs == set()
        assert orphan_roles == set()

        # Only db1/role1 expected -> db2/role2 are orphans
        orphan_dbs, orphan_roles = await postgres_provisioner.list_orphans(
            {db1}, {role1}
        )
        assert orphan_dbs == {db2}
        assert orphan_roles == {role2}
    finally:
        _pg.RESOURCE_PREFIX = original_prefix


@pytest.mark.asyncio
async def test_ping(postgres_provisioner):
    assert await postgres_provisioner.ping() is True


@pytest.mark.asyncio
async def test_bad_short_id_cannot_reach_provisioner():
    """Sanity check: a SQL injection attempt in a short_id is caught by validate_short_id
    BEFORE any connection is opened. The provisioner itself trusts its inputs.
    """
    from app.naming import pg_db_name
    with pytest.raises(ValueError):
        pg_db_name("prefix", "abc'; DROP")
