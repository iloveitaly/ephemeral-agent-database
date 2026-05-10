"""End-to-end API tests.

These tests drive the real FastAPI app (including lifespan startup/shutdown) against
the live local postgres + redis. Each test gets fresh app state via the
unique_prefix / unique_control_db fixtures in conftest.

We override the app's settings via env vars before the lifespan runs.
"""

import base64
import os

import psycopg
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient

TEST_USERNAME = "admin"
TEST_PASSWORD = "dev"
AUTH_HEADER = (
    "Basic " + base64.b64encode(f"{TEST_USERNAME}:{TEST_PASSWORD}".encode()).decode()
)


@pytest_asyncio.fixture
async def api_client(pg_url, redis_url, clean_environment):
    """Build a fresh FastAPI app with test-scoped env and drive it via ASGITransport.

    Importing app.main at module scope would pin settings before the fixtures
    patch them, so we import inside the fixture after setting env vars.
    """
    os.environ["DATABASE_URL"] = pg_url
    os.environ["REDIS_URL"] = redis_url
    os.environ["HTTP_BASIC_AUTH"] = f"{TEST_USERNAME}:{TEST_PASSWORD}"

    # Force a re-import so settings are rebuilt against the new env.
    import importlib

    import ephemeral_agent_database.main as main_module

    importlib.reload(main_module)

    transport = ASGITransport(app=main_module.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        # Drive lifespan startup
        async with main_module.app.router.lifespan_context(main_module.app):
            yield client


@pytest.mark.asyncio
async def test_healthcheck_no_auth_required(api_client):
    r = await api_client.get("/healthcheck")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["postgres_reachable"] is True
    assert body["redis_reachable"] is True
    assert body["active_provisions"] == 0
    assert body["redis_max_dbs"] == 64


@pytest.mark.asyncio
async def test_provision_requires_auth(api_client):
    r = await api_client.post("/provision", json={})
    assert r.status_code == 401
    assert r.headers.get("www-authenticate", "").lower().startswith("basic")


@pytest.mark.asyncio
async def test_provision_rejects_wrong_password(api_client):
    r = await api_client.post(
        "/provision",
        json={},
        headers={"Authorization": "Basic " + base64.b64encode(b"admin:wrong").decode()},
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_provision_happy_path_returns_usable_credentials(
    api_client, pg_url, redis_url
):
    r = await api_client.post(
        "/provision", json={"ttl_hours": 1}, headers={"Authorization": AUTH_HEADER}
    )
    assert r.status_code == 201, r.text
    body = r.json()

    # Response shape
    assert "id" in body
    assert "short_id" in body
    assert body["database_url"].startswith("postgresql://")
    assert body["redis_url"].startswith("redis://")
    assert body["redis_key_prefix"].endswith(":")
    assert "expires_at" in body

    # The returned DATABASE_URL actually works
    async with await psycopg.AsyncConnection.connect(
        body["database_url"], autocommit=True
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute("CREATE TABLE foo (x int)")
            await cur.execute("INSERT INTO foo VALUES (1)")
            await cur.execute("SELECT count(*) FROM foo")
            row = await cur.fetchone()
            assert row == (1,)

    # The returned REDIS_URL works, scoped to the key prefix
    client = aioredis.from_url(body["redis_url"], decode_responses=True)
    try:
        await client.set(f"{body['redis_key_prefix']}hello", "world")
        assert await client.get(f"{body['redis_key_prefix']}hello") == "world"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_provision_default_ttl(api_client):
    r = await api_client.post(
        "/provision", json={}, headers={"Authorization": AUTH_HEADER}
    )
    assert r.status_code == 201
    body = r.json()
    # Default is 24 hours; allow slack for the timestamp comparison
    from datetime import datetime

    expires = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
    created = datetime.fromisoformat(body["created_at"].replace("Z", "+00:00"))
    delta = expires - created
    assert 23 * 3600 < delta.total_seconds() < 25 * 3600


@pytest.mark.asyncio
async def test_provision_rejects_ttl_over_max(api_client):
    r = await api_client.post(
        "/provision",
        json={"ttl_hours": 10_000},
        headers={"Authorization": AUTH_HEADER},
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_list_provisions_empty_then_populated(api_client):
    r = await api_client.get("/provisions", headers={"Authorization": AUTH_HEADER})
    assert r.status_code == 200
    assert r.json() == []

    await api_client.post("/provision", json={}, headers={"Authorization": AUTH_HEADER})
    await api_client.post("/provision", json={}, headers={"Authorization": AUTH_HEADER})

    r = await api_client.get("/provisions", headers={"Authorization": AUTH_HEADER})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 2
    # Credentials are NEVER in list responses
    for row in rows:
        assert "database_url" not in row
        assert "redis_url" not in row
        assert row["status"] == "active"


@pytest.mark.asyncio
async def test_get_provision_by_id(api_client):
    r = await api_client.post(
        "/provision", json={}, headers={"Authorization": AUTH_HEADER}
    )
    pid = r.json()["id"]

    r = await api_client.get(
        f"/provisions/{pid}", headers={"Authorization": AUTH_HEADER}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == pid
    # No creds in summary endpoint
    assert "database_url" not in body
    assert "redis_url" not in body


@pytest.mark.asyncio
async def test_get_provision_404(api_client):
    import uuid

    r = await api_client.get(
        f"/provisions/{uuid.uuid4()}", headers={"Authorization": AUTH_HEADER}
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_provision_invalid_uuid(api_client):
    r = await api_client.get(
        "/provisions/not-a-uuid", headers={"Authorization": AUTH_HEADER}
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_delete_provision_marks_releasing(api_client):
    r = await api_client.post(
        "/provision", json={}, headers={"Authorization": AUTH_HEADER}
    )
    pid = r.json()["id"]

    r = await api_client.delete(
        f"/provisions/{pid}", headers={"Authorization": AUTH_HEADER}
    )
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "releasing"
    assert body["released_at"] is not None


@pytest.mark.asyncio
async def test_delete_provision_is_idempotent(api_client):
    r = await api_client.post(
        "/provision", json={}, headers={"Authorization": AUTH_HEADER}
    )
    pid = r.json()["id"]

    r1 = await api_client.delete(
        f"/provisions/{pid}", headers={"Authorization": AUTH_HEADER}
    )
    r2 = await api_client.delete(
        f"/provisions/{pid}", headers={"Authorization": AUTH_HEADER}
    )
    assert r1.status_code == 202
    assert r2.status_code == 202
    # released_at should be unchanged on the second call (COALESCE)
    assert r1.json()["released_at"] == r2.json()["released_at"]


@pytest.mark.asyncio
async def test_full_lifecycle_provision_use_release_cleanup(api_client, pg_url):
    """End-to-end: provision, write data, release, run cleanup pass, verify gone."""
    r = await api_client.post(
        "/provision", json={}, headers={"Authorization": AUTH_HEADER}
    )
    body = r.json()
    pid = body["id"]

    # Write some data
    async with await psycopg.AsyncConnection.connect(
        body["database_url"], autocommit=True
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute("CREATE TABLE x (a int)")

    # Release
    await api_client.delete(
        f"/provisions/{pid}", headers={"Authorization": AUTH_HEADER}
    )

    # Run a cleanup pass directly (the background loop won't fire during the test)
    import ephemeral_agent_database.main as main_module

    await main_module.app.state.cleanup.run_once()

    # Provision is CLEANED
    r = await api_client.get(
        f"/provisions/{pid}", headers={"Authorization": AUTH_HEADER}
    )
    assert r.json()["status"] == "cleaned"

    # The tenant DB no longer exists -- connecting with the old URL fails
    with pytest.raises(psycopg.OperationalError):
        await psycopg.AsyncConnection.connect(body["database_url"])
