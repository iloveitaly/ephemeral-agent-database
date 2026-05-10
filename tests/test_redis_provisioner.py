"""Integration tests for RedisProvisioner (key-prefix isolation model)."""

from urllib.parse import urlparse

import pytest
import redis.asyncio as aioredis
import redis.exceptions

from ephemeral_agent_database.naming import redis_user_name


def _tenant_url(admin_url: str, user: str, password: str) -> str:
    parsed = urlparse(admin_url)
    host = parsed.hostname
    port = parsed.port or 6379
    return f"redis://{user}:{password}@{host}:{port}/0"


@pytest.mark.asyncio
async def test_detect_max_dbs(redis_provisioner):
    n = await redis_provisioner.detect_max_dbs()
    # Local redis started with --databases 64
    assert n == 64


@pytest.mark.asyncio
async def test_provision_creates_acl_user_and_returns_key_prefix(
    redis_provisioner, redis_url, unique_prefix
):
    user = redis_user_name(unique_prefix, "abc123xyz0")
    creds = await redis_provisioner.provision(user)

    assert creds.user == user
    assert creds.key_prefix == f"{user}:"
    assert len(creds.password) >= 20

    admin = aioredis.from_url(redis_url, decode_responses=True)
    try:
        users = await admin.execute_command("ACL", "USERS")
        assert user in users
    finally:
        await admin.aclose()


@pytest.mark.asyncio
async def test_tenant_can_read_and_write_within_prefix(
    redis_provisioner, redis_url, unique_prefix
):
    user = redis_user_name(unique_prefix, "abc123xyz0")
    creds = await redis_provisioner.provision(user)

    client = aioredis.from_url(
        _tenant_url(redis_url, creds.user, creds.password), decode_responses=True
    )
    try:
        await client.set(f"{creds.key_prefix}hello", "world")
        assert await client.get(f"{creds.key_prefix}hello") == "world"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_tenant_cannot_write_outside_prefix(
    redis_provisioner, redis_url, unique_prefix
):
    """The critical isolation guarantee."""
    user = redis_user_name(unique_prefix, "abc123xyz0")
    creds = await redis_provisioner.provision(user)

    client = aioredis.from_url(
        _tenant_url(redis_url, creds.user, creds.password), decode_responses=True
    )
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.set("someoneelse:key", "x")
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.set("", "x")
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.get("unprefixed_key")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_tenant_cannot_see_another_tenants_keys(
    redis_provisioner, redis_url, unique_prefix
):
    user_a = redis_user_name(unique_prefix, "aaaaaaaaaa")
    user_b = redis_user_name(unique_prefix, "bbbbbbbbbb")
    creds_a = await redis_provisioner.provision(user_a)
    creds_b = await redis_provisioner.provision(user_b)

    # Tenant A writes a key. Tenant B cannot read it.
    ca = aioredis.from_url(
        _tenant_url(redis_url, creds_a.user, creds_a.password), decode_responses=True
    )
    cb = aioredis.from_url(
        _tenant_url(redis_url, creds_b.user, creds_b.password), decode_responses=True
    )
    try:
        await ca.set(f"{creds_a.key_prefix}secret", "classified")
        with pytest.raises(redis.exceptions.NoPermissionError):
            await cb.get(f"{creds_a.key_prefix}secret")
    finally:
        await ca.aclose()
        await cb.aclose()


@pytest.mark.asyncio
async def test_tenant_cannot_run_dangerous_commands(
    redis_provisioner, redis_url, unique_prefix
):
    user = redis_user_name(unique_prefix, "abc123xyz0")
    creds = await redis_provisioner.provision(user)

    client = aioredis.from_url(
        _tenant_url(redis_url, creds.user, creds.password), decode_responses=True
    )
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.execute_command("FLUSHALL")
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.execute_command("FLUSHDB")
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.execute_command("CONFIG", "GET", "maxmemory")
        with pytest.raises(redis.exceptions.NoPermissionError):
            await client.execute_command("ACL", "LIST")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_release_unlinks_tenant_keys_and_deletes_user(
    redis_provisioner, redis_url, unique_prefix
):
    user = redis_user_name(unique_prefix, "abc123xyz0")
    creds = await redis_provisioner.provision(user)

    # Write a bunch of keys (enough to exercise the SCAN batching).
    client = aioredis.from_url(
        _tenant_url(redis_url, creds.user, creds.password), decode_responses=True
    )
    try:
        for i in range(50):
            await client.set(f"{creds.key_prefix}k{i}", str(i))
    finally:
        await client.aclose()

    # Verify keys exist (via admin)
    admin = aioredis.from_url(redis_url, decode_responses=True)
    try:
        found = []
        cursor = 0
        while True:
            cursor, batch = await admin.scan(
                cursor=cursor, match=f"{creds.key_prefix}*", count=100
            )
            found.extend(batch)
            if cursor == 0:
                break
        assert len(found) == 50
    finally:
        await admin.aclose()

    await redis_provisioner.release(user)

    # User gone, keys gone
    admin = aioredis.from_url(redis_url, decode_responses=True)
    try:
        users = await admin.execute_command("ACL", "USERS")
        assert user not in users
        found = []
        cursor = 0
        while True:
            cursor, batch = await admin.scan(
                cursor=cursor, match=f"{creds.key_prefix}*", count=100
            )
            found.extend(batch)
            if cursor == 0:
                break
        assert found == []
    finally:
        await admin.aclose()


@pytest.mark.asyncio
async def test_release_does_not_touch_other_tenants_keys(
    redis_provisioner, redis_url, unique_prefix
):
    user_a = redis_user_name(unique_prefix, "aaaaaaaaaa")
    user_b = redis_user_name(unique_prefix, "bbbbbbbbbb")
    creds_a = await redis_provisioner.provision(user_a)
    creds_b = await redis_provisioner.provision(user_b)

    ca = aioredis.from_url(
        _tenant_url(redis_url, creds_a.user, creds_a.password), decode_responses=True
    )
    cb = aioredis.from_url(
        _tenant_url(redis_url, creds_b.user, creds_b.password), decode_responses=True
    )
    try:
        await ca.set(f"{creds_a.key_prefix}k", "a")
        await cb.set(f"{creds_b.key_prefix}k", "b")
    finally:
        await ca.aclose()
        await cb.aclose()

    await redis_provisioner.release(user_a)

    admin = aioredis.from_url(redis_url, decode_responses=True)
    try:
        assert await admin.get(f"{creds_a.key_prefix}k") is None
        assert await admin.get(f"{creds_b.key_prefix}k") == "b"
    finally:
        await admin.aclose()


@pytest.mark.asyncio
async def test_release_is_idempotent(redis_provisioner, unique_prefix):
    user = redis_user_name(unique_prefix, "abc123xyz0")
    await redis_provisioner.provision(user)
    await redis_provisioner.release(user)
    # Second release should not raise
    await redis_provisioner.release(user)


@pytest.mark.asyncio
async def test_list_orphan_users(redis_provisioner, unique_prefix):
    u1 = redis_user_name(unique_prefix, "aaaaaaaaaa")
    u2 = redis_user_name(unique_prefix, "bbbbbbbbbb")
    await redis_provisioner.provision(u1)
    await redis_provisioner.provision(u2)

    orphans = await redis_provisioner.list_orphan_users({u1, u2})
    assert orphans == set()

    orphans = await redis_provisioner.list_orphan_users({u1})
    assert orphans == {u2}


@pytest.mark.asyncio
async def test_delete_user_standalone(redis_provisioner, redis_url, unique_prefix):
    user = redis_user_name(unique_prefix, "abc123xyz0")
    await redis_provisioner.provision(user)
    await redis_provisioner.delete_user(user)

    admin = aioredis.from_url(redis_url, decode_responses=True)
    try:
        users = await admin.execute_command("ACL", "USERS")
        assert user not in users
    finally:
        await admin.aclose()

    # Idempotent
    await redis_provisioner.delete_user(user)


@pytest.mark.asyncio
async def test_pubsub_channel_isolation(redis_provisioner, redis_url, unique_prefix):
    user_a = redis_user_name(unique_prefix, "aaaaaaaaaa")
    user_b = redis_user_name(unique_prefix, "bbbbbbbbbb")
    creds_a = await redis_provisioner.provision(user_a)
    creds_b = await redis_provisioner.provision(user_b)

    ca = aioredis.from_url(
        _tenant_url(redis_url, creds_a.user, creds_a.password), decode_responses=True
    )
    try:
        # Tenant A can publish on their own channel
        await ca.publish(f"{creds_a.key_prefix}events", "hello")
        # Tenant A cannot publish on tenant B's channel
        with pytest.raises(redis.exceptions.NoPermissionError):
            await ca.publish(f"{creds_b.key_prefix}events", "injected")
    finally:
        await ca.aclose()


@pytest.mark.asyncio
async def test_ping(redis_provisioner):
    assert await redis_provisioner.ping() is True
