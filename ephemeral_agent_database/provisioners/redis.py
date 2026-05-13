"""Redis provisioner. Key-prefix isolation on a shared DB.

Why key-prefix rather than per-DB isolation:
  redis-py (and most redis clients) issues SELECT N on every new connection
  when the URL path specifies /N. If we deny `-select` to lock tenants to one
  DB, clients can't even connect. If we allow `+select`, tenants can hop to
  another tenant's DB and read/write freely. Key-prefix isolation sidesteps
  both issues: ACL key pattern `~<user>:*` is enforced regardless of DB, so
  no SELECT acrobatics are required. Everyone connects to DB 0 (or whatever DB is shared).

Model:
  - One ACL user per tenant: `<user>` (= `<prefix>_<short_id>`).
  - Key pattern: `~<user>:*` — tenant can only touch keys starting with their
    own name followed by a colon.
  - Pub/sub channel pattern: `&<user>:*` — same isolation for channels.
  - Dangerous commands denied: FLUSHALL, CONFIG, ACL, MONITOR, etc.
  - FLUSHDB is denied too, since it would wipe every tenant's keys on the shared DB.
    Per-tenant cleanup happens via SCAN + UNLINK on the tenant's prefix.
"""

import secrets

import redis.asyncio as aioredis
import redis.exceptions
import structlog

from ephemeral_agent_database.constants import RESOURCE_PREFIX

logger = structlog.get_logger(__name__)


class RedisCreds:
    __slots__ = ("user", "password", "key_prefix")

    def __init__(self, user: str, password: str, key_prefix: str):
        self.user = user
        self.password = password
        self.key_prefix = key_prefix


# Denied command set. Note that FLUSHDB is denied because the DB is shared; per-tenant
# cleanup uses SCAN + UNLINK on their prefix instead.
_ACL_DENIED = (
    "-@dangerous",
    "-@admin",
    "-flushdb",
    "-flushall",
    "-config",
    "-acl",
    "-monitor",
    "-client",
    "-debug",
    "-shutdown",
    "-replicaof",
    "-slaveof",
    "-cluster",
    "-migrate",
    "-module",
    "-save",
    "-bgsave",
    "-bgrewriteaof",
    "-swapdb",
    "-move",
    "-copy",
)

# UNLINK batch size when clearing a tenant's keys.
_UNLINK_BATCH = 500


class RedisProvisioner:
    def __init__(self, admin_url: str):
        self.admin_url = admin_url
        self._client: aioredis.Redis | None = None

    async def init(self) -> None:
        self._client = aioredis.from_url(self.admin_url, decode_responses=True)
        # Fix pyright error by using execute_command
        await self._client.execute_command("PING")

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def detect_max_dbs(self) -> int:
        """Kept for /healthcheck reporting only. We don't allocate per-DB anymore."""
        assert self._client is not None
        try:
            result = await self._client.config_get("databases")
            if result and "databases" in result:
                return int(result["databases"])
        except redis.exceptions.RedisError as e:
            logger.warning("redis_config_get_failed", error=str(e))
        return 16

    def key_prefix_for(self, user: str) -> str:
        """The key namespace a tenant must prefix all keys with."""
        return f"{user}:"

    async def provision(self, user: str) -> RedisCreds:
        """Create an ACL user scoped to keys under `<user>:` and channels under `<user>:`."""
        assert self._client is not None
        password = secrets.token_urlsafe(32)
        key_pattern = f"~{user}:*"
        channel_pattern = f"&{user}:*"

        args = [
            "SETUSER",
            user,
            "on",
            f">{password}",
            "resetkeys",  # clear any inherited key patterns
            "resetchannels",  # clear any inherited channel patterns
            key_pattern,
            channel_pattern,
            "+@all",
            *_ACL_DENIED,
        ]
        await self._client.execute_command("ACL", *args)

        logger.info("redis_provisioned", user=user)
        return RedisCreds(
            user=user,
            password=password,
            key_prefix=self.key_prefix_for(user),
        )

    async def release(self, user: str) -> None:
        """Wipe the tenant's keys (SCAN + UNLINK on their prefix) and delete the ACL user."""
        assert self._client is not None
        prefix = self.key_prefix_for(user)
        match = f"{prefix}*"

        try:
            cursor: int = 0
            while True:
                cursor, keys = await self._client.scan(
                    cursor=cursor, match=match, count=_UNLINK_BATCH
                )
                if keys:
                    await self._client.unlink(*keys)
                if cursor == 0:
                    break
        except redis.exceptions.RedisError as e:
            logger.error("redis_scan_unlink_failed", user=user, error=str(e))
            raise

        try:
            await self._client.execute_command("ACL", "DELUSER", user)
        except redis.exceptions.ResponseError as e:
            if "does not exist" not in str(e).lower():
                logger.error("redis_deluser_failed", user=user, error=str(e))
                raise

        logger.info("redis_released", user=user)

    async def list_orphan_users(self, expected_users: set[str]) -> set[str]:
        """`ephemeral_*` ACL users NOT present in `expected_users`."""
        assert self._client is not None
        all_users = await self._client.execute_command("ACL", "USERS") or []
        prefix = f"{RESOURCE_PREFIX}_"
        existing = {u for u in all_users if isinstance(u, str) and u.startswith(prefix)}
        return existing - expected_users

    async def delete_user(self, user: str) -> None:
        """Drop an ACL user without clearing keys. Used by reconcile for orphans."""
        assert self._client is not None
        try:
            await self._client.execute_command("ACL", "DELUSER", user)
        except redis.exceptions.ResponseError as e:
            if "does not exist" not in str(e).lower():
                raise

    async def ping(self) -> bool:
        try:
            assert self._client is not None
            await self._client.execute_command("PING")
            return True
        except (redis.exceptions.RedisError, AssertionError):
            return False
