"""Background cleanup loop and startup reconciliation.

Cleanup pass (runs every CLEANUP_INTERVAL_SECONDS):
  For each row returned by find_rows_to_cleanup:
    - If status is still 'active' or 'pending', transition to 'releasing' first.
    - If pg not yet dropped, try to drop it -> mark_pg_dropped on success.
    - If redis not yet cleaned, try to clean it -> mark_redis_cleaned on success.
    - If both are done, mark_cleaned.
  Failures on one resource never block the other, and never block other rows.

Reconcile (runs once at startup):
  - Compare live resources in pg and redis against the control table.
  - Orphans (resources without a control row) are torn down.
  - Missing resources (control row with no resource) are left alone; the
    normal cleanup pass will sweep them to a terminal state eventually.
"""

import asyncio
from datetime import UTC, datetime

import structlog

from ephemeral_agent_database.constants import (
    CLEANUP_INTERVAL_SECONDS,
    PROVISION_PENDING_TIMEOUT_SECONDS,
)
from ephemeral_agent_database.control import Control
from ephemeral_agent_database.models import ProvisionRow, ProvisionStatus
from ephemeral_agent_database.provisioners.postgres import PostgresProvisioner
from ephemeral_agent_database.provisioners.redis import RedisProvisioner

logger = structlog.get_logger(__name__)


class Cleanup:
    def __init__(
        self,
        control: Control,
        postgres: PostgresProvisioner,
        redis: RedisProvisioner,
    ):
        self.control = control
        self.postgres = postgres
        self.redis = redis
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="cleanup-loop")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("cleanup_pass_failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=CLEANUP_INTERVAL_SECONDS
                )
            except TimeoutError:
                pass

    async def run_once(self) -> int:
        """Execute one cleanup pass. Returns the number of rows processed."""
        rows = await self.control.find_rows_to_cleanup(
            PROVISION_PENDING_TIMEOUT_SECONDS
        )
        if not rows:
            return 0
        logger.info("cleanup_pass_start", count=len(rows))
        for row in rows:
            await self._release_row(row)
        return len(rows)

    async def _release_row(self, row: ProvisionRow) -> None:
        log = logger.bind(
            provision_id=str(row.id),
            short_id=row.short_id,
            status=row.status,
        )

        if row.status in (ProvisionStatus.ACTIVE, ProvisionStatus.PENDING):
            await self.control.mark_releasing(row.id, released_at=datetime.now(UTC))

        if row.pg_dropped_at is None:
            try:
                await self.postgres.release(row.pg_db_name, row.pg_role_name)
                await self.control.mark_pg_dropped(row.id)
                log.info("pg_released")
            except Exception:
                log.exception("pg_release_failed")

        if row.redis_cleaned_at is None:
            try:
                await self.redis.release(row.redis_user_name)
                await self.control.mark_redis_cleaned(row.id)
                log.info("redis_released")
            except Exception:
                log.exception("redis_release_failed")

        fresh = await self.control.get(row.id)
        if (
            fresh is not None
            and fresh.pg_dropped_at is not None
            and fresh.redis_cleaned_at is not None
        ):
            await self.control.mark_cleaned(row.id)
            log.info("fully_cleaned")

    async def reconcile(self) -> None:
        """Tear down orphan resources at startup."""
        (
            pg_expected,
            role_expected,
            redis_expected,
        ) = await self.control.list_live_resource_names()

        try:
            orphan_dbs, orphan_roles = await self.postgres.list_orphans(
                pg_expected, role_expected
            )
            for db_name in orphan_dbs:
                try:
                    await self.postgres.release(
                        db_name, role_name=_guess_role_for_db(db_name)
                    )
                    logger.info("reconcile_pg_orphan_dropped", db=db_name)
                except Exception:
                    logger.exception("reconcile_pg_orphan_failed", db=db_name)
            for role_name in orphan_roles:
                if role_name in role_expected:
                    continue
                try:
                    await _drop_role_only(self.postgres, role_name)
                    logger.info("reconcile_pg_orphan_role_dropped", role=role_name)
                except Exception:
                    logger.exception("reconcile_pg_orphan_role_failed", role=role_name)
        except Exception:
            logger.exception("reconcile_pg_failed")

        try:
            orphan_users = await self.redis.list_orphan_users(redis_expected)
            for user in orphan_users:
                try:
                    await self.redis.delete_user(user)
                    logger.info("reconcile_redis_orphan_user_dropped", user=user)
                except Exception:
                    logger.exception("reconcile_redis_orphan_user_failed", user=user)
        except Exception:
            logger.exception("reconcile_redis_failed")


def _guess_role_for_db(db_name: str) -> str:
    """Derive the conventional role name from a db name. Used only for reconcile
    where we've lost the control-row linkage.
    """
    from ephemeral_agent_database.constants import RESOURCE_PREFIX

    if db_name.startswith(f"{RESOURCE_PREFIX}_"):
        short_id = db_name[len(RESOURCE_PREFIX) + 1 :]
        return f"{RESOURCE_PREFIX}_user_{short_id}"
    return db_name


async def _drop_role_only(postgres: PostgresProvisioner, role_name: str) -> None:
    from psycopg import AsyncConnection
    from psycopg.sql import SQL, Identifier

    async with await AsyncConnection.connect(
        postgres.superuser_url, autocommit=True
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                SQL("DROP ROLE IF EXISTS {role}").format(role=Identifier(role_name))
            )
