"""Control-plane storage: schema bootstrap and provision-row CRUD.

The control table lives in a dedicated database (CONTROL_DB_NAME) inside the same
postgres cluster we provision tenant DBs into. We run DDL to bootstrap the control
DB and the `provisions` table at startup.

Redis DB numbers are allocated inside a transaction holding a postgres advisory lock,
so concurrent provisions never race for the same slot.
"""

from datetime import datetime
from uuid import UUID

import psycopg
import structlog
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg_pool import AsyncConnectionPool

from ephemeral_agent_database.constants import CONTROL_DB_NAME
from ephemeral_agent_database.models import ProvisionRow, ProvisionStatus
from ephemeral_agent_database.urls import postgres_url_with_db

logger = structlog.get_logger(__name__)


class Control:
    """Owns the connection pool to the control DB and exposes CRUD helpers."""

    def __init__(self, superuser_url: str):
        self.superuser_url = superuser_url
        self.control_url = postgres_url_with_db(superuser_url, CONTROL_DB_NAME)
        self.pool: AsyncConnectionPool | None = None

    async def init(self) -> None:
        await self._ensure_control_db()
        await self._ensure_schema()
        self.pool = AsyncConnectionPool(
            self.control_url,
            min_size=1,
            max_size=5,
            open=False,
        )
        await self.pool.open()
        logger.info("control_pool_opened")

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    # ----- schema bootstrap -----

    async def _ensure_control_db(self) -> None:
        """Connect to the server-default DB and create the control DB if missing."""
        async with await AsyncConnection.connect(
            self.superuser_url, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (CONTROL_DB_NAME,),
                )
                if await cur.fetchone() is None:
                    await cur.execute(
                        SQL("CREATE DATABASE {}").format(Identifier(CONTROL_DB_NAME))
                    )
                    logger.info("control_db_created", name=CONTROL_DB_NAME)

    async def _ensure_schema(self) -> None:
        async with await AsyncConnection.connect(
            self.control_url, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS provisions (
                      id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                      short_id          text UNIQUE NOT NULL,
                      status            text NOT NULL DEFAULT 'pending',
                      pg_db_name        text UNIQUE NOT NULL,
                      pg_role_name      text UNIQUE NOT NULL,
                      redis_user_name   text UNIQUE NOT NULL,
                      redis_db_number   integer NOT NULL,
                      created_at        timestamptz NOT NULL DEFAULT now(),
                      expires_at        timestamptz NOT NULL,
                      released_at       timestamptz,
                      pg_dropped_at     timestamptz,
                      redis_cleaned_at  timestamptz
                    )
                    """
                )
                # NOTE: redis_db_number is retained for schema stability but
                # currently always 0 (key-prefix isolation on shared DB 0).
                await cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_provisions_live_expiry
                      ON provisions (expires_at)
                      WHERE status IN ('pending', 'active', 'releasing')
                    """
                )
                await cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_provisions_status
                      ON provisions (status)
                    """
                )

    # ----- provision creation -----

    async def insert_pending(
        self,
        *,
        short_id: str,
        pg_db_name: str,
        pg_role_name: str,
        redis_user_name: str,
        expires_at: datetime,
    ) -> ProvisionRow:
        """Insert a pending row.

        Redis isolation is per-key-prefix on a shared DB 0, so no pool allocation
        is needed. The `redis_db_number` column is retained as 0 for all rows in
        case we want to reintroduce per-DB isolation later.
        """
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    INSERT INTO provisions
                      (short_id, pg_db_name, pg_role_name,
                       redis_user_name, redis_db_number, expires_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        short_id,
                        pg_db_name,
                        pg_role_name,
                        redis_user_name,
                        0,  # shared DB 0 for all tenants
                        expires_at,
                    ),
                )
                row = await cur.fetchone()
                assert row is not None
                return _row_to_provision(row)

    # ----- state transitions -----

    async def mark_active(self, provision_id: UUID) -> None:
        await self._set_status(provision_id, ProvisionStatus.ACTIVE)

    async def mark_releasing(
        self, provision_id: UUID, released_at: datetime | None = None
    ) -> None:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE provisions
                       SET status = %s,
                           released_at = COALESCE(released_at, %s)
                     WHERE id = %s
                    """,
                    (ProvisionStatus.RELEASING.value, released_at, provision_id),
                )

    async def mark_pg_dropped(self, provision_id: UUID) -> None:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE provisions SET pg_dropped_at = now() WHERE id = %s",
                    (provision_id,),
                )

    async def mark_redis_cleaned(self, provision_id: UUID) -> None:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE provisions SET redis_cleaned_at = now() WHERE id = %s",
                    (provision_id,),
                )

    async def mark_cleaned(self, provision_id: UUID) -> None:
        await self._set_status(provision_id, ProvisionStatus.CLEANED)

    async def delete(self, provision_id: UUID) -> None:
        """Hard delete. Used only when a pending row failed before any resources were created."""
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM provisions WHERE id = %s", (provision_id,)
                )

    async def _set_status(self, provision_id: UUID, status: ProvisionStatus) -> None:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE provisions SET status = %s WHERE id = %s",
                    (status.value, provision_id),
                )

    # ----- queries -----

    async def get(self, provision_id: UUID) -> ProvisionRow | None:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT * FROM provisions WHERE id = %s", (provision_id,)
                )
                row = await cur.fetchone()
                return _row_to_provision(row) if row else None

    async def list_all(self) -> list[ProvisionRow]:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM provisions ORDER BY created_at DESC")
                rows = await cur.fetchall()
                return [_row_to_provision(r) for r in rows]

    async def count_active(self) -> int:
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT count(*) FROM provisions WHERE status = 'active'"
                )
                row = await cur.fetchone()
                return row[0] if row else 0

    async def find_rows_to_cleanup(
        self, pending_timeout_seconds: int
    ) -> list[ProvisionRow]:
        """Rows the cleanup loop acts on:
        - active + expires_at < now()                 -> expired
        - releasing                                    -> in-flight or prior pass
        - pending + created_at older than timeout      -> failed provision
        """
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT * FROM provisions
                     WHERE (status = 'active'    AND expires_at < now())
                        OR (status = 'releasing')
                        OR (status = 'pending'   AND created_at < now() - make_interval(secs => %s))
                     ORDER BY created_at ASC
                    """,
                    (pending_timeout_seconds,),
                )
                rows = await cur.fetchall()
                return [_row_to_provision(r) for r in rows]

    async def list_live_resource_names(self) -> tuple[set[str], set[str], set[str]]:
        """For reconcile. Returns (pg_db_names, pg_role_names, redis_user_names)
        that should currently exist according to the control table.
        """
        assert self.pool is not None
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT pg_db_name, pg_role_name, redis_user_name,
                           pg_dropped_at, redis_cleaned_at
                      FROM provisions
                     WHERE status IN ('pending', 'active', 'releasing')
                    """
                )
                rows = await cur.fetchall()
                pg_dbs = {r["pg_db_name"] for r in rows if r["pg_dropped_at"] is None}
                pg_roles = {
                    r["pg_role_name"] for r in rows if r["pg_dropped_at"] is None
                }
                redis_users = {
                    r["redis_user_name"] for r in rows if r["redis_cleaned_at"] is None
                }
                return pg_dbs, pg_roles, redis_users

    async def ping(self) -> bool:
        try:
            assert self.pool is not None
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    await cur.fetchone()
            return True
        except (psycopg.Error, AssertionError):
            return False


def _row_to_provision(row: dict) -> ProvisionRow:
    return ProvisionRow(
        id=row["id"],
        short_id=row["short_id"],
        status=ProvisionStatus(row["status"]),
        pg_db_name=row["pg_db_name"],
        pg_role_name=row["pg_role_name"],
        redis_user_name=row["redis_user_name"],
        redis_db_number=row["redis_db_number"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        released_at=row["released_at"],
        pg_dropped_at=row["pg_dropped_at"],
        redis_cleaned_at=row["redis_cleaned_at"],
    )
