"""Postgres provisioner. Creates a per-tenant database + role, drops them on cleanup."""

import secrets

import psycopg
import structlog
from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier, Literal

from app.constants import RESOURCE_PREFIX
from app.urls import postgres_url_with_db

logger = structlog.get_logger(__name__)


class PostgresCreds:
    __slots__ = ("db_name", "role_name", "password")

    def __init__(self, db_name: str, role_name: str, password: str):
        self.db_name = db_name
        self.role_name = role_name
        self.password = password


class PostgresProvisioner:
    def __init__(self, superuser_url: str):
        self.superuser_url = superuser_url

    async def provision(self, db_name: str, role_name: str) -> PostgresCreds:
        """Create a LOGIN role, a database owned by that role, and lock down
        schema-level permissions so only the role can touch its own data.
        """
        password = secrets.token_urlsafe(32)

        # Role + database are created from the cluster-default DB in autocommit
        # mode. CREATE DATABASE cannot run inside a transaction.
        async with await AsyncConnection.connect(self.superuser_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    SQL("CREATE ROLE {role} LOGIN PASSWORD {pw}").format(
                        role=Identifier(role_name),
                        pw=Literal(password),
                    )
                )
                await cur.execute(
                    SQL("CREATE DATABASE {db} OWNER {role}").format(
                        db=Identifier(db_name),
                        role=Identifier(role_name),
                    )
                )

        # Reconnect into the new DB (still as superuser) to lock down `public`.
        # By default, `public` is writable by the `public` role in older pg; in
        # pg 15+ it's already locked. Be explicit either way.
        new_db_url = postgres_url_with_db(self.superuser_url, db_name)
        async with await AsyncConnection.connect(new_db_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    SQL("REVOKE ALL ON SCHEMA public FROM PUBLIC")
                )
                await cur.execute(
                    SQL("GRANT ALL ON SCHEMA public TO {role}").format(
                        role=Identifier(role_name)
                    )
                )

        logger.info("postgres_provisioned", db=db_name, role=role_name)
        return PostgresCreds(db_name=db_name, role_name=role_name, password=password)

    async def release(self, db_name: str, role_name: str) -> None:
        """Drop the database (terminating any live connections) and the role.

        Uses `DROP DATABASE ... WITH (FORCE)` (postgres 13+) to avoid the
        terminate-connections-then-drop dance.
        """
        async with await AsyncConnection.connect(self.superuser_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        SQL("DROP DATABASE IF EXISTS {db} WITH (FORCE)").format(
                            db=Identifier(db_name)
                        )
                    )
                except psycopg.Error as e:
                    logger.error("pg_drop_database_failed", db=db_name, error=str(e))
                    raise
                try:
                    await cur.execute(
                        SQL("DROP ROLE IF EXISTS {role}").format(
                            role=Identifier(role_name)
                        )
                    )
                except psycopg.Error as e:
                    logger.error("pg_drop_role_failed", role=role_name, error=str(e))
                    raise

        logger.info("postgres_released", db=db_name, role=role_name)

    async def list_orphans(
        self, expected_dbs: set[str], expected_roles: set[str]
    ) -> tuple[set[str], set[str]]:
        """Query the cluster for existing `ephemeral_*` DBs and roles, return
        the ones NOT in the expected sets (i.e. orphans to tear down).

        The control DB itself is always excluded from the sweep, since it also
        starts with the resource prefix but is not a tenant resource.
        """
        from app.constants import CONTROL_DB_NAME
        prefix_like = f"{RESOURCE_PREFIX}_%"
        async with await AsyncConnection.connect(self.superuser_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT datname FROM pg_database WHERE datname LIKE %s",
                    (prefix_like,),
                )
                all_dbs = {row[0] for row in await cur.fetchall()} - {CONTROL_DB_NAME}
                await cur.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname LIKE %s",
                    (prefix_like,),
                )
                all_roles = {row[0] for row in await cur.fetchall()}

        orphan_dbs = all_dbs - expected_dbs
        orphan_roles = all_roles - expected_roles
        return orphan_dbs, orphan_roles

    async def ping(self) -> bool:
        try:
            async with await AsyncConnection.connect(self.superuser_url, autocommit=True) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    await cur.fetchone()
            return True
        except psycopg.Error:
            return False
