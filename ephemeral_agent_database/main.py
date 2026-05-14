"""FastAPI app wiring.

Lifespan responsibilities:
  1. Load settings, configure logging.
  2. Bring up the redis provisioner and detect max DBs.
  3. Bring up the control plane (creates control DB + schema if needed).
  4. Run one reconcile pass (tear down orphan resources).
  5. Start the background cleanup loop.

Shutdown reverses this.
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from ephemeral_agent_database.cleanup import Cleanup
from ephemeral_agent_database.config import Settings, load_settings
from ephemeral_agent_database.constants import (
    DEFAULT_TTL_HOURS,
    MAX_TTL_HOURS,
    RESOURCE_PREFIX,
)
from ephemeral_agent_database.control import Control
from ephemeral_agent_database.logging_config import configure_logging
from ephemeral_agent_database.models import (
    HealthResponse,
    ProvisionRequest,
    ProvisionResponse,
    ProvisionStatus,
    ProvisionSummary,
)
from ephemeral_agent_database.naming import (
    generate_short_id,
    pg_db_name,
    pg_role_name,
    redis_user_name,
)
from ephemeral_agent_database.provisioners.postgres import PostgresProvisioner
from ephemeral_agent_database.provisioners.redis import RedisProvisioner
from ephemeral_agent_database.urls import (
    postgres_url_with_credentials,
    redis_url_with_credentials,
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()

    settings = load_settings()
    app.state.settings = settings

    postgres = PostgresProvisioner(settings.database_url)
    redis = RedisProvisioner(settings.redis_url)
    await redis.init()
    redis_max_dbs = await redis.detect_max_dbs()
    logger.info("redis_max_dbs_detected", max_dbs=redis_max_dbs)

    control = Control(settings.database_url)
    await control.init()

    cleanup = Cleanup(control=control, postgres=postgres, redis=redis)

    app.state.settings = settings
    app.state.postgres = postgres
    app.state.redis = redis
    app.state.control = control
    app.state.cleanup = cleanup
    app.state.redis_max_dbs = redis_max_dbs

    try:
        await cleanup.reconcile()
    except Exception:
        logger.exception("reconcile_failed_continuing")

    await cleanup.start()
    logger.info("service_started")

    try:
        yield
    finally:
        logger.info("service_stopping")
        await cleanup.stop()
        await control.close()
        await redis.close()


app = FastAPI(
    title="Ephemeral Preview Environment Provisioner",
    version="0.1.0",
    lifespan=lifespan,
)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_control(request: Request) -> Control:
    return request.app.state.control


def get_postgres(request: Request) -> PostgresProvisioner:
    return request.app.state.postgres


def get_redis(request: Request) -> RedisProvisioner:
    return request.app.state.redis


_http_basic = HTTPBasic()


async def require_auth(
    credentials: Annotated[HTTPBasicCredentials, Depends(_http_basic)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> str:
    import secrets as _secrets

    given_u = credentials.username.encode("utf-8")
    given_p = credentials.password.encode("utf-8")
    expected_u = settings.auth_username.encode("utf-8")
    expected_p = settings.auth_password.encode("utf-8")
    u_ok = _secrets.compare_digest(given_u, expected_u)
    p_ok = _secrets.compare_digest(given_p, expected_p)
    if not (u_ok and p_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/info", response_model=HealthResponse)
async def info(
    control: Annotated[Control, Depends(get_control)],
    postgres: Annotated[PostgresProvisioner, Depends(get_postgres)],
    redis: Annotated[RedisProvisioner, Depends(get_redis)],
    request: Request,
    _user: Annotated[str, Depends(require_auth)],
) -> HealthResponse:
    pg_ok = await postgres.ping()
    redis_ok = await redis.ping()
    control_ok = await control.ping()
    active = await control.count_active() if control_ok else 0
    return HealthResponse(
        status="ok" if (pg_ok and redis_ok and control_ok) else "degraded",
        postgres_reachable=pg_ok and control_ok,
        redis_reachable=redis_ok,
        active_provisions=active,
        redis_max_dbs=request.app.state.redis_max_dbs,
        now=datetime.now(UTC),
    )


@app.post(
    "/provision",
    response_model=ProvisionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def provision(
    body: ProvisionRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    control: Annotated[Control, Depends(get_control)],
    postgres: Annotated[PostgresProvisioner, Depends(get_postgres)],
    redis: Annotated[RedisProvisioner, Depends(get_redis)],
    _user: Annotated[str, Depends(require_auth)],
) -> ProvisionResponse:
    ttl_hours = body.ttl_hours or DEFAULT_TTL_HOURS
    if ttl_hours > MAX_TTL_HOURS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"ttl_hours exceeds MAX_TTL_HOURS ({MAX_TTL_HOURS})",
        )

    short_id = generate_short_id()
    db_name = pg_db_name(RESOURCE_PREFIX, short_id)
    role_name = pg_role_name(RESOURCE_PREFIX, short_id)
    user_name = redis_user_name(RESOURCE_PREFIX, short_id)
    expires_at = datetime.now(UTC) + timedelta(hours=ttl_hours)

    from urllib.parse import urlparse

    redis_db = int(urlparse(settings.redis_url).path.lstrip("/") or "0")

    row = await control.insert_pending(
        short_id=short_id,
        pg_db_name=db_name,
        pg_role_name=role_name,
        redis_user_name=user_name,
        redis_db_number=redis_db,
        expires_at=expires_at,
    )

    try:
        pg_creds = await postgres.provision(db_name, role_name)
    except Exception as e:
        logger.exception("provision_pg_failed", provision_id=str(row.id))
        try:
            await postgres.release(db_name, role_name)
        except Exception:
            pass
        await control.delete(row.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to provision postgres: {e}",
        ) from e

    try:
        redis_creds = await redis.provision(user_name)
    except Exception as e:
        logger.exception("provision_redis_failed", provision_id=str(row.id))
        try:
            await postgres.release(db_name, role_name)
        except Exception:
            logger.exception("rollback_pg_release_failed")
        await control.delete(row.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"failed to provision redis: {e}",
        ) from e

    await control.mark_active(row.id)

    database_url = postgres_url_with_credentials(
        settings.database_url,
        db_name=db_name,
        user=pg_creds.role_name,
        password=pg_creds.password,
    )
    redis_url = redis_url_with_credentials(
        settings.redis_url,
        db_number=redis_db,
        user=redis_creds.user,
        password=redis_creds.password,
    )

    logger.info(
        "provision_complete",
        provision_id=str(row.id),
        short_id=short_id,
        expires_at=row.expires_at.isoformat(),
    )

    return ProvisionResponse(
        id=row.id,
        short_id=short_id,
        database_url=database_url,
        redis_url=redis_url,
        redis_key_prefix=redis_creds.key_prefix,
        created_at=row.created_at,
        expires_at=row.expires_at,
    )


@app.get(
    "/provisions",
    response_model=list[ProvisionSummary],
)
async def list_provisions(
    control: Annotated[Control, Depends(get_control)],
    _user: Annotated[str, Depends(require_auth)],
) -> list[ProvisionSummary]:
    rows = await control.list_all()
    return [_to_summary(r) for r in rows]


@app.get(
    "/provisions/{provision_id}",
    response_model=ProvisionSummary,
)
async def get_provision(
    provision_id: str,
    control: Annotated[Control, Depends(get_control)],
    _user: Annotated[str, Depends(require_auth)],
) -> ProvisionSummary:
    from uuid import UUID

    try:
        uid = UUID(provision_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid provision id") from None
    row = await control.get(uid)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return _to_summary(row)


@app.delete(
    "/provisions/{provision_id}",
    response_model=ProvisionSummary,
    status_code=status.HTTP_202_ACCEPTED,
)
async def release_provision(
    provision_id: str,
    control: Annotated[Control, Depends(get_control)],
    _user: Annotated[str, Depends(require_auth)],
) -> ProvisionSummary:
    from uuid import UUID

    try:
        uid = UUID(provision_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid provision id") from None
    row = await control.get(uid)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    if row.status in (ProvisionStatus.RELEASING, ProvisionStatus.CLEANED):
        return _to_summary(row)
    await control.mark_releasing(uid, released_at=datetime.now(UTC))
    fresh = await control.get(uid)
    assert fresh is not None
    return _to_summary(fresh)


def _to_summary(row) -> ProvisionSummary:
    return ProvisionSummary(
        id=row.id,
        short_id=row.short_id,
        status=row.status,
        created_at=row.created_at,
        expires_at=row.expires_at,
        released_at=row.released_at,
    )
