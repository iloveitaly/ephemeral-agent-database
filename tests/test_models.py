import pytest
from datetime import datetime, timezone
from uuid import uuid4

from ephemeral_agent_database.models import (
    ProvisionStatus,
    ProvisionRow,
    ProvisionRequest,
    ProvisionResponse,
    ProvisionSummary,
    HealthResponse,
)

def test_provision_status_enum():
    assert ProvisionStatus.PENDING == "pending"
    assert ProvisionStatus.ACTIVE == "active"
    assert ProvisionStatus.RELEASING == "releasing"
    assert ProvisionStatus.CLEANED == "cleaned"

def test_provision_request_defaults():
    req = ProvisionRequest()
    assert req.ttl_hours is None

def test_provision_request_validation():
    with pytest.raises(ValueError):
        ProvisionRequest(ttl_hours=0)
    req = ProvisionRequest(ttl_hours=12)
    assert req.ttl_hours == 12

def test_provision_row_creation():
    uid = uuid4()
    now = datetime.now(timezone.utc)
    row = ProvisionRow(
        id=uid,
        short_id="abc123xyz0",
        status=ProvisionStatus.PENDING,
        pg_db_name="db",
        pg_role_name="role",
        redis_user_name="redis_user",
        redis_db_number=0,
        created_at=now,
        expires_at=now,
    )
    assert row.id == uid
    assert row.short_id == "abc123xyz0"
    assert row.status == ProvisionStatus.PENDING

def test_health_response_creation():
    now = datetime.now(timezone.utc)
    resp = HealthResponse(
        status="ok",
        postgres_reachable=True,
        redis_reachable=True,
        active_provisions=5,
        redis_max_dbs=16,
        now=now,
    )
    assert resp.status == "ok"
    assert resp.active_provisions == 5
