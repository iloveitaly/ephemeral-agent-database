"""Pydantic models for API and internal use."""

from datetime import datetime
from enum import StrEnum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ProvisionStatus(StrEnum):
    PENDING = "pending"       # row inserted, resources being created
    ACTIVE = "active"         # fully provisioned, client has creds
    RELEASING = "releasing"   # expired or user-released; cleanup in progress
    CLEANED = "cleaned"       # all resources gone


class ProvisionRow(BaseModel):
    """Internal representation of a control-table row."""
    id: UUID
    short_id: str
    status: ProvisionStatus
    pg_db_name: str
    pg_role_name: str
    redis_user_name: str
    redis_db_number: int
    created_at: datetime
    expires_at: datetime
    released_at: Optional[datetime] = None
    pg_dropped_at: Optional[datetime] = None
    redis_cleaned_at: Optional[datetime] = None


class ProvisionRequest(BaseModel):
    ttl_hours: Optional[int] = Field(
        default=None,
        ge=1,
        description="Provision lifetime in hours. Defaults to DEFAULT_TTL_HOURS, capped at MAX_TTL_HOURS.",
    )


class ProvisionResponse(BaseModel):
    """Returned only at creation. Subsequent GETs omit credentials."""
    id: UUID
    short_id: str
    database_url: str
    redis_url: str
    redis_key_prefix: str = Field(
        ..., description="All redis keys written by this tenant must start with this prefix."
    )
    created_at: datetime
    expires_at: datetime


class ProvisionSummary(BaseModel):
    """Credential-free representation for list/get endpoints."""
    id: UUID
    short_id: str
    status: ProvisionStatus
    created_at: datetime
    expires_at: datetime
    released_at: Optional[datetime]


class HealthResponse(BaseModel):
    status: str
    postgres_reachable: bool
    redis_reachable: bool
    active_provisions: int
    redis_max_dbs: int
    now: datetime
