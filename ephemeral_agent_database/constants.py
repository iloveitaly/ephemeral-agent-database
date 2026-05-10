"""Hardcoded constants. Env vars stay minimal: DATABASE_URL, REDIS_URL, HTTP_BASIC_AUTH."""

RESOURCE_PREFIX = "ephemeral"
CONTROL_DB_NAME = "ephemeral_control"

DEFAULT_TTL_HOURS = 24
MAX_TTL_HOURS = 168  # 1 week

CLEANUP_INTERVAL_SECONDS = 300  # 5 minutes
PROVISION_PENDING_TIMEOUT_SECONDS = 300  # unfinished provisions swept after this
