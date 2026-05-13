"""Identifier generation and validation.

Only generated short_ids that pass `validate_short_id` should ever be interpolated into DDL.
The regex is the safety boundary: identifiers in postgres and redis can't be parameterized,
so we compose them from a fixed alphabet that contains no SQL/ACL metacharacters.
"""

import re
import secrets

SHORT_ID_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
SHORT_ID_LENGTH = 10

SHORT_ID_RE = re.compile(rf"^[a-z0-9]{{{SHORT_ID_LENGTH}}}$")


def generate_short_id() -> str:
    """10 chars of [a-z0-9] -> ~51.7 bits of entropy. Collision-safe at this scale."""
    return "".join(secrets.choice(SHORT_ID_ALPHABET) for _ in range(SHORT_ID_LENGTH))


def validate_short_id(s: str) -> None:
    if not isinstance(s, str) or not SHORT_ID_RE.match(s):
        raise ValueError(f"invalid short_id: {s!r}")


def pg_db_name(prefix: str, short_id: str) -> str:
    validate_short_id(short_id)
    return f"{prefix}_{short_id}"


def pg_role_name(prefix: str, short_id: str) -> str:
    validate_short_id(short_id)
    return f"{prefix}_user_{short_id}"


def redis_user_name(prefix: str, short_id: str) -> str:
    validate_short_id(short_id)
    return f"{prefix}_{short_id}"
