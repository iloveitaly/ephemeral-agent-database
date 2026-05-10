"""URL manipulation helpers. Used both to route admin connections and to build
the credentials returned to clients at provision time.
"""

from urllib.parse import quote, urlparse, urlunparse


def postgres_url_with_db(url: str, db_name: str) -> str:
    """Return a copy of `url` with its database path replaced by `db_name`."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{db_name}"))


def postgres_url_with_credentials(
    url: str, db_name: str, user: str, password: str
) -> str:
    """Return a copy of `url` with user/password/db swapped for the client."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{quote(user, safe='')}:{quote(password, safe='')}@{host}{port}"
    return urlunparse(parsed._replace(netloc=netloc, path=f"/{db_name}"))


def redis_url_with_credentials(
    url: str, db_number: int, user: str, password: str
) -> str:
    """Return a copy of `url` with ACL user/password/db swapped for the client.

    Redis URLs carry the DB as a path: redis://host:port/<db>.
    """
    parsed = urlparse(url)
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{quote(user, safe='')}:{quote(password, safe='')}@{host}{port}"
    return urlunparse(parsed._replace(netloc=netloc, path=f"/{db_number}"))
