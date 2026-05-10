[![Release Notes](https://img.shields.io/github/release/iloveitaly/ephemeral-agent-database)](https://github.com/iloveitaly/ephemeral-agent-database/releases)
[![Downloads](https://static.pepy.tech/badge/ephemeral-agent-database/month)](https://pepy.tech/project/ephemeral-agent-database)
![GitHub CI Status](https://github.com/iloveitaly/ephemeral-agent-database/actions/workflows/build_and_publish.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# ephemeral-agent-database

A FastAPI service that hands out short-lived postgres databases and redis logical users to preview/staging environments, with automatic cleanup after a configurable TTL.

Designed for the "I need a fresh DB for this preview env and I don't want to manage its lifecycle" use case. Agents like Jules, Claude Code Web, etc. can POST `/provision` to get a one-shot `DATABASE_URL` + `REDIS_URL` pair and not worry about teardown — the service expires them after 24h.

## Config

Three env vars, no more:

| Variable | Format | Notes |
|---|---|---|
| `DATABASE_URL` | `postgresql://superuser:pw@host:5432/postgres` | Superuser on the target cluster. Must be a **direct** connection, not through a pooler. |
| `REDIS_URL` | `redis://:pw@host:6379` | Admin connection. The service calls `ACL SETUSER`. |
| `HTTP_BASIC_AUTH` | `username:password` | Credentials for the service's own API. |

## Run it

```bash
uv sync
uv run uvicorn ephemeral_agent_database.main:app --host 0.0.0.0 --port 8000
```

## API

All endpoints except `/healthcheck` require HTTP Basic auth.

### `GET /healthcheck`
Returns service status and active provisions.

### `POST /provision`
Request body (optional): `{ "ttl_hours": 24 }`
Response gives tenant owner-level access to their own dedicated postgres database and a redis URL.

### `GET /provisions`
Returns a list of all provisions (credentials omitted).

### `GET /provisions/{id}`
Single provision metadata.

### `DELETE /provisions/{id}`
Async release. Tears down the resources on the next pass.

## Deployment notes (Railway)

- Use the **direct/non-pooled** postgres URL.
- Deploy your own redis from a Dockerfile (`redis-server --databases 64 --save "" --appendonly no`).

---

*This project was created from [iloveitaly/python-package-template](https://github.com/iloveitaly/python-package-template)*