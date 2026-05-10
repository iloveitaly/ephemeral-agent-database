# ephemeral-provisioner

A FastAPI service that hands out short-lived postgres databases and redis logical users to preview/staging environments, with automatic cleanup after a configurable TTL.

Designed for the "I need a fresh DB for this preview env and I don't want to manage its lifecycle" use case. Agents like Jules, Claude Code Web, etc. can POST `/provision` to get a one-shot `DATABASE_URL` + `REDIS_URL` pair and not worry about teardown — the service expires them after 24h.

## Config

Three env vars, no more:

| Variable | Format | Notes |
|---|---|---|
| `DATABASE_URL` | `postgresql://superuser:pw@host:5432/postgres` | Superuser on the target cluster. Must be a **direct** connection, not through a pooler — `CREATE DATABASE` can't run inside a transaction and pgbouncer in transaction mode will reject it. |
| `REDIS_URL` | `redis://:pw@host:6379` | Admin connection. The service calls `ACL SETUSER`, so the user on this URL needs ACL privileges. |
| `HTTP_BASIC_AUTH` | `username:password` | Credentials for the service's own API. Colons in the password are preserved. |

The admin URLs are also the hostnames returned to clients in the credentials they receive — if agents will run outside the cluster's network, use publicly reachable hostnames here.

The following are hardcoded in `app/constants.py`: resource prefix (`ephemeral`), control DB name (`ephemeral_control`), default TTL (24h), max TTL (168h), cleanup interval (5 min), stale-provision timeout (5 min).

## Run it

```
uv sync
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

On first startup the service will:
1. Connect to postgres as superuser and create an `ephemeral_control` database if missing.
2. Create the `provisions` table in that DB.
3. Run a reconcile pass — any `ephemeral_*` postgres DB/role or redis ACL user without a matching control row is torn down.
4. Start the background cleanup loop.

## API

All endpoints except `/healthcheck` require HTTP Basic auth.

### `GET /healthcheck`

No auth. Returns service status and the count of currently active provisions.

```json
{
  "status": "ok",
  "postgres_reachable": true,
  "redis_reachable": true,
  "active_provisions": 7,
  "redis_max_dbs": 64,
  "now": "2026-04-18T19:30:00Z"
}
```

### `POST /provision`

Request body (all fields optional):
```json
{ "ttl_hours": 24 }
```

Response `201`:
```json
{
  "id": "8c4a...",
  "short_id": "abc123xyz0",
  "database_url": "postgresql://ephemeral_user_abc123xyz0:password@host:5432/ephemeral_abc123xyz0",
  "redis_url": "redis://ephemeral_abc123xyz0:password@host:6379/0",
  "redis_key_prefix": "ephemeral_abc123xyz0:",
  "created_at": "2026-04-18T19:30:00Z",
  "expires_at": "2026-04-19T19:30:00Z"
}
```

The `database_url` gives the tenant owner-level access to their own dedicated postgres database. They can do whatever they want inside it — create tables, users, extensions — and all of it gets dropped when the provision expires.

**Redis isolation is per-key-prefix, not per-DB.** All tenants share DB 0; each tenant's ACL user is scoped to keys and pubsub channels matching `<user>:*`. The tenant **must** prefix every key with `redis_key_prefix` (returned in the response). Unprefixed keys hit `NOPERM`.

Why key-prefix instead of per-DB: redis clients issue `SELECT N` on connect when the URL has `/N` in the path. To lock a tenant to a single DB we'd have to deny `-select`, but then the client can't connect. Allowing `+select` lets tenants hop between DBs. Key-prefix isolation is enforced by the server regardless of DB and doesn't interact with client behavior.

### `GET /provisions`

Returns a list of all provisions (credentials omitted — those are only returned at creation time). Useful for debugging.

### `GET /provisions/{id}`

Single provision metadata. Credentials omitted.

### `DELETE /provisions/{id}`

Async release. Marks the row as `releasing`; the background loop tears down the resources on the next pass. Returns `202`.

Idempotent — deleting an already-released provision is a no-op.

## Lifecycle

Each provision has a status:

- `pending` — row inserted, resources being created. If this phase fails mid-flight, the cleanup loop will sweep it to terminal state after 5 minutes.
- `active` — fully provisioned, client holds credentials.
- `releasing` — either `expires_at` has passed or the client called `DELETE`. The cleanup loop will attempt to drop resources on its next pass.
- `cleaned` — postgres DB and redis user both torn down. The row is retained for audit.

If a cleanup attempt fails for one resource (say, redis is temporarily unreachable), the row stays in `releasing` with `pg_dropped_at` set but `redis_cleaned_at` still null, and the next pass retries only the side that's outstanding.

## Reconciliation

At startup the service compares its control table against what's actually in the postgres cluster and redis server:
- Any `ephemeral_*` DB or role in postgres without a matching control row → dropped.
- Any `ephemeral_*` redis ACL user without a matching control row → deleted.

This handles mid-crash recovery cleanly. The control DB itself is explicitly excluded from the sweep (it shares the prefix but is not a tenant resource).

## Testing

Requires a local postgres and redis reachable at:
- `postgresql://postgres:postgres@localhost:5432/postgres`
- `redis://localhost:6379` (started with `--databases 64`)

Override via `TEST_DATABASE_URL` / `TEST_REDIS_URL` env vars.

```
uv run pytest
```

82 tests covering unit logic (naming, url manipulation, config), postgres provisioner, redis provisioner (including cross-tenant access denial), control table CRUD, cleanup + reconcile, and end-to-end HTTP flows.

Each test gets isolated state via a unique `RESOURCE_PREFIX` and a unique control DB name monkeypatched into `app.constants` and the modules that reference it. Resources matching the prefix are swept before and after each test.

## What it doesn't do

- No per-API-key auth model — single basic auth credential.
- No `PATCH /provisions/{id}/extend` — clients can't extend a TTL; they provision a new one.
- No metrics endpoint.
- No `aclfile` support for persisted redis ACLs. If redis restarts, in-flight provisions become unusable. The reconcile pass will clean them up on the service's next start.
- No redis cluster support — cluster mode doesn't allow multiple databases anyway, and the ACL model differs.

## Deployment notes (Railway)

- Use the **direct/non-pooled** postgres URL. Pgbouncer in transaction mode rejects `CREATE DATABASE`.
- Deploy your own redis from a Dockerfile (`redis-server --databases 64 --save "" --appendonly no`) rather than Railway's template, so you control the config.
- The service URLs in the env vars must be reachable from wherever clients run. On Railway, that means the public proxy host, not `*.railway.internal`.
