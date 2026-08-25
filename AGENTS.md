# AGENTS.md

**This file provides guidance to several AI assisted coding tools when working with code in this repository.**

## What this is

A Terraform/OpenTofu provider for AWS Redshift, managing users, groups, schemas, grants, datashares, roles, etc. Published on the OpenTofu Registry as `dbsystel/redshift`. Built with `terraform-plugin-sdk/v2`. All provider code lives in the `redshift/` package; `main.go` just wires it into the plugin SDK server.

## Commands

- `make build` — `gofmt` then `go install` (builds and installs the provider binary).
- `make test` — `gofmt`, `go vet`, then `go test ./redshift` (unit tests only).
- `make vet` — `go vet` across the module.
- `make testacc` — acceptance tests (`TF_ACC=1 go test ./... -v -count=1 -timeout 120m`). **Requires a real, reachable Redshift cluster** — see the extensive env var list in README.md's "Running Tests" section (`REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `TF_ACC_TERRAFORM_PATH`, Data API vars, etc.). Acceptance tests self-skip with a log line if `TF_ACC` isn't set, so `go test ./redshift/...` runs safely without a cluster and only exercises true unit tests.
- Single test: `go test ./redshift/... -run TestName -v` (add `-race` for concurrency-sensitive tests). For acceptance tests, `TF_ACC` and the relevant `REDSHIFT_*` env vars must also be set.
- `make doc` — regenerates `docs/` from schema `Description` fields, `examples/`, and `templates/` via `go generate` + `tfplugindocs`. Requires a local `tofu` binary. Never hand-edit files under `docs/`.

## Architecture

### Two connection transports, one `Config`/`Client` abstraction

The provider talks to Redshift through one of two interchangeable transports, selected in `provider.go`'s `getConfigFromResourceData` based on whether `data_api` is set in the resource config:

- **libpq/proxy** (`config_pq_proxy.go`, `proxy_driver.go`) — a real Postgres wire-protocol connection via `github.com/lib/pq`, registered under driver name `postgresql-proxy` (`proxy_driver.go`). Wraps `pq` to support `ALL_PROXY`/SOCKS dialing for clusters only reachable from inside a VPC. Handles `temporary_credentials` (via `redshift:GetClusterCredentials`) and `session_parameters` (sent as the libpq `options` param, which silently overrides `PGOPTIONS`).
- **Data API** (`config_data_api.go`) — uses `github.com/mmichaelb/redshift-data-sql-driver` (driver name `redshift-data`) to talk to Redshift via the AWS Redshift Data API instead of a direct connection. No pooling (`maxConns` is always 1). Transactions are emulated as a sequence of individual statements, not real DB transactions — see the caveats in README.md if touching transactional behavior here.

Both paths converge on `NewConfig(driverName, connStr, database, maxConns)` in `config.go`, producing a `*Config` → `*Client`. Resource/data-source code never branches on transport; it only ever calls `client.Connect()`.

### Connection lifecycle (`config.go`)

`Client.Connect()` is the single entry point every resource/data-source op uses to get a `*DBConnection` (a `*sql.DB` wrapper). It's called concurrently — Terraform runs CRUD operations for different resources in parallel (`-parallelism`), all sharing one `*Client` set as the SDK's `meta`.

- A package-level `dbRegistry map[string]*DBConnection` caches one `*DBConnection` per DSN, guarded by `dbRegistryLock`. A second map, `dbConnectLocks`, holds a `*sync.Mutex` per DSN so that establishing a *new* connection for one DSN never blocks concurrent `Connect()` calls for a *different* DSN — only same-DSN callers serialize (and de-dupe: the first one to acquire the per-DSN lock does the real work, the rest hit the now-populated cache).
- `dbRegistryLock` itself is only ever held for a map read or write — never across `sql.Open`/network I/O.
- Cache hits are returned without any liveness probe (no `Ping`). This relies on `db.SetMaxIdleConns(0)` (set when a connection is first established) — the pool never holds an idle connection, so every real use redials fresh and a dead cluster surfaces as a normal query error to the caller. If `MaxIdleConns` is ever raised above 0, this reasoning breaks and a liveness check would need to come back.
- `Config.GetUsername` memoizes the connected user (`SELECT current_user`) per `Config` via its own `usernameRetrievalMutex`, independent of the connection registry.

### Resource wiring (`helpers.go`)

`ResourceFunc(fn func(*DBConnection, *schema.ResourceData) error)` adapts the simpler `(*DBConnection, *schema.ResourceData) error` signature used throughout `resource_redshift_*.go`/`data_source_redshift_*.go` into the SDK's `CreateContext`/`ReadContext`/etc. signature, calling `client.Connect()` internally. Nearly every resource's CRUD functions are registered through this wrapper — start here when tracing how a resource operation reaches the database.

`startTransaction(client)` similarly wraps `client.Connect()` + `db.Begin()` for code paths that need a real `*sql.Tx` (only meaningful on the libpq/proxy transport, since Data API transactions aren't real transactions).

`ResourceRetryOnPQErrors` retries an operation (default 10x with increasing backoff) on specific retryable Postgres error codes (deadlock, concurrent update, etc.) — used to wrap `DeleteContext` in several resources where Redshift's concurrency model produces spurious conflicts.

### Provider schema (`provider.go`)

The three mutually-exclusive connection methods (`host`+`password`/`temporary_credentials`, `data_api`) are enforced via `ConflictsWith` in the schema, plus a defence-in-depth check in `getConfigFromResourceData`. When adding a new provider-level argument, check whether it's meaningful for both transports — several existing arguments (`connect_timeout`, `sslmode`, `port`) are libpq-only but deliberately don't `ConflictsWith` `data_api`, since their `DefaultFunc` always returns a value and the SDK would otherwise report a false conflict for every Data API user.

### Resource file structure

Each `resource_redshift_*.go` / `data_source_redshift_*.go` follows the same shape: a `schema.Resource` returned from a `redshiftXxx()` constructor, `*Attr` constants for schema keys, CRUD functions taking `(*DBConnection, *schema.ResourceData) error`, and raw SQL built with `pqQuoteLiteral`/`pq.QuoteIdentifier` (never string-interpolated user input directly). `validation.go` holds shared `ValidateFunc`/`ValidateDiagFunc` helpers; `custom_diff.go` holds shared `CustomizeDiffFunc` helpers (currently just `forceNewIfListSizeChanged`).
