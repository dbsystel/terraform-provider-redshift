# AGENTS.md

**This file provides guidance to several AI assisted coding tools when working with code in this repository.**

## What this is

A Terraform/OpenTofu provider for AWS Redshift, managing users, groups, schemas, grants, datashares, roles, etc. Published on the OpenTofu Registry as `dbsystel/redshift`. Built with `terraform-plugin-framework` (protocol 6). All provider code lives in the `redshift/` package; `main.go` just serves it.

## Commands

- `make build` — `gofmt` then `go install` (builds and installs the provider binary).
- `make test` — `gofmt`, `go vet`, then `go test ./redshift` (unit tests only).
- `make vet` — `go vet` across the module.
- `make testacc` — acceptance tests (`TF_ACC=1 go test ./... -v -count=1 -timeout 120m`). **Requires a real, reachable Redshift cluster** — see the extensive env var list in README.md's "Running Tests" section (`REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `TF_ACC_TERRAFORM_PATH`, Data API vars, etc.). Acceptance tests self-skip with a log line if `TF_ACC` isn't set, so `go test ./redshift/...` runs safely without a cluster and only exercises true unit tests.
- Single test: `go test ./redshift/... -run TestName -v` (add `-race` for concurrency-sensitive tests). For acceptance tests, `TF_ACC` and the relevant `REDSHIFT_*` env vars must also be set.
- `make doc` — regenerates `docs/` from schema `Description` fields, `examples/`, and `templates/` via `go generate` + `tfplugindocs`. Requires a local `tofu` binary. Never hand-edit files under `docs/`.

## Architecture

### Two connection transports, one `Config`/`Client` abstraction

The provider talks to Redshift through one of two interchangeable transports, selected in `config.go`'s `providerSettings.newConfig` based on whether `data_api` is set in the provider configuration:

- **libpq/proxy** (`config_pq_proxy.go`, `proxy_driver.go`) — a real Postgres wire-protocol connection via `github.com/lib/pq`, registered under driver name `postgresql-proxy` (`proxy_driver.go`). Wraps `pq` to support `ALL_PROXY`/SOCKS dialing for clusters only reachable from inside a VPC. Handles `temporary_credentials` (via `redshift:GetClusterCredentials`) and `session_parameters` (sent as the libpq `options` param, which silently overrides `PGOPTIONS`).
- **Data API** (`config_data_api.go`) — uses `github.com/mmichaelb/redshift-data-sql-driver` (driver name `redshift-data`) to talk to Redshift via the AWS Redshift Data API instead of a direct connection. No pooling (`maxConns` is always 1). Transactions are emulated as a sequence of individual statements, not real DB transactions — see the caveats in README.md if touching transactional behavior here.

Both paths converge on `NewConfig(driverName, connStr, database, maxConns)` in `config.go`, producing a `*Config` → `*Client`. Resource/data-source code never branches on transport; it only ever calls `client.Connect()`.

### Connection lifecycle (`config.go`)

`Client.Connect()` is the single entry point every resource/data-source op uses to get a `*DBConnection` (a `*sql.DB` wrapper). It's called concurrently — Terraform runs CRUD operations for different resources in parallel (`-parallelism`), all sharing the one `*Client` the provider hands out in `Configure`.

- A package-level `dbRegistry map[string]*DBConnection` caches one `*DBConnection` per DSN, guarded by `dbRegistryLock`. A second map, `dbConnectLocks`, holds a `*sync.Mutex` per DSN so that establishing a *new* connection for one DSN never blocks concurrent `Connect()` calls for a *different* DSN — only same-DSN callers serialize (and de-dupe: the first one to acquire the per-DSN lock does the real work, the rest hit the now-populated cache).
- `dbRegistryLock` itself is only ever held for a map read or write — never across `sql.Open`/network I/O.
- Cache hits are returned without any liveness probe (no `Ping`). This relies on `db.SetMaxIdleConns(0)` (set when a connection is first established) — the pool never holds an idle connection, so every real use redials fresh and a dead cluster surfaces as a normal query error to the caller. If `MaxIdleConns` is ever raised above 0, this reasoning breaks and a liveness check would need to come back.
- `Config.GetUsername` memoizes the connected user (`SELECT current_user`) per `Config` via its own `usernameRetrievalMutex`, independent of the connection registry.

### Resource wiring (`framework.go`, `helpers.go`)

Every resource and data source embeds `frameworkClient` (`framework.go`): `Configure` stores the `*Client` the provider handed out, and `connect(&resp.Diagnostics)` opens a connection from it, reporting failures as diagnostics. Start there when tracing how a resource operation reaches the database.

`framework.go` also holds the shared schema behaviour: `normalizeString`/`normalizeSet` (store a normalized form of the configured value), `scaleInt64`, `ignoreChangesAfterCreate`, `requiresReplaceIfListSizeChanged`, `regexDoesNotMatch`, and the small helpers that convert framework values to and from Go slices and maps.

`startTransaction(client)` (`helpers.go`) wraps `client.Connect()` + `db.Begin()` for code paths that need a real `*sql.Tx` (only meaningful on the libpq/proxy transport, since Data API transactions aren't real transactions).

`retryOnPQErrors` retries an operation (10x with increasing backoff, honouring the context) on specific retryable Postgres error codes (deadlock, concurrent update, etc.) — used in `Delete` in several resources where Redshift's concurrency model produces spurious conflicts.

### Provider schema (`fwprovider.go`)

The three mutually-exclusive connection methods (`host`+`password`/`temporary_credentials`, `data_api`) are enforced with `ConflictsWith` validators, plus a defence-in-depth check in `providerSettings.newConfig`. The framework has no `DefaultFunc`, so environment variable defaults are resolved in `Configure` through `stringWithEnvDefault`/`int64WithEnvDefault`; `settings()` turns the provider model into the transport-agnostic `providerSettings`. When adding a new provider-level argument, check whether it's meaningful for both transports — `connect_timeout`, `sslmode` and `port` are libpq-only and simply unused by the Data API transport.

### Resource file structure

Each `resource_redshift_*.go` / `data_source_redshift_*.go` follows the same shape: a `newXxxResource()`/`newXxxDataSource()` constructor registered in `fwprovider.go`, a struct embedding `frameworkClient`, a `xxxResourceModel` with `tfsdk:` tags, `*Attr` constants for schema keys, `Metadata`/`Schema`/`Configure` and the CRUD methods, and raw SQL built with `pqQuoteLiteral`/`pq.QuoteIdentifier` (never string-interpolated user input directly). Every resource declares its own `id` attribute: the framework adds none.

Create and Update store the planned values and resolve what the plan left unknown; reading back from the cluster in the same operation would risk the framework's plan and apply consistency check. `Read` is where state is refreshed from the cluster. `validation.go` holds the shared regular expressions and reserved words.
