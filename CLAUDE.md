# Claude Guide — business-analytics-api

Last reviewed: 2026-04-17

Go + Gin + GORM + PostgreSQL backend for ESC Auto Group's internal business analytics, accounting, and document-matching platform. Single service. The frontend lives in a separate repo (its host is in the CORS allowlist in `internal/middleware/cors.go`).

---

## What This Product Is

Financial-operations API for a multi-location automotive business (10 shops — see `services.ShopIDs`). Core surfaces:

- **Dashboard** — bank balances, AP/AR, credit-card balances, aging, revenue vs. expense, low-balance and due-soon signals
- **Document pipeline** — PDF intake → multi-agent OCR (classifier → extractor → validator → matcher) → vendor matching → AP posting
- **Part matching** — invoice line items ↔ RO line items with vendor-prefix normalization, OCR-substitution tolerance, and an AI tiebreaker
- **Reconciliation** — bank statement ↔ transaction daily match, statement-completeness scoring
- **Xero integration** — two-way sync of bank transactions, invoices, payments, contacts, journals, accounts, reports (rate-limited)
- **Plaid integration** — bank account linking, cursor-based transaction sync, daily balance snapshots, re-auth handling
- **Tekmetric integration** — RO + customer + job import, parallel shop fan-out
- **SMS API integration** — outbound calls to `sms-api.carshopanalytics.com` for RO-side data
- **AP / AR / Paybills** — vendor bill workflow (manual + auto sources), receivables, credits, scheduled payments, reminders
- **Assets** — CSV import, AI classification, depreciation calculation
- **Auth + 2FA** — JWT with HS256; mobile OTP, email verification, authenticator-app TOTP

---

## Runtime Shape

- Go 1.25, Gin HTTP engine, GORM (with a narrow `pgx/pgxpool` path in `database.Connect`)
- Single entrypoint: `cmd/server/main.go` → `routes.Register()` mounts everything
- CORS wraps Gin at the `http.Handler` level so trailing-slash 301 redirects still carry CORS headers (see `middleware/cors.go`)
- Cron scheduler is started from `main.go` and runs syncs, alerts, and snapshots (`internal/cron/scheduler.go`)
- GORM `AutoMigrate` runs on startup against a fixed model batch in `database.ConnectGORM`; some handlers own additional tables and call their own `AutoMigrate()` at construction time

---

## Directory Layout

```
cmd/server/              entrypoint — builds config, DB, routes, cron
internal/
  config/                env-backed Config (Load reads .env + os.Getenv)
  database/              GORM + pgxpool connect; startup AutoMigrate batch
  middleware/            JWT auth + CORS
  routes/                single router registry — all endpoints registered here
  handlers/              HTTP handlers (primary business-logic layer, 40+ files)
  services/              cross-system clients (SMS API, part-matching engine)
  models/                single-file GORM schema (~1k lines)
  cron/                  scheduled sync/alert/snapshot jobs
  notifications/         email sending + alert checks
  xero/                  Xero rate limiter (singleton)
  migrations/            reference SQL (GORM auto-migrates at runtime; these are not applied)
  testutil/              test DB + JWT helpers
scripts/                 one-shot seed SQL (e.g. b1bank import)
```

---

## Layer Rules

1. **`routes/router.go` is the single source of truth for endpoints.** New endpoint → add it to `Register()`. Don't register routes from inside handlers or from `main.go`.
2. **Handlers own business logic.** The `services/` package is deliberately thin — it holds cross-system HTTP clients (SMS API, part-matching engine). Most workflow logic lives on handler methods.
3. **Models live in one file** (`models/models.go`). Keep the existing section-header groupings, and add `TableName()` for every type.
4. **New tables** — two paths:
   - Add the struct pointer to the batch in `database.ConnectGORM` so it auto-migrates on every start, OR
   - If the table is owned by a single handler, give that handler an `AutoMigrate()` method and call it from `routes.Register()`. Existing examples: `APHandler`, `MatchingHandler`, `ReceivablesHandler`, `DocumentMatchHandler`, `PlaidHandler`.
   - `internal/migrations/*.sql` are reference/setup artifacts — they are **not** applied at runtime.
5. **DB access** via the injected `*gorm.DB` (or `h.sqlDB()` for raw SQL on handlers that expose it). Don't open new connections in feature code.
6. **Auth** is JWT via `middleware.Auth(secretKey)`. Never add endpoint-local bypasses. Tokens come from `Authorization: Bearer …` or `?token=…` query. The query-param path exists specifically for `<iframe>` / `<img>` file previews — preserve it if you touch file-serving endpoints.
7. **CORS origins** are a hard-coded allowlist in `middleware/cors.go`. Adding a frontend host = edit the map.
8. **Secrets stay server-side.** Fields like `HashedPassword`, `AccessToken`, `RefreshToken`, `OTP`, `AuthenticatorSecret`, `ResetPasswordToken`, `PlaidAccessToken` carry `json:"-"`. Preserve that for any new sensitive field.

---

## Cron Jobs

Everything scheduled runs in `internal/cron/scheduler.go`. Every job must be wrapped in one of:

- `wrapSimpleJob(name, fn)` — panic recovery + start/complete logging
- `wrapJob(h, name, fn)` — fetches an active Xero connection, skips if none, panic-recovered
- `wrapSyncAll(h)` — daily full Xero resync

Current schedule (UTC, standard 5-field cron):

- `*/15` — Plaid transaction sync (cursor-based)
- `*/30` — Xero bank transactions, invoices, payments, contacts; document auto-match
- `0 *` — Xero journals + tracking categories
- `5 *` — Xero accounts (staggered to dodge rate-limit contention at `:00`)
- `0 2` — Xero full resync (clears sync state)
- `30 2` / `35 2` — customer-number extraction → location resolution chain
- `0 7` — overdue-bills + low-balance alert emails
- `0 7 * * 1` — weekly reconciliation alert (Mondays)
- `0 23` — daily balance snapshot

A balance snapshot also runs once in a panic-recovered goroutine at startup.

---

## Xero Rate Limiting

**Every outbound Xero API call goes through `xero.GetRateLimiter().WaitForSlot()` before firing.** The limiter enforces 55 calls / rolling minute and 4800 calls / UTC day (well under Xero's 60/min, 5000/day caps). `WaitForSlot()` blocks until the minute window resets; a `false` return means the daily cap is exhausted — stop until tomorrow, don't retry.

Currently enforced in `handlers/xero_sync.go`. Preserve the pattern when adding new Xero call sites.

---

## External Integrations

| System | Config env vars | Code |
|---|---|---|
| Xero | `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET`, `XERO_REDIRECT_URI` | `handlers/xero*.go`, `internal/xero/` |
| Plaid | `PLAID_CLIENT_ID`, `PLAID_SECRET`, `PLAID_ENV` | `handlers/plaid.go`, `handlers/balance_snapshot.go` |
| Tekmetric | `TEKMETRIC_CLIENT_ID/SECRET/BASE_URL/BASE64_AUTH_KEY` | `handlers/tekmetric.go` |
| SMS API | `SMS_API_BASE_URL/EMAIL/PASSWORD` | `services/sms_api.go` |
| Anthropic | `ANTHROPIC_API_KEY` | `handlers/assets_ai.go`, `document_pipeline.go`, `vision_extractor.go`, `services/part_matching.go` |
| WickedFile | `WICKEDFILE_API_URL/KEY/EMAIL_INTAKE` | `handlers/documents.go`, `handlers/document_*.go` |
| SMTP | `SMTP_HOST/PORT/USER/PASS/FROM` | `internal/notifications/` |
| AWS / S3 | `AWS_REGION`, `DOCUMENTS_S3_BUCKET` | `handlers/documents.go` |
| PDF store | `WF_PDF_DIR` (default `/var/www/html/wf-pdfs`) | `config.pdfDir()` |

---

## Shops

Ten shop IDs are hard-coded in `services.ShopIDs`: `Alpharetta`, `Piedmont`, `Tracy`, `Duluth`, `Houston`, `Roswell`, `Cedar Springs`, `Preston`, `Highlands`, `Sandy Springs`. Adding or renaming a shop likely touches Tekmetric fan-out, reconciliation, and dashboard queries — grep the codebase for the ID before assuming a one-line change.

---

## Local Dev

Prereqs: Go 1.25+, PostgreSQL, `psql` on `PATH`.

```bash
# first run
cp .env.example .env              # fill in DATABASE_URL + integration keys

# build and run
make build                        # -> ./server
make run                          # go run ./cmd/server/

# tests (DB-backed tests skip unless TEST_DATABASE_URL is set)
make test

# one-time: create the test DB
make test-db
TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/autoaccount_test make test
```

Default server port is `8080` unless `PORT` is set.

A large `./server` binary is committed at the repo root (~42 MB). `make clean` removes it.

---

## Verification

Minimum after any change:

```bash
go build ./...
```

Stricter:

```bash
make test
go vet ./...
```

When touching DB-backed handlers, point `TEST_DATABASE_URL` at a scratch Postgres so `testutil.SetupDB(t)` doesn't skip the test.

---

## Testing Notes

- Shared helpers in `internal/testutil/setup.go`. `SetupDB(t)` skips when `TEST_DATABASE_URL` is unset — intentional so `go test ./...` stays green everywhere.
- Test JWT secret is `testutil.TestSecret`; use `testutil.AuthedRequest()` to build authenticated requests.
- Existing coverage: `handlers/vendors_test.go`, `handlers/documents_test.go`, `handlers/dashboard_layouts_test.go`. Keep new tests in the same package with the `_test.go` suffix.

---

## Logging

Unstructured `log.Printf` throughout. No request IDs, no structured fields. Existing prefix conventions worth matching:

```
log.Printf("Cron %s: starting", name)
log.Printf("EMAIL SENT: to=%s subject=%s", to, subject)
log.Printf("WARN: AutoMigrate %T: %v", model, err)
```

Panics in goroutines and cron jobs **must** be recovered — use the `cron/scheduler.go` `wrap*` helpers or an explicit `defer func(){ recover() }()`.

---

## Known Sharp Edges

- **AutoMigrate is optimistic.** A failing `AutoMigrate` is logged `WARN:` and the server keeps starting. Confirm the schema actually applied before relying on a new column.
- **Route registration has migration side effects.** `PlaidHandler`, `APHandler`, `MatchingHandler`, `ReceivablesHandler`, `DocumentMatchHandler` each call `AutoMigrate()` when constructed in `routes.Register()`. Reordering or skipping handler construction can miss a table.
- **Trailing slashes matter.** Gin emits 301s on mismatches; CORS wrapping exists so redirects carry headers. If a frontend reports a CORS failure on a redirect, align the declared path rather than touching CORS.
- **`internal/migrations/*.sql` is reference.** GORM is what actually applies schema at runtime. If the two disagree, trust the models.
- **Xero full resync at 02:00 UTC clears sync state** — expect transient gaps if you inspect mid-resync.
- **Daily balance snapshot runs on startup.** Fast restarts will log multiple snapshot runs, but the write is idempotent per-day.
- **Query-param JWT exists for file previews.** Removing that path in `middleware/auth.go` breaks `<iframe>` / `<img>` document previews.

---

## Subfolder Guides

Read the closer `CLAUDE.md` when working in:

- `./internal/handlers/` — primary business-logic layer
- `./internal/routes/` — endpoint registry
- `./internal/models/` — schema and migration flow
- `./internal/cron/` — scheduled jobs
- `./internal/xero/` — rate limiter

---

## Safety

- Don't push, force-push, or rewrite published branches without explicit ask.
- Don't commit `.env` files or populated integration credentials.
- Preserve unrelated dirty worktree state; investigate unfamiliar files/branches before overwriting.
- Don't add endpoint-local auth bypasses, even temporarily.

---

## Keeping CLAUDE.md Files Current

When a change adds or removes an endpoint group, cron job, integration, or table:

1. Update the nearest `CLAUDE.md` in the same PR.
2. If a new `internal/` subpackage gains real structure, add a `CLAUDE.md` to it.
3. If docs and code disagree, trust the code and fix the doc in the same change.
