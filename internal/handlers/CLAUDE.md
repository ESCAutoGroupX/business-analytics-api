# Handlers Claude Guide

The primary business-logic layer. Every routable feature lives here. The `services/` package is deliberately thin (cross-system clients only).

## Handler Shape

The convention across the package:

```go
type FooHandler struct {
    GormDB    *gorm.DB
    Cfg       *config.Config    // when the handler reads env config
    SecretKey string             // auth/2FA handlers only
    // additional injected deps as needed
}

func (h *FooHandler) CreateFoo(c *gin.Context) { ... }
```

Handlers are constructed (by pointer) in `routes.Register()` and each method is mounted onto a Gin router group. Dependencies are set via struct fields, not function parameters.

## Domain Files (large files worth reading first)

- `dashboard.go` (~2.2k lines) — bank balance, AP, AR, credit cards, revenue, ledgers
- `matching_engine.go` (~2.2k) — statement ↔ transaction matching, vendor/location scoring, force-match, missing-pages review
- `documents.go` (~1.7k) — document CRUD, upload, rescan, AP workflow, WickedFile integration
- `xero_sync.go` (~1.1k) — per-endpoint Xero incremental + full resync
- `document_pipeline.go` (~1k) — 4-agent OCR pipeline (classifier → extractor → validator → matcher), prefix `multi-agent-v1`
- `paybills.go` (~1k) — scheduled payments, reminders, manual bills

Smaller domain files (`auth.go`, `vendors.go`, `permissions.go`, `users.go`, `assets.go`, `reports.go`, etc.) tend to be CRUD + modest policy.

## Rules

1. **Mount new endpoints in `routes/router.go`.** Don't `r.GET(...)` from inside a handler.
2. **Never create new DB engines.** Use the injected `h.GormDB`, or the `h.sqlDB()` helper on handlers that expose it for raw SQL.
3. **JWT/auth is enforced at the router group.** Handlers read `c.Get("user_id")`, `c.Get("email")`, `c.Get("role")` off the Gin context.
4. **Query-param token path exists for file-preview endpoints** (see `middleware/auth.go`). If you serve file bytes to a browser via `<iframe>` / `<img>`, relying on `?token=` is valid; otherwise prefer the `Authorization` header.
5. **Handler-owned AutoMigrate.** `APHandler`, `MatchingHandler`, `ReceivablesHandler`, `DocumentMatchHandler`, `PlaidHandler` each have an `AutoMigrate()` method called from `routes.Register()`. Match that pattern when a handler owns domain-local tables, OR add the model to the batch in `database.ConnectGORM`.
6. **Panic-recover long-lived goroutines.** Anything that outlives the request (background fan-out, async work) must `defer recover()` or use the `cron/scheduler.go` wrappers.
7. **Error envelope.** Respond with `gin.H{"error": ...}` on failure — the frontend parses `.error`. Don't invent new error shapes.

## External-Integration Handlers

- **Xero** (`xero.go`, `xero_sync.go`, `xero_api.go`) — every outbound Xero call MUST pass `xero.GetRateLimiter().WaitForSlot()` before firing. A `false` return means the daily cap hit; skip until UTC midnight.
- **Plaid** (`plaid.go`, `balance_snapshot.go`) — cursor-based sync; `ExchangePublicToken` creates `plaid_items`; re-auth-flagged items are skipped by snapshot and sync.
- **Tekmetric** (`tekmetric.go`) — OAuth access token fetched per call-group; shop IDs come from `services.ShopIDs`; parallel customer fetch exists at `/tekmetric/custo`.
- **AI endpoints** (`assets_ai.go`, `customer_number_extractor.go`, `vision_extractor.go`, `document_pipeline.go`) — hit Anthropic directly via `http.Client`. Keep timeouts and error logging; prefer async/background patterns over blocking request paths on long model calls.

## Validation

```bash
go build ./...
go vet ./internal/handlers/...
TEST_DATABASE_URL=... go test ./internal/handlers/...
```

If a handler's response contract changes (shape, required fields, auth behavior), audit the frontend consumer in the separate repo.
