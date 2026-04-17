# Routes Claude Guide

Single source of truth for HTTP endpoint registration.

## The Rule

`Register(r, gormDB, secretKey, cfg)` constructs every handler and mounts every route. If it's not in this file, it's not routed.

## Layout

1. Instantiate all handlers at the top of `Register()`.
2. Call handler-owned `AutoMigrate()` for handlers that carry domain-local tables (currently `PlaidHandler`, `APHandler`, `MatchingHandler`, `ReceivablesHandler`, `DocumentMatchHandler`).
3. Register public routes directly on `r` (`/health`, `/auth/*`, `/xero/authorize|callback`, `/2fa/*`).
4. Create `protected := r.Group("/")`, apply `middleware.Auth(secretKey)`, register everything else inside that block.

## Rules

1. **Authenticated endpoints go inside `protected { ... }`.** Do not mount authed routes directly on `r`.
2. **Group endpoints by domain.** `vendors := protected.Group("/vendors") { ... }` — match the existing nesting pattern.
3. **Trailing slashes matter.** Gin 301-redirects on mismatches; that's why `middleware/cors.go` wraps the engine at the `http.Handler` level. When a frontend hits a CORS error on a redirect, fix the declared path rather than touching CORS.
4. **Handler-level AutoMigrate ordering matters.** If a new handler depends on tables owned by another handler's `AutoMigrate`, construct and migrate that one first.

## Validation

```bash
go build ./...
```

If a route group changes, audit the frontend API client for stale paths.
