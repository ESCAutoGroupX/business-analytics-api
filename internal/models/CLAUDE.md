# Models Claude Guide

Single-file GORM schema (`models.go`, ~1k lines). Every persisted type lives here.

## Conventions

- Explicit `gorm:"column:..."` tags on every field — even when the default would match. Preserve this; the codebase relies on naming being explicit rather than inferred.
- Pointer types (`*string`, `*bool`, `*time.Time`) = nullable column. Non-pointer = NOT NULL (or zero-value on read).
- Every type has a `TableName()` method. Add one for every new type.
- Every sensitive field (`HashedPassword`, `AccessToken`, `RefreshToken`, `OTP`, `AuthenticatorSecret`, `ResetPasswordToken`, `PlaidAccessToken`, `PlaidCursor`) carries `json:"-"`. Preserve that for any new credential-like field.
- Section-header comment blocks (`// ─────...`) group related types. Put new models in the right section, or add a new section header if introducing a new domain.

## Adding a Table

Two paths, both end-to-end:

1. **Global AutoMigrate** — add the struct pointer to the batch list in `internal/database/db.go` `ConnectGORM`. Runs on every server start.
2. **Handler-local AutoMigrate** — give the owning handler an `AutoMigrate()` method and call it from `routes.Register()` when the handler is constructed. Pattern used by `APHandler`, `MatchingHandler`, `ReceivablesHandler`, `DocumentMatchHandler`, `PlaidHandler`.

`internal/migrations/*.sql` are reference/initial-setup artifacts — they do **not** run at server startup.

## Rules

1. Add `TableName()` for every new struct.
2. Use pointer types for nullable columns.
3. Mark secrets with `json:"-"`.
4. Use `autoCreateTime` / `autoUpdateTime` on `CreatedAt` / `UpdatedAt` where GORM should manage them.
5. JSONB columns are stored as `*string` with `gorm:"type:jsonb"` — the codebase marshals/unmarshals manually. See `Transaction.Category`, `Transaction.Location`, `Transaction.Counterparties`.
6. A failing AutoMigrate is logged `WARN:` and startup continues — if you add a NOT NULL column to an existing populated table, it will likely warn and skip. Prefer nullable-with-default when adding columns to live tables.

## Validation

```bash
go build ./...
```
