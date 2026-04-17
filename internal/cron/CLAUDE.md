# Cron Claude Guide

Scheduled background jobs. `Start(db, cfg)` is called from `cmd/server/main.go` and returns the `*cron.Cron` the caller defers `Stop()` on.

## The Wrapping Rules

Never register a raw function with `c.AddFunc`. Always wrap:

- `wrapSimpleJob(name, fn)` — no-arg function; adds panic recovery + start/complete logs.
- `wrapJob(h, name, fn)` — fetches the active Xero connection, skips if none, runs `fn(conn)` with recovery.
- `wrapSyncAll(h)` — daily full Xero resync (clears sync state first).

An unwrapped cron func that panics will take down the scheduler goroutine silently.

## Schedule Map (UTC, standard 5-field cron)

| Cron | Job | Handler |
|---|---|---|
| `*/15 * * * *` | Plaid transaction sync | `PlaidHandler.SyncPlaidTransactions` |
| `*/30 * * * *` | Xero bank txns, invoices, payments, contacts | `XeroSyncHandler.Sync*` |
| `*/30 * * * *` | Document auto-match | `DocumentMatchHandler.MatchDocumentsToTransactions` |
| `0 * * * *` | Xero journals + tracking categories | `XeroSyncHandler.Sync*` |
| `5 * * * *` | Xero accounts (staggered) | `XeroSyncHandler.SyncAccounts` |
| `0 2 * * *` | Xero full resync | `wrapSyncAll` |
| `30 2 * * *` | Backfill customer numbers from statements | `handlers.BackfillCustomerNumbersSync` |
| `35 2 * * *` | Resolve locations from customer numbers | `handlers.ResolveLocationsSync` |
| `0 7 * * *` | Overdue-bills alert email | `EmailSender.CheckOverdueBills` |
| `0 7 * * *` | Low-balance alert email | `EmailSender.CheckLowBankBalance` |
| `0 7 * * 1` | Reconciliation alert (Mondays) | `EmailSender.CheckReconciliationAlert` |
| `0 23 * * *` | Daily balance snapshot | `PlaidHandler.TakeDailyBalanceSnapshot` |

A balance snapshot also runs once in a panic-recovered goroutine at the end of `Start()`.

## Rules

1. **Always wrap.** A panic in an unwrapped cron func brings down the scheduler.
2. **Log start + completion.** The wrappers already do this — don't duplicate.
3. **Xero sync jobs skip silently** when no active Xero connection exists — that's the normal steady state before the integration is connected. Don't treat it as an error.
4. **Stagger new hourly jobs.** That's why accounts sync runs at `5 *` and not `0 *` — to avoid rate-limit contention with the `:00` batch.
5. `robfig/cron/v3` uses standard 5-field cron expressions (not the 6-field variant with seconds).

## Validation

```bash
go build ./...
```

Test cron expressions at [crontab.guru](https://crontab.guru) or with a quick unit test before shipping.
