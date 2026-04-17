# Xero Claude Guide

Xero-specific infrastructure. Currently just the rate limiter.

## The Rate Limiter

`RateLimiter` is a process-global singleton (`globalLimiter`) shared by every Xero caller. Caps:

- **55 calls per rolling minute** — Xero's hard cap is 60; we leave 5-call headroom.
- **4800 calls per UTC day** — Xero's hard cap is 5000; 200-call buffer.

## The Rule

**Every outbound Xero API call passes through `xero.GetRateLimiter().WaitForSlot()` first.**

```go
if !xero.GetRateLimiter().WaitForSlot() {
    // daily cap hit — stop, don't retry until UTC midnight
    return fmt.Errorf("xero daily limit reached")
}
// ... make the HTTP call
```

`WaitForSlot()` blocks (sleeps) until the minute window resets if we've hit 55 calls. It returns `false` only when the daily cap is exhausted — treat that as "stop trying today."

## Rules

1. Every outbound Xero HTTP call goes through `WaitForSlot()`. No exceptions, including ad-hoc test endpoints.
2. Never instantiate a second `RateLimiter`. Use `GetRateLimiter()` so all callers share the same window.
3. Don't raise the daily cap casually — a miscount under load can leave Xero 429-ing every caller until UTC midnight.

## Validation

```bash
go build ./...
```

After changes here, grep the repo for any new Xero call path (`api.xero.com`, `xero_` handler files) that bypasses the limiter.
