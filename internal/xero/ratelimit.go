package xero

import (
	"log"
	"sync"
	"time"
)

// RateLimiter tracks Xero API calls and enforces per-minute and daily limits.
// All sync methods share a single instance.
type RateLimiter struct {
	mu              sync.Mutex
	callsThisMinute int
	windowStart     time.Time
	callsToday      int
	dailyCallDate   string // "2006-01-02" UTC date string
}

var globalLimiter = &RateLimiter{
	windowStart:   time.Now(),
	dailyCallDate: time.Now().UTC().Format("2006-01-02"),
}

// GetRateLimiter returns the singleton rate limiter.
func GetRateLimiter() *RateLimiter {
	return globalLimiter
}

// WaitForSlot blocks until an API call slot is available.
// Returns false if the daily limit (4800) has been reached.
func (rl *RateLimiter) WaitForSlot() bool {
	for {
		rl.mu.Lock()
		now := time.Now()

		// Reset daily counter on new day (UTC string comparison)
		today := now.UTC().Format("2006-01-02")
		if rl.dailyCallDate != today {
			rl.callsToday = 0
			rl.dailyCallDate = today
		}

		// Check daily limit (leave 200-call buffer from Xero's 5000 limit)
		if rl.callsToday >= 4800 {
			rl.mu.Unlock()
			log.Printf("DAILY_LIMIT_APPROACHING: %d calls today, stopping syncs until tomorrow", rl.callsToday)
			return false
		}

		// Reset minute window if elapsed
		if now.Sub(rl.windowStart) >= time.Minute {
			rl.callsThisMinute = 0
			rl.windowStart = now
		}

		// If at minute limit (55 of 60), sleep until window resets
		if rl.callsThisMinute >= 55 {
			sleepDuration := rl.windowStart.Add(time.Minute).Sub(now)
			rl.mu.Unlock()
			if sleepDuration > 0 {
				log.Printf("Rate limit: %d calls this minute, sleeping %v", rl.callsThisMinute, sleepDuration)
				time.Sleep(sleepDuration)
			}
			continue // re-check after sleep
		}

		// Slot available — claim it
		rl.callsThisMinute++
		rl.callsToday++
		rl.mu.Unlock()
		return true
	}
}

// CallsToday returns the current daily call count (for logging/monitoring).
func (rl *RateLimiter) CallsToday() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.callsToday
}
