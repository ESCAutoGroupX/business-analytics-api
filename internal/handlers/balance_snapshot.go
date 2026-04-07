package handlers

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm/clause"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

// TakeDailyBalanceSnapshot fetches live Plaid balances and backfills from transactions.
func (h *PlaidHandler) TakeDailyBalanceSnapshot() {
	today := time.Now().Format("2006-01-02")
	log.Printf("Balance snapshot: starting for %s", today)

	// 1. Fetch live balances from Plaid for all items
	var items []models.PlaidItem
	h.GormDB.Find(&items)

	liveCount := 0
	for _, item := range items {
		result, err := h.plaidRequest("/accounts/balance/get", map[string]interface{}{
			"access_token": item.AccessToken,
		})
		if err != nil {
			log.Printf("Balance snapshot: Plaid balance error for item %s: %v", item.ItemID, err)
			continue
		}

		accounts, ok := result["accounts"].([]interface{})
		if !ok {
			continue
		}

		for _, a := range accounts {
			acct, ok := a.(map[string]interface{})
			if !ok {
				continue
			}

			acctID, _ := acct["account_id"].(string)
			acctName, _ := acct["name"].(string)
			acctType, _ := acct["type"].(string)

			var currentBal, availBal *float64
			if balances, ok := acct["balances"].(map[string]interface{}); ok {
				if v, ok := balances["current"].(float64); ok {
					currentBal = &v
				}
				if v, ok := balances["available"].(float64); ok {
					availBal = &v
				}
			}

			snap := models.DailyBalanceSnapshot{
				AccountID:        acctID,
				AccountName:      acctName,
				InstitutionName:  item.InstitutionName,
				AccountType:      acctType,
				CurrentBalance:   currentBal,
				AvailableBalance: availBal,
				SnapshotDate:     today,
			}

			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "account_id"}, {Name: "snapshot_date"}},
				DoUpdates: clause.AssignmentColumns([]string{"account_name", "institution_name", "account_type", "current_balance", "available_balance"}),
			}).Create(&snap)
			liveCount++
		}
	}

	log.Printf("Balance snapshot: %d live balances saved", liveCount)

	// 2. Backfill from transactions table (last 90 days)
	h.backfillFromTransactions()
}

func (h *PlaidHandler) backfillFromTransactions() {
	cutoff := time.Now().AddDate(0, 0, -90).Format("2006-01-02")

	// Get the most recent transaction per account per day that has a balance
	type dailyRow struct {
		AccountID        string   `gorm:"column:account_id"`
		AccountName      string   `gorm:"column:account_name"`
		AccountType      string   `gorm:"column:account_type"`
		Date             string   `gorm:"column:date"`
		CurrentBalance   *float64 `gorm:"column:current_balance"`
		AvailableBalance *float64 `gorm:"column:available_balance"`
	}

	var rows []dailyRow
	h.GormDB.Raw(`
		SELECT DISTINCT ON (account_id, date)
			account_id, account_name, account_type, date,
			current_balance, available_balance
		FROM transactions
		WHERE date >= ? AND (current_balance IS NOT NULL OR available_balance IS NOT NULL)
		ORDER BY account_id, date, transaction_datetime DESC NULLS LAST, created_at DESC
	`, cutoff).Scan(&rows)

	backfilled := 0
	for _, r := range rows {
		if r.AccountID == "" || r.Date == "" {
			continue
		}

		snap := models.DailyBalanceSnapshot{
			AccountID:        r.AccountID,
			AccountName:      r.AccountName,
			AccountType:      r.AccountType,
			CurrentBalance:   r.CurrentBalance,
			AvailableBalance: r.AvailableBalance,
			SnapshotDate:     r.Date,
		}

		// Only insert if no snapshot exists yet (don't overwrite live Plaid data)
		result := h.GormDB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "account_id"}, {Name: "snapshot_date"}},
			DoNothing: true,
		}).Create(&snap)
		if result.RowsAffected > 0 {
			backfilled++
		}
	}

	log.Printf("Balance snapshot: %d days backfilled from transactions", backfilled)
}

// GET /plaid/balance-history?days=30|from=YYYY-MM-DD&to=YYYY-MM-DD|ytd=true|all=true
func (h *PlaidHandler) BalanceHistory(c *gin.Context) {
	now := time.Now()
	today := now.Format("2006-01-02")

	// --- 1. Parse date range from query params ---
	var fromDate, toDate string
	toDate = today

	days := 0
	if c.Query("all") == "true" {
		fromDate = "2020-01-01"
	} else if c.Query("ytd") == "true" {
		fromDate = fmt.Sprintf("%d-01-01", now.Year())
	} else if from := c.Query("from"); from != "" {
		fromDate = from
		if to := c.Query("to"); to != "" {
			toDate = to
		}
	} else {
		days, _ = strconv.Atoi(c.DefaultQuery("days", "30"))
		if days <= 0 {
			days = 30
		}
		fromDate = now.AddDate(0, 0, -days).Format("2006-01-02")
	}

	// Compute days for response if not already set
	if days == 0 {
		if t1, err := time.Parse("2006-01-02", fromDate); err == nil {
			if t2, err := time.Parse("2006-01-02", toDate); err == nil {
				days = int(t2.Sub(t1).Hours()/24) + 1
			}
		}
	}

	// --- 2. Query transactions for end-of-day balances per account ---
	type balanceRow struct {
		AccountID        string   `gorm:"column:account_id"`
		AccountName      string   `gorm:"column:account_name"`
		AccountType      string   `gorm:"column:account_type"`
		Date             string   `gorm:"column:date"`
		CurrentBalance   *float64 `gorm:"column:current_balance"`
		AvailableBalance *float64 `gorm:"column:available_balance"`
	}

	var txnRows []balanceRow
	h.GormDB.Raw(`
		SELECT DISTINCT ON (account_id, date)
			account_id, account_name, account_type, date,
			current_balance, available_balance
		FROM transactions
		WHERE date >= ? AND date <= ?
		AND (current_balance IS NOT NULL OR available_balance IS NOT NULL)
		ORDER BY account_id, date, transaction_datetime DESC NULLS LAST, created_at DESC
	`, fromDate, toDate).Scan(&txnRows)

	// --- 3. Query daily_balance_snapshots for live Plaid balances ---
	type snapshotRow struct {
		AccountID        string   `gorm:"column:account_id"`
		AccountName      string   `gorm:"column:account_name"`
		AccountType      string   `gorm:"column:account_type"`
		Date             string   `gorm:"column:date"`
		CurrentBalance   *float64 `gorm:"column:current_balance"`
		AvailableBalance *float64 `gorm:"column:available_balance"`
		InstitutionName  string   `gorm:"column:institution_name"`
	}

	var snapRows []snapshotRow
	h.GormDB.Raw(`
		SELECT account_id, account_name, account_type, snapshot_date as date,
			current_balance, available_balance, institution_name
		FROM daily_balance_snapshots
		WHERE snapshot_date >= ? AND snapshot_date <= ?
	`, fromDate, toDate).Scan(&snapRows)

	// --- 4. Build unified balance map keyed by account_name ---
	// We merge by account_name to handle re-linked Plaid items with different
	// account_ids but the same logical account name.

	// Track which account_id has the most recent data per account_name
	type accountMeta struct {
		AccountID   string
		AccountName string
		AccountType string
		MaxDate     string
	}

	// balancePoint is a single day's balance for an account
	type balancePoint struct {
		CurrentBalance   *float64
		AvailableBalance *float64
		FromSnapshot     bool // snapshot data overrides txn data
	}

	// Map: account_name -> date -> balancePoint
	nameToBalances := make(map[string]map[string]balancePoint)
	// Map: account_name -> accountMeta (track canonical account_id)
	nameMeta := make(map[string]accountMeta)

	updateMeta := func(name, id, acctType, date string) {
		if name == "" {
			return
		}
		existing, ok := nameMeta[name]
		if !ok || date > existing.MaxDate {
			nameMeta[name] = accountMeta{
				AccountID:   id,
				AccountName: name,
				AccountType: acctType,
				MaxDate:     date,
			}
		}
	}

	// Insert transaction rows first
	for _, r := range txnRows {
		if r.AccountName == "" || r.AccountID == "" {
			continue
		}
		if _, ok := nameToBalances[r.AccountName]; !ok {
			nameToBalances[r.AccountName] = make(map[string]balancePoint)
		}
		nameToBalances[r.AccountName][r.Date] = balancePoint{
			CurrentBalance:   r.CurrentBalance,
			AvailableBalance: r.AvailableBalance,
			FromSnapshot:     false,
		}
		updateMeta(r.AccountName, r.AccountID, r.AccountType, r.Date)
	}

	// Overlay snapshot rows (snapshots override transaction-derived data)
	for _, r := range snapRows {
		if r.AccountName == "" || r.AccountID == "" {
			continue
		}
		if _, ok := nameToBalances[r.AccountName]; !ok {
			nameToBalances[r.AccountName] = make(map[string]balancePoint)
		}
		nameToBalances[r.AccountName][r.Date] = balancePoint{
			CurrentBalance:   r.CurrentBalance,
			AvailableBalance: r.AvailableBalance,
			FromSnapshot:     true,
		}
		updateMeta(r.AccountName, r.AccountID, r.AccountType, r.Date)
	}

	// --- 5. Generate all dates in range ---
	fromTime, err := time.Parse("2006-01-02", fromDate)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from date"})
		return
	}
	toTime, err := time.Parse("2006-01-02", toDate)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to date"})
		return
	}

	var allDates []string
	for d := fromTime; !d.After(toTime); d = d.AddDate(0, 0, 1) {
		allDates = append(allDates, d.Format("2006-01-02"))
	}

	// --- 6. For each account, fill gaps (carry forward) ---
	type dailyBalance struct {
		Date             string   `json:"date"`
		CurrentBalance   *float64 `json:"current_balance"`
		AvailableBalance *float64 `json:"available_balance"`
	}

	type accountResult struct {
		AccountID   string         `json:"account_id"`
		AccountName string         `json:"account_name"`
		AccountType string         `json:"account_type"`
		Balances    []dailyBalance `json:"balances"`
	}

	// Sort account names for deterministic output
	var accountNames []string
	for name := range nameMeta {
		accountNames = append(accountNames, name)
	}
	sort.Strings(accountNames)

	var accounts []accountResult
	for _, acctName := range accountNames {
		meta := nameMeta[acctName]
		dateMap := nameToBalances[acctName]

		// Look backward for the most recent balance before the range if
		// the first day has no data
		var lastCurrent *float64
		var lastAvail *float64

		if _, ok := dateMap[allDates[0]]; !ok {
			// Find all account_ids that share this name
			var accountIDs []string
			for _, r := range txnRows {
				if r.AccountName == acctName {
					found := false
					for _, id := range accountIDs {
						if id == r.AccountID {
							found = true
							break
						}
					}
					if !found {
						accountIDs = append(accountIDs, r.AccountID)
					}
				}
			}
			for _, r := range snapRows {
				if r.AccountName == acctName {
					found := false
					for _, id := range accountIDs {
						if id == r.AccountID {
							found = true
							break
						}
					}
					if !found {
						accountIDs = append(accountIDs, r.AccountID)
					}
				}
			}

			// Query the most recent transaction balance before the range
			if len(accountIDs) > 0 {
				var priorRow balanceRow
				result := h.GormDB.Raw(`
					SELECT DISTINCT ON (account_id)
						account_id, account_name, account_type, date,
						current_balance, available_balance
					FROM transactions
					WHERE date < ? AND account_id IN ?
					AND (current_balance IS NOT NULL OR available_balance IS NOT NULL)
					ORDER BY account_id, date DESC, transaction_datetime DESC NULLS LAST, created_at DESC
					LIMIT 1
				`, fromDate, accountIDs).Scan(&priorRow)
				if result.RowsAffected > 0 {
					lastCurrent = priorRow.CurrentBalance
					lastAvail = priorRow.AvailableBalance
				}
			}
		}

		var balances []dailyBalance
		for _, date := range allDates {
			if bp, ok := dateMap[date]; ok {
				lastCurrent = bp.CurrentBalance
				lastAvail = bp.AvailableBalance
			}
			// Only emit entries once we have a known balance
			if lastCurrent != nil || lastAvail != nil {
				cur := copyFloat(lastCurrent)
				avail := copyFloat(lastAvail)
				balances = append(balances, dailyBalance{
					Date:             date,
					CurrentBalance:   cur,
					AvailableBalance: avail,
				})
			}
		}

		accounts = append(accounts, accountResult{
			AccountID:   meta.AccountID,
			AccountName: meta.AccountName,
			AccountType: meta.AccountType,
			Balances:    balances,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"accounts": accounts,
		"date_range": gin.H{
			"from": fromDate,
			"to":   toDate,
		},
		"days": days,
	})
}

// copyFloat returns a new pointer to a copy of the float value, or nil.
func copyFloat(f *float64) *float64 {
	if f == nil {
		return nil
	}
	v := *f
	return &v
}

// POST /plaid/snapshot-now — manually trigger snapshot
func (h *PlaidHandler) TriggerSnapshot(c *gin.Context) {
	go h.TakeDailyBalanceSnapshot()
	c.JSON(http.StatusOK, gin.H{"status": "started"})
}
