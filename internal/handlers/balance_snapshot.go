package handlers

import (
	"log"
	"net/http"
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
		ORDER BY account_id, date, created_at DESC
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

// GET /plaid/balance-history?account_id=X&days=90
func (h *PlaidHandler) BalanceHistory(c *gin.Context) {
	accountID := c.Query("account_id")
	daysStr := c.DefaultQuery("days", "90")
	days, _ := strconv.Atoi(daysStr)
	if days <= 0 || days > 365 {
		days = 90
	}

	cutoff := time.Now().AddDate(0, 0, -days).Format("2006-01-02")

	var snapshots []models.DailyBalanceSnapshot
	query := h.GormDB.Where("snapshot_date >= ?", cutoff).Order("snapshot_date ASC")
	if accountID != "" {
		query = query.Where("account_id = ?", accountID)
	}
	query.Find(&snapshots)

	// If no snapshots, fall back to transactions
	if len(snapshots) == 0 {
		type txnBalance struct {
			Date             string   `json:"date" gorm:"column:date"`
			CurrentBalance   *float64 `json:"current_balance" gorm:"column:current_balance"`
			AvailableBalance *float64 `json:"available_balance" gorm:"column:available_balance"`
			AccountName      string   `json:"account_name" gorm:"column:account_name"`
		}
		var rows []txnBalance
		q := h.GormDB.Raw(`
			SELECT DISTINCT ON (date)
				date, current_balance, available_balance, account_name
			FROM transactions
			WHERE date >= ? AND account_id = ? AND (current_balance IS NOT NULL OR available_balance IS NOT NULL)
			ORDER BY date, created_at DESC
		`, cutoff, accountID)
		q.Scan(&rows)
		c.JSON(http.StatusOK, rows)
		return
	}

	c.JSON(http.StatusOK, snapshots)
}

// POST /plaid/snapshot-now — manually trigger snapshot
func (h *PlaidHandler) TriggerSnapshot(c *gin.Context) {
	go h.TakeDailyBalanceSnapshot()
	c.JSON(http.StatusOK, gin.H{"status": "started"})
}
