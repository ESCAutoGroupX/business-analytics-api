package handlers

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm/clause"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

// ── Live snapshot + reconstruction ─────────────────────────────────────────

// liveAccount holds one account's live Plaid balance.
type liveAccount struct {
	ID              string
	Name            string
	Type            string
	InstitutionName string
	Current         float64
	Available       *float64
}

// TakeDailyBalanceSnapshot fetches today's live Plaid balances, saves them,
// then reconstructs historical daily balances from posted transactions.
func (h *PlaidHandler) TakeDailyBalanceSnapshot() {
	today := time.Now().Format("2006-01-02")
	log.Printf("Balance snapshot: starting for %s", today)

	live := h.saveLiveBalances(today)
	h.reconstructFromTransactions(today, live)
}

// saveLiveBalances calls Plaid /accounts/balance/get for every item, saves
// today's snapshot, and returns the collected live balances.
func (h *PlaidHandler) saveLiveBalances(today string) []liveAccount {
	var items []models.PlaidItem
	h.GormDB.Where("needs_reauth = FALSE OR needs_reauth IS NULL").Find(&items)

	var live []liveAccount
	for i, item := range items {
		result, err := h.plaidRequest("/accounts/balance/get", map[string]interface{}{
			"access_token": item.AccessToken,
		})
		if err != nil {
			if isReauthError(err) {
				h.markItemNeedsReauth(&items[i], err.Error())
			}
			log.Printf("Balance snapshot: Plaid error for item %s: %v", item.ItemID, err)
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

			var cur float64
			var avail *float64
			if balances, ok := acct["balances"].(map[string]interface{}); ok {
				if v, ok := balances["current"].(float64); ok {
					cur = v
				}
				if v, ok := balances["available"].(float64); ok {
					avail = &v
				}
			}

			// Save today's live snapshot
			curCopy := cur
			snap := models.DailyBalanceSnapshot{
				AccountID:        acctID,
				AccountName:      acctName,
				InstitutionName:  item.InstitutionName,
				AccountType:      acctType,
				CurrentBalance:   &curCopy,
				AvailableBalance: avail,
				SnapshotDate:     today,
				Source:           "plaid",
			}
			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "account_id"}, {Name: "snapshot_date"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"account_name", "institution_name", "account_type",
					"current_balance", "available_balance", "source",
				}),
			}).Create(&snap)

			live = append(live, liveAccount{
				ID:              acctID,
				Name:            acctName,
				Type:            acctType,
				InstitutionName: item.InstitutionName,
				Current:         cur,
				Available:       avail,
			})
		}
	}

	log.Printf("Balance snapshot: %d live balances saved", len(live))
	return live
}

// reconstructFromTransactions rebuilds historical daily end-of-day balances
// by starting from each account's current Plaid balance and walking backwards
// through posted (pending=false) transactions.
//
// Plaid sign convention:
//
//	positive amount → money left account (debit)
//	negative amount → money entered account (credit)
//
// Going backwards (undoing a transaction):
//
//	running += amount
//
// This works for both signs:
//
//	debit (+): balance was higher before money left  → running increases
//	credit (-): balance was lower before money came in → running decreases
func (h *PlaidHandler) reconstructFromTransactions(today string, liveAccounts []liveAccount) {
	log.Println("Balance reconstruction: starting")

	todayTime, _ := time.Parse("2006-01-02", today)
	totalDays := 0

	for _, la := range liveAccounts {
		// Skip loan accounts — not enough transaction history
		if la.Type == "loan" {
			log.Printf("Balance reconstruction: %s — loan, skipping", la.Name)
			continue
		}

		// Sum posted transaction amounts per date
		type dateSumRow struct {
			Date      string  `gorm:"column:date"`
			AmountSum float64 `gorm:"column:amount_sum"`
		}
		var sums []dateSumRow
		h.GormDB.Raw(`
			SELECT TO_CHAR(date, 'YYYY-MM-DD') as date,
			       COALESCE(SUM(amount), 0) as amount_sum
			FROM transactions
			WHERE account_id = ? AND pending = false
			  AND amount IS NOT NULL AND date IS NOT NULL
			GROUP BY date
			ORDER BY date DESC
		`, la.ID).Scan(&sums)

		if len(sums) == 0 {
			log.Printf("Balance reconstruction: %s — no posted transactions", la.Name)
			continue
		}

		// Build date→sum lookup
		daySum := make(map[string]float64, len(sums))
		for _, s := range sums {
			daySum[s.Date] = s.AmountSum
		}
		earliestDate := sums[len(sums)-1].Date // last in DESC order
		earliestTime, _ := time.Parse("2006-01-02", earliestDate)

		// Walk backwards from today's known balance.
		// For each date, record end-of-day balance, then reverse that day's
		// transactions to get the previous day's end-of-day balance.
		running := la.Current

		// dateBalances[d] = end-of-day balance for date d
		dateBalances := make(map[string]float64)
		dateBalances[today] = running

		for d := todayTime; !d.Before(earliestTime); d = d.AddDate(0, 0, -1) {
			ds := d.Format("2006-01-02")
			dateBalances[ds] = math.Round(running*100) / 100

			// Reverse this day's posted transactions
			if s, ok := daySum[ds]; ok {
				running += s
			}
		}

		// Build snapshot records (skip today — live data already saved)
		var snapshots []models.DailyBalanceSnapshot
		for d := earliestTime; !d.After(todayTime); d = d.AddDate(0, 0, 1) {
			ds := d.Format("2006-01-02")
			if ds == today {
				continue
			}
			bal := dateBalances[ds]
			snapshots = append(snapshots, models.DailyBalanceSnapshot{
				AccountID:       la.ID,
				AccountName:     la.Name,
				InstitutionName: la.InstitutionName,
				AccountType:     la.Type,
				CurrentBalance:  &bal,
				SnapshotDate:    ds,
				Source:          "reconstructed",
			})
		}

		if len(snapshots) > 0 {
			res := h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "account_id"}, {Name: "snapshot_date"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"current_balance", "account_name", "institution_name",
					"account_type", "source",
				}),
			}).CreateInBatches(&snapshots, 100)
			if res.Error != nil {
				log.Printf("Balance reconstruction: %s — upsert error: %v", la.Name, res.Error)
			}
		}

		totalDays += len(snapshots)

		// Log first and last reconstructed balance
		if len(snapshots) > 0 {
			first := *snapshots[0].CurrentBalance
			last := *snapshots[len(snapshots)-1].CurrentBalance
			log.Printf("Balance reconstruction: %s (%s) — %d days (%s → %s), earliest=$%.2f, latest=$%.2f, live=$%.2f",
				la.Name, la.Type, len(snapshots), earliestDate, snapshots[len(snapshots)-1].SnapshotDate,
				first, last, la.Current)
		}
	}

	log.Printf("Balance reconstruction: completed — %d total days upserted", totalDays)
}

// ── GET /plaid/balance-history ─────────────────────────────────────────────

// BalanceHistory returns per-account daily balances from the daily_balance_snapshots
// table (populated by reconstruction + live snapshots).
//
// Query params: ?days=30 | ?from=YYYY-MM-DD&to=YYYY-MM-DD | ?ytd=true | ?all=true
func (h *PlaidHandler) BalanceHistory(c *gin.Context) {
	now := time.Now()
	today := now.Format("2006-01-02")

	// --- 1. Parse date range ---
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

	if days == 0 {
		if t1, err := time.Parse("2006-01-02", fromDate); err == nil {
			if t2, err := time.Parse("2006-01-02", toDate); err == nil {
				days = int(t2.Sub(t1).Hours()/24) + 1
			}
		}
	}

	// --- 2. Query snapshots in range ---
	type snapRow struct {
		AccountID        string   `gorm:"column:account_id"`
		AccountName      string   `gorm:"column:account_name"`
		AccountType      string   `gorm:"column:account_type"`
		Date             string   `gorm:"column:snapshot_date"`
		CurrentBalance   *float64 `gorm:"column:current_balance"`
		AvailableBalance *float64 `gorm:"column:available_balance"`
	}

	var snapRows []snapRow
	h.GormDB.Raw(`
		SELECT account_id, account_name, account_type, snapshot_date,
		       current_balance, available_balance
		FROM daily_balance_snapshots
		WHERE snapshot_date >= ? AND snapshot_date <= ?
	`, fromDate, toDate).Scan(&snapRows)

	// --- 3. Query the most recent snapshot before the range for carry-forward ---
	var priorRows []snapRow
	h.GormDB.Raw(`
		SELECT DISTINCT ON (account_name)
		       account_id, account_name, account_type, snapshot_date,
		       current_balance, available_balance
		FROM daily_balance_snapshots
		WHERE snapshot_date < ? AND account_name != ''
		ORDER BY account_name, snapshot_date DESC
	`, fromDate).Scan(&priorRows)

	// --- 4. Build per-account balance maps ---
	type accountMeta struct {
		AccountID   string
		AccountName string
		AccountType string
	}

	nameToBalances := make(map[string]map[string][2]*float64) // name → date → [current, available]
	nameMeta := make(map[string]accountMeta)

	for _, r := range snapRows {
		if r.AccountName == "" {
			continue
		}
		if _, ok := nameToBalances[r.AccountName]; !ok {
			nameToBalances[r.AccountName] = make(map[string][2]*float64)
		}
		nameToBalances[r.AccountName][r.Date] = [2]*float64{r.CurrentBalance, r.AvailableBalance}
		nameMeta[r.AccountName] = accountMeta{r.AccountID, r.AccountName, r.AccountType}
	}

	// Seed carry-forward from prior snapshots
	priorBal := make(map[string][2]*float64)
	for _, r := range priorRows {
		if r.AccountName == "" {
			continue
		}
		priorBal[r.AccountName] = [2]*float64{r.CurrentBalance, r.AvailableBalance}
		if _, ok := nameMeta[r.AccountName]; !ok {
			nameMeta[r.AccountName] = accountMeta{r.AccountID, r.AccountName, r.AccountType}
		}
	}

	// --- 5. Generate date range and fill gaps ---
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

	// --- 6. Build response per account ---
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

	var names []string
	for n := range nameMeta {
		names = append(names, n)
	}
	sort.Strings(names)

	var accounts []accountResult
	for _, name := range names {
		meta := nameMeta[name]
		dateMap := nameToBalances[name]

		var lastCur, lastAvail *float64
		if p, ok := priorBal[name]; ok {
			lastCur = p[0]
			lastAvail = p[1]
		}

		var balances []dailyBalance
		for _, date := range allDates {
			if bp, ok := dateMap[date]; ok {
				lastCur = bp[0]
				lastAvail = bp[1]
			}
			if lastCur != nil || lastAvail != nil {
				balances = append(balances, dailyBalance{
					Date:             date,
					CurrentBalance:   copyFloat(lastCur),
					AvailableBalance: copyFloat(lastAvail),
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

// ── Helpers ────────────────────────────────────────────────────────────────

func copyFloat(f *float64) *float64 {
	if f == nil {
		return nil
	}
	v := *f
	return &v
}

// POST /plaid/snapshot-now — manually trigger snapshot + reconstruction
func (h *PlaidHandler) TriggerSnapshot(c *gin.Context) {
	go h.TakeDailyBalanceSnapshot()
	c.JSON(http.StatusOK, gin.H{"status": "started"})
}
