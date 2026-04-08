package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type DashboardHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

func (h *DashboardHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

func round2(x float64) float64 {
	return math.Round(x*100) / 100
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return def
	}
	return v
}

// BankBalance is kept for backward compatibility; delegates to GetBankBalance.
func BankBalance(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "not yet implemented"})
}

// GET /dashboard/bank-balance
func (h *DashboardHandler) GetBankBalance(c *gin.Context) {
	// Try to get live Plaid balances from all connected items (company-wide)
	if h.Cfg != nil && h.Cfg.PlaidClientID != "" && h.Cfg.PlaidSecret != "" {
		var items []models.PlaidItem
		h.GormDB.Find(&items)

		// Fallback: try legacy tokens from users table
		if len(items) == 0 {
			var users []models.User
			h.GormDB.Select("plaid_access_token").Where("plaid_access_token IS NOT NULL AND plaid_access_token != ''").Find(&users)
			for _, u := range users {
				items = append(items, models.PlaidItem{AccessToken: *u.PlaidAccessToken})
			}
		}

		if len(items) > 0 {
			totalBalance := 0.0
			anySuccess := false
			for _, item := range items {
				balance, err := h.fetchPlaidDepositoryBalance(item.AccessToken)
				if err == nil {
					totalBalance += balance
					anySuccess = true
				} else {
					log.Printf("GetBankBalance: Plaid balance fetch failed for item %s: %v", item.ItemID, err)
				}
			}
			if anySuccess {
				c.JSON(http.StatusOK, gin.H{
					"totals": gin.H{
						"total_balance_available": round2(totalBalance),
					},
				})
				return
			}
			log.Printf("GetBankBalance: all Plaid balance fetches failed, falling back to local calc")
		}
	}

	// Fallback: compute from local data
	ctx := context.Background()

	var startingTotal float64
	err := h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(starting_credit_card_bal), 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'`).Scan(&startingTotal)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}

	var txTotal float64
	err = h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE account_type = 'depository'`).Scan(&txTotal)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query transactions", "error": err.Error()})
		return
	}

	totalBalance := round2(startingTotal + txTotal*-1)

	c.JSON(http.StatusOK, gin.H{
		"totals": gin.H{
			"total_balance_available": totalBalance,
		},
	})
}

func (h *DashboardHandler) plaidBaseURL() string {
	if h.Cfg == nil {
		return "https://sandbox.plaid.com"
	}
	switch h.Cfg.PlaidEnv {
	case "production":
		return "https://production.plaid.com"
	case "development":
		return "https://development.plaid.com"
	default:
		return "https://sandbox.plaid.com"
	}
}

func (h *DashboardHandler) fetchPlaidDepositoryBalance(accessToken string) (float64, error) {
	payload := map[string]interface{}{
		"client_id":    h.Cfg.PlaidClientID,
		"secret":       h.Cfg.PlaidSecret,
		"access_token": accessToken,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal Plaid request: %w", err)
	}

	resp, err := http.Post(h.plaidBaseURL()+"/accounts/balance/get", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to call Plaid API: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read Plaid response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("Plaid API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Accounts []struct {
			AccountID string `json:"account_id"`
			Balances  struct {
				Available *float64 `json:"available"`
				Current   *float64 `json:"current"`
			} `json:"balances"`
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"accounts"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("failed to parse Plaid response: %w", err)
	}

	totalAvailable := 0.0
	for _, acct := range result.Accounts {
		if acct.Type == "depository" && acct.Balances.Available != nil {
			totalAvailable += *acct.Balances.Available
		}
	}

	return totalAvailable, nil
}

// GET /dashboard/payment-method
func (h *DashboardHandler) GetPaymentMethod(c *gin.Context) {
	ctx := context.Background()

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(bank_name, ''), COALESCE(card_name, ''),
		        COALESCE(card_last_4_digits, ''),
		        COALESCE(balance_available, 0), COALESCE(balance_current, 0),
		        COALESCE(credit_limit, 0), COALESCE(starting_credit_card_bal, 0)
		 FROM payment_methods WHERE method_type = 'CREDIT_CARD'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type ccAcct struct {
		ID               string
		Title            string
		BankName         string
		CardName         string
		CardLast4        string
		BalanceAvailable float64
		BalanceCurrent   float64
		CreditLimit      float64
		StartingBal      float64
	}
	var accounts []ccAcct
	for rows.Next() {
		var a ccAcct
		if err := rows.Scan(&a.ID, &a.Title, &a.BankName, &a.CardName, &a.CardLast4,
			&a.BalanceAvailable, &a.BalanceCurrent, &a.CreditLimit, &a.StartingBal); err != nil {
			continue
		}
		accounts = append(accounts, a)
	}

	totalBalanceAvailable := 0.0
	totalCreditLimit := 0.0
	totalBalanceCurrent := 0.0
	liabilities := []gin.H{}

	for _, a := range accounts {
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT COALESCE(amount, 0) FROM transactions WHERE account_type = 'credit' AND account_id = $1 ORDER BY date, created_at`, a.ID)
		if err != nil {
			continue
		}
		balance := a.StartingBal
		for txRows.Next() {
			var amt float64
			txRows.Scan(&amt)
			balance += amt * -1
		}
		txRows.Close()
		balance = round2(balance)

		totalBalanceAvailable += balance
		totalCreditLimit += a.CreditLimit
		totalBalanceCurrent += a.BalanceCurrent

		liabilities = append(liabilities, gin.H{
			"id":                a.ID,
			"title":             a.Title,
			"bank_name":         a.BankName,
			"card_name":         a.CardName,
			"card_last_4":       a.CardLast4,
			"balance_available": balance,
			"balance_current":   a.BalanceCurrent,
			"credit_limit":      a.CreditLimit,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"liabilities": liabilities,
		"totals": gin.H{
			"total_balance_available": round2(totalBalanceAvailable),
			"total_credit_limit":      round2(totalCreditLimit),
			"total_balance_current":   round2(totalBalanceCurrent),
		},
	})
}

// GET /dashboard/credit-card-Balance-list
func (h *DashboardHandler) GetCreditCardBalanceList(c *gin.Context) {
	ctx := context.Background()

	accountID := c.Query("account_id")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	vendorName := c.Query("vendor_name")
	paymentType := c.Query("payment_type")
	locationName := c.Query("location_name")
	page := parseIntDefault(c.DefaultQuery("page", "1"), 1)
	pageSize := parseIntDefault(c.DefaultQuery("page_size", "50"), 50)
	sortBy := c.DefaultQuery("sort_by", "date")
	sortOrder := c.DefaultQuery("sort_order", "desc")

	// Get CREDIT_CARD accounts
	pmWhere := "WHERE method_type = 'CREDIT_CARD'"
	pmArgs := []interface{}{}
	pmArgIdx := 1

	if accountID != "" {
		pmWhere += fmt.Sprintf(" AND id = $%d", pmArgIdx)
		pmArgs = append(pmArgs, accountID)
		pmArgIdx++
	}

	pmRows, err := h.sqlDB().QueryContext(ctx,
		fmt.Sprintf(`SELECT id, COALESCE(title, ''), COALESCE(starting_credit_card_bal, 0)
		 FROM payment_methods %s ORDER BY sorting_order, created_at`, pmWhere), pmArgs...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer pmRows.Close()

	type pmInfo struct {
		ID          string
		Title       string
		StartingBal float64
	}
	var pms []pmInfo
	for pmRows.Next() {
		var p pmInfo
		pmRows.Scan(&p.ID, &p.Title, &p.StartingBal)
		pms = append(pms, p)
	}

	// Build WHERE for transactions — filter directly by account_type
	txWhere := "WHERE account_type = 'credit'"
	txArgs := []interface{}{}
	txArgIdx := 1

	if accountID != "" {
		txWhere += fmt.Sprintf(" AND account_id = $%d", txArgIdx)
		txArgs = append(txArgs, accountID)
		txArgIdx++
	}

	if startDate != "" {
		txWhere += fmt.Sprintf(" AND date >= $%d", txArgIdx)
		txArgs = append(txArgs, startDate)
		txArgIdx++
	}
	if endDate != "" {
		txWhere += fmt.Sprintf(" AND date <= $%d", txArgIdx)
		txArgs = append(txArgs, endDate)
		txArgIdx++
	}
	if vendorName != "" {
		txWhere += fmt.Sprintf(" AND vendor ILIKE $%d", txArgIdx)
		txArgs = append(txArgs, "%"+vendorName+"%")
		txArgIdx++
	}
	if paymentType != "" {
		txWhere += fmt.Sprintf(" AND transaction_type = $%d", txArgIdx)
		txArgs = append(txArgs, paymentType)
		txArgIdx++
	}
	if locationName != "" {
		txWhere += fmt.Sprintf(" AND location::text ILIKE $%d", txArgIdx)
		txArgs = append(txArgs, "%"+locationName+"%")
		txArgIdx++
	}

	// Validate sort column
	validSorts := map[string]string{
		"date": "date", "amount": "amount", "vendor": "vendor",
		"transaction_type": "transaction_type", "created_at": "created_at",
		"name": "name", "merchant_name": "merchant_name",
	}
	sortCol := "date"
	if v, ok := validSorts[sortBy]; ok {
		sortCol = v
	}
	order := "DESC"
	if strings.ToLower(sortOrder) == "asc" {
		order = "ASC"
	}

	// Count
	var totalRecords int
	countQ := fmt.Sprintf("SELECT COUNT(*) FROM transactions %s", txWhere)
	h.sqlDB().QueryRowContext(ctx, countQ, txArgs...).Scan(&totalRecords)

	totalPages := int(math.Ceil(float64(totalRecords) / float64(pageSize)))
	offset := (page - 1) * pageSize

	dataQ := fmt.Sprintf(
		`SELECT id, account_id, COALESCE(date::text, ''), COALESCE(amount, 0),
		        COALESCE(vendor, ''), COALESCE(name, ''), COALESCE(transaction_type, ''),
		        COALESCE(merchant_name, ''), COALESCE(account_name, '')
		 FROM transactions %s ORDER BY %s %s OFFSET $%d LIMIT $%d`,
		txWhere, sortCol, order, txArgIdx, txArgIdx+1)
	txArgs = append(txArgs, offset, pageSize)

	txRows, err := h.sqlDB().QueryContext(ctx, dataQ, txArgs...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query transactions", "error": err.Error()})
		return
	}
	defer txRows.Close()

	data := []gin.H{}
	for txRows.Next() {
		var txID, acctID, dateStr, vendorStr, nameStr, txType, merchantName, acctName string
		var amt float64
		txRows.Scan(&txID, &acctID, &dateStr, &amt, &vendorStr, &nameStr, &txType, &merchantName, &acctName)
		data = append(data, gin.H{
			"id":               txID,
			"account_id":       acctID,
			"date":             dateStr,
			"amount":           round2(amt),
			"vendor":           vendorStr,
			"name":             nameStr,
			"transaction_type": txType,
			"merchant_name":    merchantName,
			"account_name":     acctName,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"data": data,
		"pagination": gin.H{
			"page":          page,
			"page_size":     pageSize,
			"total_pages":   totalPages,
			"total_records": totalRecords,
		},
	})
}

// GET /dashboard/api/bank-balance-trans
func (h *DashboardHandler) GetBankBalanceTrans(c *gin.Context) {
	ctx := context.Background()

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(bank_name, ''),
		        COALESCE(balance_available, 0), COALESCE(balance_current, 0),
		        COALESCE(starting_credit_card_bal, 0), COALESCE(minimum_balance, 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type acctInfo struct {
		ID          string
		Title       string
		BankName    string
		BalAvail    float64
		BalCurrent  float64
		StartingBal float64
		MinBal      float64
	}
	var accounts []acctInfo
	for rows.Next() {
		var a acctInfo
		rows.Scan(&a.ID, &a.Title, &a.BankName, &a.BalAvail, &a.BalCurrent, &a.StartingBal, &a.MinBal)
		accounts = append(accounts, a)
	}

	totalOverallBalance := 0.0
	acctResults := []gin.H{}

	for _, a := range accounts {
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT id, COALESCE(date::text, ''), COALESCE(amount, 0), COALESCE(vendor, ''),
			        COALESCE(name, ''), COALESCE(transaction_type, ''), COALESCE(merchant_name, '')
			 FROM transactions WHERE account_type = 'depository' AND account_id = $1
			 ORDER BY date, created_at`, a.ID)
		if err != nil {
			continue
		}

		balance := a.StartingBal
		txList := []gin.H{}
		for txRows.Next() {
			var txID, dateStr, vendorStr, nameStr, txType, merchantName string
			var amt float64
			txRows.Scan(&txID, &dateStr, &amt, &vendorStr, &nameStr, &txType, &merchantName)
			balance += amt * -1
			txList = append(txList, gin.H{
				"id":               txID,
				"date":             dateStr,
				"amount":           round2(amt),
				"vendor":           vendorStr,
				"name":             nameStr,
				"transaction_type": txType,
				"merchant_name":    merchantName,
				"running_balance":  round2(balance),
			})
		}
		txRows.Close()

		totalOverallBalance += round2(balance)
		acctResults = append(acctResults, gin.H{
			"account_id":     a.ID,
			"account_name":   a.Title,
			"bank_name":      a.BankName,
			"balance":        round2(balance),
			"minimum_balance": a.MinBal,
			"transactions":   txList,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"total_overall_account_balance": round2(totalOverallBalance),
		"accounts":                      acctResults,
	})
}

// GET /dashboard/api/bank-balance-trends
func (h *DashboardHandler) GetBankBalanceTrends(c *gin.Context) {
	ctx := context.Background()
	weeks := parseIntDefault(c.DefaultQuery("weeks", "1"), 1)

	now := time.Now()
	startDate := now.AddDate(0, 0, -7*weeks)

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(starting_credit_card_bal, 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type pmInfo struct {
		ID          string
		Title       string
		StartingBal float64
	}
	var pms []pmInfo
	for rows.Next() {
		var p pmInfo
		rows.Scan(&p.ID, &p.Title, &p.StartingBal)
		pms = append(pms, p)
	}

	results := []gin.H{}
	for _, pm := range pms {
		// Get all transactions up to end of range, ordered by date
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT COALESCE(date::text, ''), COALESCE(amount, 0)
			 FROM transactions WHERE account_type = 'depository' AND account_id = $1
			 ORDER BY date, created_at`, pm.ID)
		if err != nil {
			continue
		}

		// Compute balance at each transaction
		type txEntry struct {
			Date   string
			Amount float64
		}
		var allTx []txEntry
		for txRows.Next() {
			var t txEntry
			txRows.Scan(&t.Date, &t.Amount)
			allTx = append(allTx, t)
		}
		txRows.Close()

		// Compute balance for each day in range
		dailyBalance := map[string]float64{}
		balance := pm.StartingBal

		// Process all transactions, tracking balance by day
		txIdx := 0
		current := startDate
		weeklyStartBalance := 0.0

		// First compute balance up to start date
		for txIdx < len(allTx) {
			txDate, err := time.Parse("2006-01-02", allTx[txIdx].Date)
			if err != nil {
				txIdx++
				continue
			}
			if txDate.Before(startDate) || txDate.Equal(startDate) {
				if txDate.Before(startDate) {
					balance += allTx[txIdx].Amount * -1
					txIdx++
					continue
				}
			}
			break
		}
		weeklyStartBalance = round2(balance)

		for !current.After(now) {
			dateStr := current.Format("2006-01-02")
			for txIdx < len(allTx) && allTx[txIdx].Date <= dateStr {
				balance += allTx[txIdx].Amount * -1
				txIdx++
			}
			label := current.Format("Monday, Jan 02, 2006")
			dailyBalance[label] = round2(balance)
			current = current.AddDate(0, 0, 1)
		}

		results = append(results, gin.H{
			"account_id":           pm.ID,
			"account_name":         pm.Title,
			"weekly_start_balance": weeklyStartBalance,
			"daily_bank_balance":   dailyBalance,
		})
	}

	c.JSON(http.StatusOK, results)
}

// GET /dashboard/bank-ledger
func (h *DashboardHandler) GetBankLedger(c *gin.Context) {
	ctx := context.Background()

	page := parseIntDefault(c.DefaultQuery("page", "1"), 1)
	pageSize := parseIntDefault(c.DefaultQuery("page_size", "50"), 50)
	sortBy := c.DefaultQuery("sort_by", "date")
	sortOrder := c.DefaultQuery("sort_order", "desc")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	paymentType := c.Query("payment_type")
	vendorName := c.Query("vendor_name")
	accountID := c.Query("account_id")

	// ── Step 1: Get latest balance per depository account from daily_balance_snapshots ──
	acctBalances := map[string]float64{}
	balRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT DISTINCT ON (account_id) account_id, current_balance
		 FROM daily_balance_snapshots
		 WHERE account_type = 'depository' AND current_balance IS NOT NULL
		 ORDER BY account_id, snapshot_date DESC`)
	if err != nil {
		log.Printf("ERROR: failed to query balance snapshots: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query balance snapshots", "error": err.Error()})
		return
	}
	defer balRows.Close()
	for balRows.Next() {
		var aid string
		var bal float64
		if err := balRows.Scan(&aid, &bal); err != nil {
			log.Printf("ERROR: scanning balance snapshot row: %v", err)
			continue
		}
		acctBalances[aid] = bal
	}

	// ── Step 2: Load ALL depository transactions for running-balance calculation ──
	// Only date-range and account_id filters apply here; vendor/payment_type are display-only.
	balWhere := "WHERE account_type = 'depository'"
	balArgs := []interface{}{}
	balIdx := 1

	if startDate != "" {
		balWhere += fmt.Sprintf(" AND date >= $%d", balIdx)
		balArgs = append(balArgs, startDate)
		balIdx++
	}
	if endDate != "" {
		balWhere += fmt.Sprintf(" AND date <= $%d", balIdx)
		balArgs = append(balArgs, endDate)
		balIdx++
	}
	if accountID != "" {
		balWhere += fmt.Sprintf(" AND account_id = $%d", balIdx)
		balArgs = append(balArgs, accountID)
		balIdx++
	}

	allTxRows, err := h.sqlDB().QueryContext(ctx,
		fmt.Sprintf(`SELECT id, COALESCE(plaid_id, ''), COALESCE(date::text, ''),
		        COALESCE(amount, 0), COALESCE(vendor, ''), COALESCE(name, ''),
		        COALESCE(transaction_type, ''), COALESCE(merchant_name, ''),
		        COALESCE(account_name, ''), COALESCE(account_id, ''),
		        COALESCE(pending, false), COALESCE(source, '')
		 FROM transactions %s
		 ORDER BY account_id, date DESC, transaction_datetime DESC NULLS LAST, created_at DESC`, balWhere), balArgs...)
	if err != nil {
		log.Printf("ERROR: failed to query transactions for ledger: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query transactions", "error": err.Error()})
		return
	}
	defer allTxRows.Close()

	// ── Step 3: Per-account running balance, walking newest-first ──
	type ledgerEntry struct {
		ID             string
		PlaidID        string
		Date           string
		Amount         float64
		Vendor         string
		Name           string
		TxType         string
		MerchantName   string
		AccountName    string
		AccountID      string
		Source         string
		Pending        bool
		RunningBalance *float64 // nil for pending transactions
	}

	var allEntries []ledgerEntry
	currentAcct := ""
	var running float64

	for allTxRows.Next() {
		var e ledgerEntry
		if err := allTxRows.Scan(&e.ID, &e.PlaidID, &e.Date, &e.Amount, &e.Vendor,
			&e.Name, &e.TxType, &e.MerchantName, &e.AccountName, &e.AccountID,
			&e.Pending, &e.Source); err != nil {
			log.Printf("ERROR: scanning transaction row: %v", err)
			continue
		}

		if e.AccountID != currentAcct {
			currentAcct = e.AccountID
			running = acctBalances[currentAcct]
		}

		if !e.Pending {
			bal := round2(running)
			e.RunningBalance = &bal
			running += e.Amount // Plaid convention: positive = money out; += undoes the txn
		}
		// Pending transactions: RunningBalance stays nil

		allEntries = append(allEntries, e)
	}

	// ── Step 3b: Load overrides for description enrichment ──
	var blOverrides []models.ReconciliationOverride
	h.GormDB.Find(&blOverrides)
	blOverrideMap := map[string]models.ReconciliationOverride{}
	for _, o := range blOverrides {
		blOverrideMap[o.PlaidID] = o
	}

	// ── Step 4: Apply vendor / payment_type display filters ──
	var filtered []ledgerEntry
	for _, e := range allEntries {
		if paymentType != "" && !strings.EqualFold(e.TxType, paymentType) {
			continue
		}
		if vendorName != "" && !strings.Contains(strings.ToLower(e.Name), strings.ToLower(vendorName)) {
			continue
		}
		filtered = append(filtered, e)
	}

	// ── Step 5: Count and pagination metadata ──
	totalTransactions := len(filtered)
	totalPages := int(math.Ceil(float64(totalTransactions) / float64(pageSize)))

	// ── Step 6: Sort filtered entries ──
	validSorts := map[string]string{
		"date": "date", "amount": "amount", "vendor": "vendor",
		"transaction_type": "transaction_type", "created_at": "created_at",
		"name": "name",
	}
	sortCol := "date"
	if v, ok := validSorts[sortBy]; ok {
		sortCol = v
	}
	asc := strings.ToLower(sortOrder) == "asc"

	switch sortCol {
	case "amount":
		sort.Slice(filtered, func(i, j int) bool {
			if asc {
				return filtered[i].Amount < filtered[j].Amount
			}
			return filtered[i].Amount > filtered[j].Amount
		})
	case "vendor":
		sort.Slice(filtered, func(i, j int) bool {
			if asc {
				return strings.ToLower(filtered[i].Vendor) < strings.ToLower(filtered[j].Vendor)
			}
			return strings.ToLower(filtered[i].Vendor) > strings.ToLower(filtered[j].Vendor)
		})
	default:
		// "date" and everything else: data is already in account/date DESC order from SQL.
		// For a pure date sort we re-sort across accounts.
		sort.Slice(filtered, func(i, j int) bool {
			if asc {
				return filtered[i].Date < filtered[j].Date
			}
			return filtered[i].Date > filtered[j].Date
		})
	}

	// ── Step 7: Paginate ──
	offset := (page - 1) * pageSize
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}

	ledger := []gin.H{}
	if offset < len(filtered) {
		for _, e := range filtered[offset:end] {
			// Description = override description > merchant_name
			desc := e.MerchantName
			if ov, ok := blOverrideMap[e.PlaidID]; ok && ov.Description != "" {
				desc = ov.Description
			}
			entry := gin.H{
				"id":               e.ID,
				"plaid_id":         e.PlaidID,
				"date":             e.Date,
				"amount":           round2(e.Amount),
				"vendor":           e.Name,
				"name":             e.Name,
				"transaction_type": e.TxType,
				"merchant_name":    e.MerchantName,
				"description":      desc,
				"account_name":     e.AccountName,
				"account_id":       e.AccountID,
				"pending":          e.Pending,
				"source":           e.Source,
			}
			if e.RunningBalance != nil {
				entry["running_balance"] = *e.RunningBalance
			} else {
				entry["running_balance"] = nil
			}
			ledger = append(ledger, entry)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"total_transactions": totalTransactions,
		"page":               page,
		"page_size":          pageSize,
		"total_pages":        totalPages,
		"sort_by":            sortBy,
		"sort_order":         sortOrder,
		"ledger":             ledger,
	})
}

// GET /dashboard/credit-card-ledger
func (h *DashboardHandler) GetCreditCardLedger(c *gin.Context) {
	ctx := context.Background()

	page := parseIntDefault(c.DefaultQuery("page", "1"), 1)
	pageSize := parseIntDefault(c.DefaultQuery("page_size", "25"), 25)
	sortBy := c.DefaultQuery("sort_by", "date")
	sortOrder := c.DefaultQuery("sort_order", "desc")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	search := c.Query("search")
	accountID := c.Query("account_id")
	accountOwner := c.Query("account_owner")

	// ── Step 1: Get latest balance per credit account from daily_balance_snapshots ──
	acctBalances := map[string]float64{}
	acctAvailable := map[string]float64{}
	balRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT DISTINCT ON (account_id) account_id, COALESCE(current_balance, 0), COALESCE(available_balance, 0)
		 FROM daily_balance_snapshots
		 WHERE account_type = 'credit' AND current_balance IS NOT NULL
		 ORDER BY account_id, snapshot_date DESC`)
	if err != nil {
		log.Printf("ERROR: failed to query credit balance snapshots: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query balance snapshots", "error": err.Error()})
		return
	}
	defer balRows.Close()
	for balRows.Next() {
		var aid string
		var bal, avail float64
		if err := balRows.Scan(&aid, &bal, &avail); err != nil {
			log.Printf("ERROR: scanning credit balance snapshot row: %v", err)
			continue
		}
		acctBalances[aid] = bal
		acctAvailable[aid] = avail
	}

	// Build account names from transactions
	acctNames := map[string]string{}
	acctRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT DISTINCT ON (account_id) account_id, account_name
		 FROM transactions WHERE account_type = 'credit' AND account_name != ''
		 ORDER BY account_id, created_at DESC`)
	if err != nil {
		log.Printf("ERROR: failed to query credit account names: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query account names", "error": err.Error()})
		return
	}
	defer acctRows.Close()
	for acctRows.Next() {
		var aid, aname string
		if err := acctRows.Scan(&aid, &aname); err != nil {
			log.Printf("ERROR: scanning credit account name row: %v", err)
			continue
		}
		acctNames[aid] = aname
	}

	type ccAccount struct {
		AccountID        string
		AccountName      string
		CurrentBalance   float64
		AvailableBalance float64
	}
	var ccAccounts []ccAccount
	for aid, bal := range acctBalances {
		ccAccounts = append(ccAccounts, ccAccount{
			AccountID:        aid,
			AccountName:      acctNames[aid],
			CurrentBalance:   bal,
			AvailableBalance: acctAvailable[aid],
		})
	}

	// ── Step 2: Load ALL credit transactions for running-balance calculation ──
	balWhere := "WHERE account_type = 'credit'"
	balArgs := []interface{}{}
	balIdx := 1

	if startDate != "" {
		balWhere += fmt.Sprintf(" AND date >= $%d", balIdx)
		balArgs = append(balArgs, startDate)
		balIdx++
	}
	if endDate != "" {
		balWhere += fmt.Sprintf(" AND date <= $%d", balIdx)
		balArgs = append(balArgs, endDate)
		balIdx++
	}
	if accountID != "" {
		balWhere += fmt.Sprintf(" AND account_id = $%d", balIdx)
		balArgs = append(balArgs, accountID)
		balIdx++
	}

	allTxRows, err := h.sqlDB().QueryContext(ctx,
		fmt.Sprintf(`SELECT id, COALESCE(plaid_id, ''), COALESCE(date::text, ''),
		        COALESCE(amount, 0), COALESCE(vendor, ''), COALESCE(name, ''),
		        COALESCE(transaction_type, ''), COALESCE(merchant_name, ''),
		        COALESCE(account_name, ''), COALESCE(account_id, ''),
		        COALESCE(pending, false), COALESCE(source, ''),
		        COALESCE(account_owner, '')
		 FROM transactions %s
		 ORDER BY account_id, date DESC, created_at DESC`, balWhere), balArgs...)
	if err != nil {
		log.Printf("ERROR: failed to query credit transactions for ledger: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query transactions", "error": err.Error()})
		return
	}
	defer allTxRows.Close()

	// ── Step 3: Per-account running balance, walking newest-first ──
	type ledgerEntry struct {
		ID             string
		PlaidID        string
		Date           string
		Amount         float64
		Vendor         string
		Name           string
		TxType         string
		MerchantName   string
		AccountName    string
		AccountID      string
		Source         string
		Pending        bool
		AccountOwner   string
		RunningBalance *float64
	}

	var allEntries []ledgerEntry
	currentAcct := ""
	var running float64

	for allTxRows.Next() {
		var e ledgerEntry
		if err := allTxRows.Scan(&e.ID, &e.PlaidID, &e.Date, &e.Amount, &e.Vendor,
			&e.Name, &e.TxType, &e.MerchantName, &e.AccountName, &e.AccountID,
			&e.Pending, &e.Source, &e.AccountOwner); err != nil {
			log.Printf("ERROR: scanning credit transaction row: %v", err)
			continue
		}

		if e.AccountID != currentAcct {
			currentAcct = e.AccountID
			running = acctBalances[currentAcct]
		}

		if !e.Pending {
			bal := round2(running)
			e.RunningBalance = &bal
			running -= e.Amount // Credit card: positive=charge increased balance, so undo by subtracting
		}
		// Pending transactions: RunningBalance stays nil

		allEntries = append(allEntries, e)
	}

	// ── Step 3b: Load overrides for description enrichment ──
	var ccOverrides []models.ReconciliationOverride
	h.GormDB.Find(&ccOverrides)
	ccOverrideMap := map[string]models.ReconciliationOverride{}
	for _, o := range ccOverrides {
		ccOverrideMap[o.PlaidID] = o
	}

	// ── Step 4: Apply search / account_owner display filters ──
	var filtered []ledgerEntry
	for _, e := range allEntries {
		// Build effective description for search
		desc := e.MerchantName
		if ov, ok := ccOverrideMap[e.PlaidID]; ok && ov.Description != "" {
			desc = ov.Description
		}
		if search != "" {
			lower := strings.ToLower(search)
			if !strings.Contains(strings.ToLower(e.Name), lower) &&
				!strings.Contains(strings.ToLower(desc), lower) &&
				!strings.Contains(strings.ToLower(e.MerchantName), lower) {
				continue
			}
		}
		if accountOwner != "" {
			if !strings.HasSuffix(strings.TrimSpace(e.AccountOwner), accountOwner) {
				continue
			}
		}
		filtered = append(filtered, e)
	}

	// ── Step 5: Count and pagination metadata ──
	totalTransactions := len(filtered)
	totalPages := int(math.Ceil(float64(totalTransactions) / float64(pageSize)))

	// ── Step 6: Sort filtered entries ──
	validSorts := map[string]string{
		"date": "date", "amount": "amount", "vendor": "vendor",
		"transaction_type": "transaction_type", "created_at": "created_at",
		"name": "name",
	}
	sortCol := "date"
	if v, ok := validSorts[sortBy]; ok {
		sortCol = v
	}
	asc := strings.ToLower(sortOrder) == "asc"

	switch sortCol {
	case "amount":
		sort.Slice(filtered, func(i, j int) bool {
			if asc {
				return filtered[i].Amount < filtered[j].Amount
			}
			return filtered[i].Amount > filtered[j].Amount
		})
	case "vendor":
		sort.Slice(filtered, func(i, j int) bool {
			if asc {
				return strings.ToLower(filtered[i].Vendor) < strings.ToLower(filtered[j].Vendor)
			}
			return strings.ToLower(filtered[i].Vendor) > strings.ToLower(filtered[j].Vendor)
		})
	default:
		sort.Slice(filtered, func(i, j int) bool {
			if asc {
				return filtered[i].Date < filtered[j].Date
			}
			return filtered[i].Date > filtered[j].Date
		})
	}

	// ── Step 7: Paginate ──
	offset := (page - 1) * pageSize
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}

	ledger := []gin.H{}
	if offset < len(filtered) {
		for _, e := range filtered[offset:end] {
			// Description = override description > merchant_name
			desc := e.MerchantName
			if ov, ok := ccOverrideMap[e.PlaidID]; ok && ov.Description != "" {
				desc = ov.Description
			}
			entry := gin.H{
				"id":               e.ID,
				"plaid_id":         e.PlaidID,
				"date":             e.Date,
				"amount":           round2(e.Amount),
				"vendor":           e.Name,         // vendor = raw Plaid name
				"name":             e.Name,
				"description":      desc,            // override description or merchant_name
				"merchant_name":    e.MerchantName,
				"account_name":     e.AccountName,
				"account_id":       e.AccountID,
				"pending":          e.Pending,
				"source":           e.Source,
				"account_owner":    e.AccountOwner,
			}
			if e.RunningBalance != nil {
				entry["running_balance"] = *e.RunningBalance
			} else {
				entry["running_balance"] = nil
			}
			ledger = append(ledger, entry)
		}
	}

	// ── Step 9: Compute summary ──
	now := time.Now()
	monthStart := fmt.Sprintf("%d-%02d-01", now.Year(), now.Month())
	totalCharges := 0.0
	totalPayments := 0.0
	for _, e := range allEntries {
		if e.Date >= monthStart && !e.Pending {
			if e.Amount > 0 {
				totalCharges += e.Amount
			} else {
				totalPayments += -e.Amount
			}
		}
	}

	totalBalance := 0.0
	totalAvailable := 0.0
	for _, a := range ccAccounts {
		totalBalance += a.CurrentBalance
		totalAvailable += a.AvailableBalance
	}

	accountsJSON := []gin.H{}
	for _, a := range ccAccounts {
		accountsJSON = append(accountsJSON, gin.H{
			"account_id":        a.AccountID,
			"account_name":      a.AccountName,
			"current_balance":   round2(a.CurrentBalance),
			"available_balance": round2(a.AvailableBalance),
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"accounts":           accountsJSON,
		"ledger":             ledger,
		"total_transactions": totalTransactions,
		"page":               page,
		"page_size":          pageSize,
		"total_pages":        totalPages,
		"sort_by":            sortBy,
		"sort_order":         sortOrder,
		"summary": gin.H{
			"total_charges_this_month":  round2(totalCharges),
			"total_payments_this_month": round2(totalPayments),
			"current_balance":           round2(totalBalance),
			"available_credit":          round2(totalAvailable),
		},
	})
}

// GET /dashboard/vendor/:vendor_id/ledger
func (h *DashboardHandler) GetVendorLedger(c *gin.Context) {
	ctx := context.Background()
	vendorID := c.Param("vendor_id")
	status := c.DefaultQuery("status", "")

	// Get vendor name
	var vendorName string
	err := h.sqlDB().QueryRowContext(ctx, "SELECT name FROM vendors WHERE id = $1", vendorID).Scan(&vendorName)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "data": gin.H{"detail": "Vendor not found"}})
		return
	}

	// Query pay_bills for this vendor
	where := "WHERE vendor_id = $1"
	args := []interface{}{vendorID}
	argIdx := 2

	today := time.Now().Format("2006-01-02")
	if status == "current" {
		where += fmt.Sprintf(" AND date >= $%d", argIdx)
		args = append(args, today)
		argIdx++
	} else if status == "overdue" {
		where += fmt.Sprintf(" AND date < $%d", argIdx)
		args = append(args, today)
		argIdx++
	}

	rows, err := h.sqlDB().QueryContext(ctx,
		fmt.Sprintf(`SELECT id, COALESCE(amount, 0), COALESCE(date::text, ''),
		        COALESCE(category, ''), COALESCE(reference, ''), COALESCE(invoice_url, '')
		 FROM pay_bills %s ORDER BY date DESC`, where), args...)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "data": gin.H{"detail": "failed to query invoices"}, "error": err.Error()})
		return
	}
	defer rows.Close()

	totalBalance := 0.0
	invoices := []gin.H{}
	for rows.Next() {
		var id int
		var amount float64
		var dateStr, category, reference, invoiceURL string
		rows.Scan(&id, &amount, &dateStr, &category, &reference, &invoiceURL)
		totalBalance += amount
		invoices = append(invoices, gin.H{
			"id":          id,
			"amount":      round2(amount),
			"date":        dateStr,
			"category":    category,
			"reference":   reference,
			"invoice_url": invoiceURL,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"vendor_name":   vendorName,
			"total_balance": round2(totalBalance),
			"invoices":      invoices,
		},
	})
}

// GET /dashboard/credit-card-balances-monthly
func (h *DashboardHandler) GetCreditCardBalancesMonthly(c *gin.Context) {
	ctx := context.Background()
	months := parseIntDefault(c.DefaultQuery("months", "6"), 6)

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(starting_credit_card_bal, 0), COALESCE(minimum_balance, 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type pmInfo struct {
		ID          string
		Title       string
		StartingBal float64
		MinBal      float64
	}
	var pms []pmInfo
	for rows.Next() {
		var p pmInfo
		rows.Scan(&p.ID, &p.Title, &p.StartingBal, &p.MinBal)
		pms = append(pms, p)
	}

	now := time.Now()
	results := []gin.H{}

	for _, pm := range pms {
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT COALESCE(date::text, ''), COALESCE(amount, 0)
			 FROM transactions WHERE account_type = 'depository' AND account_id = $1
			 ORDER BY date, created_at`, pm.ID)
		if err != nil {
			continue
		}

		type txEntry struct {
			Date   string
			Amount float64
		}
		var allTx []txEntry
		for txRows.Next() {
			var t txEntry
			txRows.Scan(&t.Date, &t.Amount)
			allTx = append(allTx, t)
		}
		txRows.Close()

		// Compute monthly balances
		monthlyBalance := map[string]float64{}
		monthlyShortfall := map[string]float64{}
		balance := pm.StartingBal
		txIdx := 0

		for m := months - 1; m >= 0; m-- {
			targetMonth := now.AddDate(0, -m, 0)
			monthStart := time.Date(targetMonth.Year(), targetMonth.Month(), 1, 0, 0, 0, 0, time.UTC)
			monthEnd := monthStart.AddDate(0, 1, -1)
			monthLabel := monthStart.Format("Jan 2006")

			// Process transactions up to end of this month
			for txIdx < len(allTx) {
				txDate, err := time.Parse("2006-01-02", allTx[txIdx].Date)
				if err != nil {
					txIdx++
					continue
				}
				if !txDate.After(monthEnd) {
					balance += allTx[txIdx].Amount * -1
					txIdx++
				} else {
					break
				}
			}

			monthlyBalance[monthLabel] = round2(balance)
			shortfall := pm.MinBal - balance
			if shortfall > 0 {
				monthlyShortfall[monthLabel] = round2(shortfall)
			} else {
				monthlyShortfall[monthLabel] = 0
			}
		}

		results = append(results, gin.H{
			"account_id":        pm.ID,
			"account_name":      pm.Title,
			"minimum_balance":   pm.MinBal,
			"monthly_bank_balance": monthlyBalance,
			"monthly_shortfall": monthlyShortfall,
		})
	}

	c.JSON(http.StatusOK, results)
}

// GET /dashboard/credit-card-balances-weekly
func (h *DashboardHandler) GetCreditCardBalancesWeekly(c *gin.Context) {
	ctx := context.Background()
	weeks := parseIntDefault(c.DefaultQuery("weeks", "1"), 1)

	now := time.Now()
	startDate := now.AddDate(0, 0, -7*weeks)

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(starting_credit_card_bal, 0), COALESCE(minimum_balance, 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type pmInfo struct {
		ID          string
		Title       string
		StartingBal float64
		MinBal      float64
	}
	var pms []pmInfo
	for rows.Next() {
		var p pmInfo
		rows.Scan(&p.ID, &p.Title, &p.StartingBal, &p.MinBal)
		pms = append(pms, p)
	}

	results := []gin.H{}

	for _, pm := range pms {
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT COALESCE(date::text, ''), COALESCE(amount, 0)
			 FROM transactions WHERE account_type = 'depository' AND account_id = $1
			 ORDER BY date, created_at`, pm.ID)
		if err != nil {
			continue
		}

		type txEntry struct {
			Date   string
			Amount float64
		}
		var allTx []txEntry
		for txRows.Next() {
			var t txEntry
			txRows.Scan(&t.Date, &t.Amount)
			allTx = append(allTx, t)
		}
		txRows.Close()

		// Compute daily balances over the range
		dailyBalance := map[string]float64{}
		dailyShortfall := map[string]float64{}
		balance := pm.StartingBal
		txIdx := 0
		hasShortfall := false

		// Process transactions before start date
		for txIdx < len(allTx) {
			txDate, err := time.Parse("2006-01-02", allTx[txIdx].Date)
			if err != nil {
				txIdx++
				continue
			}
			if txDate.Before(startDate) {
				balance += allTx[txIdx].Amount * -1
				txIdx++
			} else {
				break
			}
		}

		current := startDate
		for !current.After(now) {
			dateStr := current.Format("2006-01-02")
			for txIdx < len(allTx) && allTx[txIdx].Date <= dateStr {
				balance += allTx[txIdx].Amount * -1
				txIdx++
			}
			label := current.Format("Monday, Jan 02, 2006")
			dailyBalance[label] = round2(balance)
			shortfall := pm.MinBal - balance
			if shortfall > 0 {
				dailyShortfall[label] = round2(shortfall)
				hasShortfall = true
			} else {
				dailyShortfall[label] = 0
			}
			current = current.AddDate(0, 0, 1)
		}

		// Only include accounts that have shortfalls
		if hasShortfall {
			results = append(results, gin.H{
				"account_id":       pm.ID,
				"account_name":     pm.Title,
				"minimum_balance":  pm.MinBal,
				"daily_balance":    dailyBalance,
				"daily_shortfall":  dailyShortfall,
			})
		}
	}

	c.JSON(http.StatusOK, results)
}

// GET /dashboard/credit
func (h *DashboardHandler) GetCredit(c *gin.Context) {
	ctx := context.Background()
	weeks := parseIntDefault(c.DefaultQuery("weeks", "1"), 1)

	now := time.Now()
	startDate := now.AddDate(0, 0, -7*weeks)

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(starting_credit_card_bal, 0)
		 FROM payment_methods WHERE method_type = 'CREDIT_CARD'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type pmInfo struct {
		ID          string
		StartingBal float64
	}
	var pms []pmInfo
	for rows.Next() {
		var p pmInfo
		rows.Scan(&p.ID, &p.StartingBal)
		pms = append(pms, p)
	}

	// For each day, compute cumulative balance across all CC accounts
	type txEntry struct {
		AccountID string
		Date      string
		Amount    float64
	}

	// Gather all transactions for all CC accounts — only primary cardholder
	allTx := []txEntry{}
	for _, pm := range pms {
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT COALESCE(date::text, ''), COALESCE(amount, 0)
			 FROM transactions WHERE account_type = 'credit' AND account_id = $1
			   AND (account_owner IS NULL OR account_owner = '' OR UPPER(account_owner) LIKE '%ROBERT%SALADNA%')
			 ORDER BY date, created_at`, pm.ID)
		if err != nil {
			continue
		}
		for txRows.Next() {
			var t txEntry
			t.AccountID = pm.ID
			txRows.Scan(&t.Date, &t.Amount)
			allTx = append(allTx, t)
		}
		txRows.Close()
	}

	// Compute per-account balances, then sum daily
	accountBalances := map[string]float64{}
	for _, pm := range pms {
		accountBalances[pm.ID] = pm.StartingBal
	}

	// Process all transactions before start date
	txByAccount := map[string][]txEntry{}
	for _, t := range allTx {
		txByAccount[t.AccountID] = append(txByAccount[t.AccountID], t)
	}

	accountTxIdx := map[string]int{}
	for _, pm := range pms {
		idx := 0
		txs := txByAccount[pm.ID]
		for idx < len(txs) {
			txDate, err := time.Parse("2006-01-02", txs[idx].Date)
			if err != nil {
				idx++
				continue
			}
			if txDate.Before(startDate) {
				accountBalances[pm.ID] += txs[idx].Amount * -1
				idx++
			} else {
				break
			}
		}
		accountTxIdx[pm.ID] = idx
	}

	results := []gin.H{}
	current := startDate
	for !current.After(now) {
		dateStr := current.Format("2006-01-02")
		for _, pm := range pms {
			txs := txByAccount[pm.ID]
			idx := accountTxIdx[pm.ID]
			for idx < len(txs) && txs[idx].Date <= dateStr {
				accountBalances[pm.ID] += txs[idx].Amount * -1
				idx++
			}
			accountTxIdx[pm.ID] = idx
		}

		totalBalance := 0.0
		for _, bal := range accountBalances {
			totalBalance += bal
		}

		results = append(results, gin.H{
			"date":          dateStr,
			"total_balance": round2(totalBalance),
		})
		current = current.AddDate(0, 0, 1)
	}

	c.JSON(http.StatusOK, results)
}

// GET /dashboard/api/credit_cards_due_soon
func (h *DashboardHandler) GetCreditCardsDueSoon(c *gin.Context) {
	ctx := context.Background()

	now := time.Now()
	thirtyDaysLater := now.AddDate(0, 0, 30)

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT pm.id, COALESCE(pm.plaid_account_id, ''), COALESCE(pm.title, ''), COALESCE(pm.card_name, ''),
		        COALESCE(pm.card_last_4_digits, ''), COALESCE(pm.balance_available, 0),
		        COALESCE(pm.balance_current, 0), COALESCE(pm.credit_limit, 0),
		        pm.next_payment_due_date
		 FROM payment_methods pm
		 WHERE pm.method_type = 'CREDIT_CARD'
		   AND pm.next_payment_due_date IS NOT NULL
		   AND pm.next_payment_due_date <= $1
		   AND (
		     NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = pm.id AND t.account_type = 'credit')
		     OR EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = pm.id AND t.account_type = 'credit'
		                AND (t.account_owner IS NULL OR t.account_owner = '' OR UPPER(t.account_owner) LIKE '%ROBERT%SALADNA%'))
		   )
		 ORDER BY pm.next_payment_due_date`, thirtyDaysLater)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query credit cards", "error": err.Error()})
		return
	}
	defer rows.Close()

	results := []gin.H{}
	for rows.Next() {
		var id, plaidAccountID, title, cardName, cardLast4 string
		var balAvail, balCurrent, creditLimit float64
		var dueDate *time.Time
		rows.Scan(&id, &plaidAccountID, &title, &cardName, &cardLast4,
			&balAvail, &balCurrent, &creditLimit, &dueDate)

		entry := gin.H{
			"plaid_account_id":      plaidAccountID,
			"account_name":          title,
			"card_name":             cardName,
			"card_last_4_digits":    cardLast4,
			"balance_available":     round2(balAvail),
			"balance_current":       round2(balCurrent),
			"credit_limit":          round2(creditLimit),
		}
		if dueDate != nil {
			entry["next_payment_due_date"] = dueDate.Format("2006-01-02")
		}
		results = append(results, entry)
	}

	c.JSON(http.StatusOK, results)
}

// GET /dashboard/low-balance-account
func (h *DashboardHandler) GetLowBalanceAccount(c *gin.Context) {
	ctx := context.Background()

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(bank_name, ''),
		        COALESCE(balance_available, 0), COALESCE(balance_current, 0),
		        COALESCE(starting_credit_card_bal, 0), COALESCE(minimum_balance, 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'
		 ORDER BY sorting_order, created_at`)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query payment methods", "error": err.Error()})
		return
	}
	defer rows.Close()

	type acctInfo struct {
		ID          string
		Title       string
		BankName    string
		BalAvail    float64
		BalCurrent  float64
		StartingBal float64
		MinBal      float64
	}
	var accounts []acctInfo
	for rows.Next() {
		var a acctInfo
		rows.Scan(&a.ID, &a.Title, &a.BankName, &a.BalAvail, &a.BalCurrent, &a.StartingBal, &a.MinBal)
		accounts = append(accounts, a)
	}

	mainShortfall := 0.0
	data := []gin.H{}

	for _, a := range accounts {
		// Compute running balance
		txRows, err := h.sqlDB().QueryContext(ctx,
			`SELECT COALESCE(amount, 0) FROM transactions WHERE account_type = 'depository' AND account_id = $1 ORDER BY date, created_at`, a.ID)
		if err != nil {
			continue
		}
		balance := a.StartingBal
		for txRows.Next() {
			var amt float64
			txRows.Scan(&amt)
			balance += amt * -1
		}
		txRows.Close()
		balance = round2(balance)

		if balance < a.MinBal {
			shortfall := round2(a.MinBal - balance)
			mainShortfall += shortfall
			data = append(data, gin.H{
				"account_id":      a.ID,
				"account_name":    a.Title,
				"bank_name":       a.BankName,
				"balance":         balance,
				"minimum_balance": a.MinBal,
				"shortfall":       shortfall,
			})
		}
	}

	// Top 10 recent transactions across low balance accounts
	lowAcctIDs := []string{}
	for _, d := range data {
		lowAcctIDs = append(lowAcctIDs, d["account_id"].(string))
	}

	transactions := []gin.H{}
	if len(lowAcctIDs) > 0 {
		// Build IN clause for account IDs
		placeholders := make([]string, len(lowAcctIDs))
		inArgs := make([]interface{}, len(lowAcctIDs))
		for i, id := range lowAcctIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			inArgs[i] = id
		}
		inClause := strings.Join(placeholders, ", ")
		txRows, err := h.sqlDB().QueryContext(ctx,
			fmt.Sprintf(`SELECT id, COALESCE(account_id, ''), COALESCE(date::text, ''), COALESCE(amount, 0),
			        COALESCE(vendor, ''), COALESCE(name, ''), COALESCE(transaction_type, '')
			 FROM transactions WHERE account_type = 'depository' AND account_id IN (%s)
			 ORDER BY date DESC, created_at DESC LIMIT 10`, inClause), inArgs...)
		if err == nil {
			for txRows.Next() {
				var txID, acctID, dateStr, vendor, name, txType string
				var amt float64
				txRows.Scan(&txID, &acctID, &dateStr, &amt, &vendor, &name, &txType)
				transactions = append(transactions, gin.H{
					"id": txID, "account_id": acctID, "date": dateStr,
					"amount": round2(amt), "vendor": vendor, "name": name,
					"transaction_type": txType,
				})
			}
			txRows.Close()
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"main_shortfall": round2(mainShortfall),
		"data":           data,
		"transactions":   transactions,
	})
}

// GET /dashboard/overdue-payables
func (h *DashboardHandler) GetOverduePayables(c *gin.Context) {
	ctx := context.Background()
	today := time.Now().Format("2006-01-02")

	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT v.id, v.name, COALESCE(SUM(pb.amount), 0) as total, COUNT(pb.id) as bill_count
		 FROM pay_bills pb
		 JOIN vendors v ON pb.vendor_id = v.id
		 WHERE pb.date < $1
		 GROUP BY v.id, v.name
		 ORDER BY total DESC`, today)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query overdue payables", "error": err.Error()})
		return
	}
	defer rows.Close()

	totalOverdue := 0.0
	vendors := []gin.H{}
	for rows.Next() {
		var vendorID, vendorName string
		var total float64
		var billCount int
		rows.Scan(&vendorID, &vendorName, &total, &billCount)
		totalOverdue += total
		vendors = append(vendors, gin.H{
			"vendor_id":   vendorID,
			"vendor_name": vendorName,
			"total":       round2(total),
			"bill_count":  billCount,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"total_overdue": round2(totalOverdue),
		"vendors":       vendors,
	})
}

// GET /dashboard/accounts-payable
func (h *DashboardHandler) GetAccountsPayable(c *gin.Context) {
	ctx := context.Background()
	today := time.Now().Format("2006-01-02")

	// Overdue vendors
	overdueRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT v.id, v.name, COALESCE(SUM(pb.amount), 0), COUNT(pb.id)
		 FROM pay_bills pb
		 JOIN vendors v ON pb.vendor_id = v.id
		 WHERE pb.date < $1
		 GROUP BY v.id, v.name
		 ORDER BY SUM(pb.amount) DESC`, today)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query overdue payables", "error": err.Error()})
		return
	}
	defer overdueRows.Close()

	totalOverdue := 0.0
	overdueVendors := []gin.H{}
	for overdueRows.Next() {
		var vendorID, vendorName string
		var total float64
		var billCount int
		overdueRows.Scan(&vendorID, &vendorName, &total, &billCount)
		totalOverdue += total
		overdueVendors = append(overdueVendors, gin.H{
			"vendor_id":   vendorID,
			"vendor_name": vendorName,
			"total":       round2(total),
			"bill_count":  billCount,
		})
	}

	// Current vendors
	currentRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT v.id, v.name, COALESCE(SUM(pb.amount), 0), COUNT(pb.id)
		 FROM pay_bills pb
		 JOIN vendors v ON pb.vendor_id = v.id
		 WHERE pb.date >= $1
		 GROUP BY v.id, v.name
		 ORDER BY SUM(pb.amount) DESC`, today)
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query current payables", "error": err.Error()})
		return
	}
	defer currentRows.Close()

	totalCurrent := 0.0
	currentVendors := []gin.H{}
	for currentRows.Next() {
		var vendorID, vendorName string
		var total float64
		var billCount int
		currentRows.Scan(&vendorID, &vendorName, &total, &billCount)
		totalCurrent += total
		currentVendors = append(currentVendors, gin.H{
			"vendor_id":   vendorID,
			"vendor_name": vendorName,
			"total":       round2(total),
			"bill_count":  billCount,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"total_overdue_payable":  round2(totalOverdue),
		"total_current_payable":  round2(totalCurrent),
		"overdue_vendors":        overdueVendors,
		"current_vendors":        currentVendors,
	})
}

// GET /dashboard/items-need-attention
func (h *DashboardHandler) GetItemsNeedAttention(c *gin.Context) {
	ctx := context.Background()
	today := time.Now().Format("2006-01-02")
	now := time.Now()
	thirtyDaysLater := now.AddDate(0, 0, 30)

	// 1. Overdue payables summary
	var overdueTotal float64
	var overdueCount int
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(amount), 0), COUNT(*)
		 FROM pay_bills WHERE date < $1`, today).Scan(&overdueTotal, &overdueCount)

	// 2. Credit cards due soon
	ccRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(card_name, ''),
		        COALESCE(balance_current, 0), next_payment_due_date
		 FROM payment_methods
		 WHERE method_type = 'CREDIT_CARD'
		   AND next_payment_due_date IS NOT NULL
		   AND next_payment_due_date <= $1
		 ORDER BY next_payment_due_date`, thirtyDaysLater)
	creditCardOverdue := []gin.H{}
	if err == nil {
		for ccRows.Next() {
			var id, title, cardName string
			var balCurrent float64
			var dueDate *time.Time
			ccRows.Scan(&id, &title, &cardName, &balCurrent, &dueDate)
			entry := gin.H{
				"id":              id,
				"account_name":    title,
				"card_name":       cardName,
				"balance_current": round2(balCurrent),
			}
			if dueDate != nil {
				entry["next_payment_due_date"] = dueDate.Format("2006-01-02")
			}
			creditCardOverdue = append(creditCardOverdue, entry)
		}
		ccRows.Close()
	}

	// 3. Lowest bank account (low balance)
	pmRows, err := h.sqlDB().QueryContext(ctx,
		`SELECT id, COALESCE(title, ''), COALESCE(starting_credit_card_bal, 0), COALESCE(minimum_balance, 0)
		 FROM payment_methods WHERE method_type = 'DEPOSITORY'
		 ORDER BY sorting_order, created_at`)
	var lowestAccount gin.H
	lowestBalance := math.MaxFloat64
	if err == nil {
		for pmRows.Next() {
			var id, title string
			var startBal, minBal float64
			pmRows.Scan(&id, &title, &startBal, &minBal)

			// Compute balance
			balance := startBal
			txRows, txErr := h.sqlDB().QueryContext(ctx,
				`SELECT COALESCE(amount, 0) FROM transactions WHERE account_type = 'depository' AND account_id = $1 ORDER BY date, created_at`, id)
			if txErr == nil {
				for txRows.Next() {
					var amt float64
					txRows.Scan(&amt)
					balance += amt * -1
				}
				txRows.Close()
			}
			balance = round2(balance)

			if balance < minBal && balance < lowestBalance {
				lowestBalance = balance
				lowestAccount = gin.H{
					"account_id":      id,
					"account_name":    title,
					"balance":         balance,
					"minimum_balance": minBal,
					"shortfall":       round2(minBal - balance),
				}
			}
		}
		pmRows.Close()
	}

	// 4. Account receivable summary
	currentMonth := now.Format("2006-01")
	var totalReceivable float64
	var totalROs int
	h.sqlDB().QueryRowContext(ctx,
		`SELECT COALESCE(SUM(j.subtotal), 0), COUNT(DISTINCT ro.id)
		 FROM tekmetric_repair_orders ro
		 JOIN tekmetric_jobs j ON j.repair_order_id = ro.id
		 WHERE ro.repair_order_status_code = 'WORKINPROGRESS'
		   AND TO_CHAR(ro.created_date, 'YYYY-MM') = $1`, currentMonth).Scan(&totalReceivable, &totalROs)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"overdue_payables": gin.H{
				"total_overdue":       round2(overdueTotal),
				"overdue_bill_count":  overdueCount,
			},
			"credit_card_overdue":  creditCardOverdue,
			"lowest_bank_account":  lowestAccount,
			"account_receivable": gin.H{
				"total_receivable":     round2(totalReceivable),
				"total_repair_orders":  totalROs,
			},
		},
	})
}

// GET /dashboard/accounts-payable-by-vendor
func (h *DashboardHandler) GetAccountsPayableByVendor(c *gin.Context) {
	ctx := context.Background()
	weeks := parseIntDefault(c.DefaultQuery("weeks", "1"), 1)

	now := time.Now()
	startDate := now.AddDate(0, 0, -7*weeks)

	// Get vendors with pay_bills in the date range
	rows, err := h.sqlDB().QueryContext(ctx,
		`SELECT v.id, v.name, pb.date::text, COALESCE(SUM(pb.amount), 0)
		 FROM pay_bills pb
		 JOIN vendors v ON pb.vendor_id = v.id
		 WHERE pb.date >= $1
		 GROUP BY v.id, v.name, pb.date
		 ORDER BY v.name, pb.date`, startDate.Format("2006-01-02"))
	if err != nil {
		log.Printf("ERROR: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query accounts payable", "error": err.Error()})
		return
	}
	defer rows.Close()

	type vendorDay struct {
		VendorID   string
		VendorName string
		Date       string
		Total      float64
	}

	vendorDays := []vendorDay{}
	for rows.Next() {
		var vd vendorDay
		rows.Scan(&vd.VendorID, &vd.VendorName, &vd.Date, &vd.Total)
		vendorDays = append(vendorDays, vd)
	}

	// Group by vendor
	vendorMap := map[string]gin.H{}
	vendorOrder := []string{}
	for _, vd := range vendorDays {
		if _, exists := vendorMap[vd.VendorID]; !exists {
			vendorMap[vd.VendorID] = gin.H{
				"vendor_id":    vd.VendorID,
				"vendor_name":  vd.VendorName,
				"daily_totals": map[string]float64{},
			}
			vendorOrder = append(vendorOrder, vd.VendorID)
		}
		dailyTotals := vendorMap[vd.VendorID]["daily_totals"].(map[string]float64)
		dailyTotals[vd.Date] = round2(vd.Total)
	}

	results := []gin.H{}
	for _, vid := range vendorOrder {
		results = append(results, vendorMap[vid])
	}

	c.JSON(http.StatusOK, results)
}
