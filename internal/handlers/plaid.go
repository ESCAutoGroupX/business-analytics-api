package handlers

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type PlaidHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

type publicTokenRequest struct {
	PublicToken string `json:"public_token" binding:"required"`
	UserID      string `json:"user_id" binding:"required"`
}

type transactionsRequest struct {
	StartDate string `json:"start_date" binding:"required"`
	EndDate   string `json:"end_date" binding:"required"`
}

type linkTokenRequest struct {
	UserID string `json:"user_id" binding:"required"`
}

func (h *PlaidHandler) plaidBaseURL() string {
	env := h.Cfg.PlaidEnv
	switch env {
	case "production":
		return "https://production.plaid.com"
	case "development":
		return "https://development.plaid.com"
	default:
		return "https://sandbox.plaid.com"
	}
}

func (h *PlaidHandler) plaidRequest(endpoint string, body interface{}) (map[string]interface{}, error) {
	payload := map[string]interface{}{
		"client_id": h.Cfg.PlaidClientID,
		"secret":    h.Cfg.PlaidSecret,
	}
	if bodyMap, ok := body.(map[string]interface{}); ok {
		for k, v := range bodyMap {
			payload[k] = v
		}
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(h.plaidBaseURL()+endpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		detail := "Plaid API error"
		if msg, ok := result["error_message"].(string); ok {
			detail = msg
		}
		if code, ok := result["error_code"].(string); ok && code != "" {
			return nil, fmt.Errorf("%s: %s", code, detail)
		}
		return nil, fmt.Errorf("%s", detail)
	}

	return result, nil
}

// AutoMigrate adds optional columns used for tracking item auth state.
func (h *PlaidHandler) AutoMigrate() {
	h.GormDB.Exec(`ALTER TABLE plaid_items ADD COLUMN IF NOT EXISTS needs_reauth BOOLEAN NOT NULL DEFAULT FALSE`)
	h.GormDB.Exec(`ALTER TABLE plaid_items ADD COLUMN IF NOT EXISTS last_error VARCHAR`)
}

// isReauthError returns true if err is a Plaid item-level auth failure
// that requires the user to re-link via Link update mode.
func isReauthError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ITEM_LOGIN_REQUIRED") ||
		strings.Contains(msg, "PENDING_EXPIRATION") ||
		strings.Contains(msg, "PENDING_DISCONNECT") ||
		strings.Contains(msg, "the login details of this item have changed")
}

// markItemNeedsReauth flags a plaid_item row as needing re-authentication.
// Safe to call for legacy rows with no ID (ID <= 0) — they are ignored.
func (h *PlaidHandler) markItemNeedsReauth(item *models.PlaidItem, reason string) {
	if item == nil || item.ID <= 0 {
		return
	}
	h.GormDB.Model(&models.PlaidItem{}).Where("id = ?", item.ID).Updates(map[string]interface{}{
		"needs_reauth": true,
		"last_error":   reason,
	})
}

// POST /plaid/exchange_public_token
func (h *PlaidHandler) ExchangePublicToken(c *gin.Context) {
	var req publicTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	result, err := h.plaidRequest("/item/public_token/exchange", map[string]interface{}{
		"public_token": req.PublicToken,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	accessToken, _ := result["access_token"].(string)
	itemID, _ := result["item_id"].(string)

	// Verify user exists
	var user models.User
	if err := h.GormDB.First(&user, "id = ?", req.UserID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Look up institution name via /item/get
	institutionName := "Unknown"
	institutionID := ""
	itemResult, itemErr := h.plaidRequest("/item/get", map[string]interface{}{
		"access_token": accessToken,
	})
	if itemErr == nil {
		if item, ok := itemResult["item"].(map[string]interface{}); ok {
			if instID, ok := item["institution_id"].(string); ok {
				institutionID = instID
				// Try to get institution name
				instResult, instErr := h.plaidRequest("/institutions/get_by_id", map[string]interface{}{
					"institution_id": instID,
					"country_codes":  []string{"US"},
				})
				if instErr == nil {
					if inst, ok := instResult["institution"].(map[string]interface{}); ok {
						if name, ok := inst["name"].(string); ok {
							institutionName = name
						}
					}
				}
			}
		}
	}

	// Save to plaid_items table
	plaidItem := models.PlaidItem{
		UserID:          req.UserID,
		ItemID:          itemID,
		AccessToken:     accessToken,
		InstitutionID:   institutionID,
		InstitutionName: institutionName,
	}
	if err := h.GormDB.Create(&plaidItem).Error; err != nil {
		log.Printf("ERROR saving plaid_item: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to store plaid item", "error": err.Error()})
		return
	}

	// Also update users.plaid_access_token for backwards compatibility
	if err := h.GormDB.Model(&user).Update("plaid_access_token", accessToken).Error; err != nil {
		log.Printf("ERROR updating user plaid_access_token: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"message": "Successfully linked bank account"})
}

// POST /plaid/fetch_transactions
func (h *PlaidHandler) FetchTransactions(c *gin.Context) {
	var req transactionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Query all plaid_items (company-wide — all users see the same transactions)
	var items []models.PlaidItem
	if err := h.GormDB.Where("needs_reauth = FALSE OR needs_reauth IS NULL").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query plaid items"})
		return
	}

	// Fallback: if no plaid_items, try legacy tokens from users table
	if len(items) == 0 {
		var users []models.User
		if err := h.GormDB.Select("plaid_access_token").Where("plaid_access_token IS NOT NULL AND plaid_access_token != ''").Find(&users).Error; err != nil || len(users) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "No Plaid access tokens found"})
			return
		}
		for _, u := range users {
			items = append(items, models.PlaidItem{AccessToken: *u.PlaidAccessToken})
		}
	}

	var allAccounts []interface{}
	var allTransactions []interface{}

	for i, item := range items {
		result, err := h.plaidRequest("/transactions/get", map[string]interface{}{
			"access_token": item.AccessToken,
			"start_date":   req.StartDate,
			"end_date":     req.EndDate,
		})
		if err != nil {
			if isReauthError(err) {
				h.markItemNeedsReauth(&items[i], err.Error())
			}
			log.Printf("WARN: failed to fetch transactions for item %s: %v", item.ItemID, err)
			continue
		}

		if accounts, ok := result["accounts"].([]interface{}); ok {
			allAccounts = append(allAccounts, accounts...)
		}
		if transactions, ok := result["transactions"].([]interface{}); ok {
			allTransactions = append(allTransactions, transactions...)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"accounts":     allAccounts,
		"transactions": allTransactions,
	})
}

// GET /plaid/sync_transactions
func (h *PlaidHandler) SyncTransactions(c *gin.Context) {
	// Query all plaid_items (company-wide — all users see the same transactions)
	var items []models.PlaidItem
	if err := h.GormDB.Where("needs_reauth = FALSE OR needs_reauth IS NULL").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query plaid items"})
		return
	}

	// Fallback: if no plaid_items, try legacy tokens from users table
	if len(items) == 0 {
		var users []models.User
		if err := h.GormDB.Select("id, plaid_access_token, plaid_cursor").Where("plaid_access_token IS NOT NULL AND plaid_access_token != ''").Find(&users).Error; err != nil || len(users) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "No Plaid access tokens found"})
			return
		}
		for _, u := range users {
			cursor := ""
			if u.PlaidCursor != nil {
				cursor = *u.PlaidCursor
			}
			items = append(items, models.PlaidItem{UserID: fmt.Sprintf("%v", u.ID), AccessToken: *u.PlaidAccessToken, Cursor: cursor})
		}
	}

	var allAdded []interface{}
	var allModified []interface{}
	var allRemoved []interface{}

	for i, item := range items {
		reqBody := map[string]interface{}{
			"access_token": item.AccessToken,
		}
		if item.Cursor != "" {
			reqBody["cursor"] = item.Cursor
		}

		result, err := h.plaidRequest("/transactions/sync", reqBody)
		if err != nil {
			if isReauthError(err) {
				h.markItemNeedsReauth(&items[i], err.Error())
			}
			log.Printf("WARN: failed to sync transactions for item %s: %v", item.ItemID, err)
			continue
		}

		if added, ok := result["added"].([]interface{}); ok {
			allAdded = append(allAdded, added...)
		}
		if modified, ok := result["modified"].([]interface{}); ok {
			allModified = append(allModified, modified...)
		}
		if removed, ok := result["removed"].([]interface{}); ok {
			allRemoved = append(allRemoved, removed...)
		}

		// Update cursor for this item
		if newCursor, ok := result["next_cursor"].(string); ok && newCursor != "" {
			if item.ID > 0 {
				h.GormDB.Model(&items[i]).Update("cursor", newCursor)
			} else {
				// Legacy fallback — update users table
				h.GormDB.Model(&models.User{}).Where("id = ?", item.UserID).Update("plaid_cursor", newCursor)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"added":    allAdded,
		"modified": allModified,
		"removed":  allRemoved,
	})
}

// POST /plaid/link-token
func (h *PlaidHandler) CreateLinkToken(c *gin.Context) {
	var req linkTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Verify user exists
	var count int64
	h.GormDB.Model(&models.User{}).Where("id = ?", req.UserID).Count(&count)
	if count == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	result, err := h.plaidRequest("/link/token/create", map[string]interface{}{
		"user":                          map[string]string{"client_user_id": req.UserID},
		"products":                      []string{"transactions", "auth"},
		"additional_consented_products": []string{"investments", "liabilities"},
		"country_codes":                 []string{"US"},
		"language":                      "en",
		"client_name":                   "Business Analytics",
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	linkToken, _ := result["link_token"].(string)
	c.JSON(http.StatusOK, gin.H{"link_token": linkToken})
}

// GET /plaid/items — list all connected Plaid items for the current user
func (h *PlaidHandler) ListPlaidItems(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	var items []models.PlaidItem
	if err := h.GormDB.Where("user_id = ?", uid).Order("created_at DESC").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query plaid items"})
		return
	}

	c.JSON(http.StatusOK, items)
}

// DELETE /plaid/items/:id — disconnect a Plaid item
func (h *PlaidHandler) DeletePlaidItem(c *gin.Context) {
	userID, _ := c.Get("user_id")
	uid := fmt.Sprintf("%v", userID)

	itemID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid item id"})
		return
	}

	var item models.PlaidItem
	if err := h.GormDB.Where("id = ? AND user_id = ?", itemID, uid).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "plaid item not found"})
		return
	}

	if err := h.GormDB.Delete(&item).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete plaid item"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Plaid item disconnected"})
}

// SyncPlaidTransactions runs as a cron job — syncs transactions for every
// plaid_item using the cursor-based /transactions/sync endpoint.  Items with
// a NULL/empty cursor get an initial full sync (up to 730 days).
func (h *PlaidHandler) SyncPlaidTransactions() {
	var items []models.PlaidItem
	if err := h.GormDB.Where("needs_reauth = FALSE OR needs_reauth IS NULL").Find(&items).Error; err != nil {
		log.Printf("PlaidSync: failed to query plaid_items: %v", err)
		return
	}
	if len(items) == 0 {
		return
	}

	for i, item := range items {
		totalAdded, totalModified, totalRemoved, dbErrors := 0, 0, 0, 0
		cursor := item.Cursor
		hasMore := true

		for hasMore {
			reqBody := map[string]interface{}{
				"access_token": item.AccessToken,
			}
			if cursor != "" {
				reqBody["cursor"] = cursor
			}

			result, err := h.plaidRequest("/transactions/sync", reqBody)
			if err != nil {
				if isReauthError(err) {
					h.markItemNeedsReauth(&items[i], err.Error())
					log.Printf("PlaidSync: %s (id=%d): marked needs_reauth: %v", item.InstitutionName, item.ID, err)
				} else {
					log.Printf("PlaidSync: %s (id=%d): API error: %v", item.InstitutionName, item.ID, err)
				}
				break
			}

			// Build account_id → metadata lookup from accounts array
			acctMeta := map[string]struct{ Name, Type, Subtype string }{}
			if accounts, ok := result["accounts"].([]interface{}); ok {
				for _, a := range accounts {
					acct, _ := a.(map[string]interface{})
					aid, _ := acct["account_id"].(string)
					aname, _ := acct["name"].(string)
					atype, _ := acct["type"].(string)
					asub, _ := acct["subtype"].(string)
					acctMeta[aid] = struct{ Name, Type, Subtype string }{aname, atype, asub}
				}
			}

			// Process added transactions
			if added, ok := result["added"].([]interface{}); ok {
				for _, t := range added {
					if err := h.upsertPlaidTransaction(t, acctMeta); err != nil {
						dbErrors++
					}
				}
				totalAdded += len(added)
			}

			// Process modified transactions
			if modified, ok := result["modified"].([]interface{}); ok {
				for _, t := range modified {
					if err := h.upsertPlaidTransaction(t, acctMeta); err != nil {
						dbErrors++
					}
				}
				totalModified += len(modified)
			}

			// Process removed transactions
			if removed, ok := result["removed"].([]interface{}); ok {
				for _, r := range removed {
					rm, _ := r.(map[string]interface{})
					if txID, ok := rm["transaction_id"].(string); ok && txID != "" {
						h.GormDB.Where("plaid_id = ?", txID).Delete(&models.Transaction{})
					}
				}
				totalRemoved += len(removed)
			}

			// Advance cursor
			if nc, ok := result["next_cursor"].(string); ok && nc != "" {
				cursor = nc
			}
			if hm, ok := result["has_more"].(bool); ok {
				hasMore = hm
			} else {
				hasMore = false
			}
		}

		// Persist cursor
		if cursor != items[i].Cursor {
			h.GormDB.Model(&items[i]).Update("cursor", cursor)
		}

		if totalAdded > 0 || totalModified > 0 || totalRemoved > 0 {
			log.Printf("PlaidSync: %s (id=%d): +%d added, ~%d modified, -%d removed, %d db_errors",
				item.InstitutionName, item.ID, totalAdded, totalModified, totalRemoved, dbErrors)
		}
	}
}

// upsertPlaidTransaction maps a single Plaid transaction JSON object to the
// Transaction model and upserts it by plaid_id.
func (h *PlaidHandler) upsertPlaidTransaction(
	t interface{},
	acctMeta map[string]struct{ Name, Type, Subtype string },
) error {
	tx, ok := t.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid transaction object")
	}

	plaidID := plaidStr(tx, "transaction_id")
	if plaidID == nil || *plaidID == "" {
		return fmt.Errorf("missing transaction_id")
	}

	accountID := plaidStr(tx, "account_id")
	var acctName, acctType, acctSubtype *string
	if accountID != nil {
		if meta, ok := acctMeta[*accountID]; ok {
			acctName = &meta.Name
			acctType = &meta.Type
			acctSubtype = &meta.Subtype
		}
	}

	id := uuid.New().String()

	result := h.GormDB.Exec(`
		INSERT INTO transactions (
			id, plaid_id, account_id, date, authorized_date, amount,
			currency_iso, name, merchant_name, merchant_entity_id,
			website, logo_url, payment_channel, transaction_type,
			transaction_code, pending, pending_id, account_owner,
			check_number, category, personal_finance_category,
			location, counterparties, payment_meta,
			account_name, account_type, account_subtype, source
		) VALUES (
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?::json, ?::json,
			?::json, ?::json, ?::json,
			?, ?, ?, ?
		)
		ON CONFLICT (plaid_id) DO UPDATE SET
			account_id = EXCLUDED.account_id,
			date = EXCLUDED.date,
			authorized_date = EXCLUDED.authorized_date,
			amount = EXCLUDED.amount,
			currency_iso = EXCLUDED.currency_iso,
			name = EXCLUDED.name,
			merchant_name = EXCLUDED.merchant_name,
			merchant_entity_id = EXCLUDED.merchant_entity_id,
			website = EXCLUDED.website,
			logo_url = EXCLUDED.logo_url,
			payment_channel = EXCLUDED.payment_channel,
			transaction_type = EXCLUDED.transaction_type,
			transaction_code = EXCLUDED.transaction_code,
			pending = EXCLUDED.pending,
			pending_id = EXCLUDED.pending_id,
			account_owner = EXCLUDED.account_owner,
			check_number = EXCLUDED.check_number,
			category = EXCLUDED.category,
			personal_finance_category = EXCLUDED.personal_finance_category,
			location = EXCLUDED.location,
			counterparties = EXCLUDED.counterparties,
			payment_meta = EXCLUDED.payment_meta,
			account_name = EXCLUDED.account_name,
			account_type = EXCLUDED.account_type,
			account_subtype = EXCLUDED.account_subtype,
			source = EXCLUDED.source,
			updated_at = NOW()
	`,
		id, plaidID, accountID, plaidStr(tx, "date"), plaidStr(tx, "authorized_date"), plaidFloat(tx, "amount"),
		plaidStr(tx, "iso_currency_code"), plaidStr(tx, "name"), plaidStr(tx, "merchant_name"), plaidStr(tx, "merchant_entity_id"),
		plaidStr(tx, "website"), plaidStr(tx, "logo_url"), plaidStr(tx, "payment_channel"), plaidStr(tx, "transaction_type"),
		plaidStr(tx, "transaction_code"), plaidBool(tx, "pending"), plaidStr(tx, "pending_transaction_id"), plaidStr(tx, "account_owner"),
		plaidStr(tx, "check_number"), plaidJSON(tx, "category"), plaidJSON(tx, "personal_finance_category"),
		plaidJSON(tx, "location"), plaidJSON(tx, "counterparties"), plaidJSON(tx, "payment_meta"),
		acctName, acctType, acctSubtype, "plaid",
	)
	if result.Error != nil {
		log.Printf("PlaidSync: DB error for %s: %v", *plaidID, result.Error)
		return result.Error
	}
	return nil
}

// ── Plaid JSON field helpers ─────────────────────────────────────────────

func plaidStr(m map[string]interface{}, key string) *string {
	if v, ok := m[key].(string); ok {
		return &v
	}
	return nil
}

func plaidFloat(m map[string]interface{}, key string) *float64 {
	if v, ok := m[key].(float64); ok {
		return &v
	}
	return nil
}

func plaidBool(m map[string]interface{}, key string) *bool {
	if v, ok := m[key].(bool); ok {
		return &v
	}
	return nil
}

func plaidJSON(m map[string]interface{}, key string) *string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}

// ── Plaid primary cardholder helper ──────────────────────────────────────

// IsPrimaryCardholder returns true when the account_owner is the main
// cardholder (ROBERT…SALADNA) or is empty/null — i.e. NOT a sub-card.
func IsPrimaryCardholder(owner string) bool {
	if owner == "" {
		return true
	}
	upper := strings.ToUpper(owner)
	return strings.Contains(upper, "ROBERT") && strings.Contains(upper, "SALADNA")
}

// POST /plaid/sandbox/connect-bank
func (h *PlaidHandler) SandboxConnectBank(c *gin.Context) {
	uid := c.Query("user_id")
	if uid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "user_id query parameter is required"})
		return
	}

	// Create sandbox public token
	sandboxResult, err := h.plaidRequest("/sandbox/public_token/create", map[string]interface{}{
		"institution_id":   "ins_109508",
		"initial_products": []string{"transactions", "auth"},
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	publicToken, _ := sandboxResult["public_token"].(string)

	// Exchange for access token
	exchangeResult, err := h.plaidRequest("/item/public_token/exchange", map[string]interface{}{
		"public_token": publicToken,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	accessToken, _ := exchangeResult["access_token"].(string)
	itemID, _ := exchangeResult["item_id"].(string)

	// Store in DB
	var user models.User
	if err := h.GormDB.First(&user, "id = ?", uid).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}

	// Save to plaid_items table
	plaidItem := models.PlaidItem{
		UserID:          uid,
		ItemID:          itemID,
		AccessToken:     accessToken,
		InstitutionName: "First Platypus Bank (Sandbox)",
	}
	if err := h.GormDB.Create(&plaidItem).Error; err != nil {
		log.Printf("ERROR saving sandbox plaid_item: %v", err)
	}

	// Also update users table for backwards compatibility
	h.GormDB.Model(&user).Update("plaid_access_token", accessToken)

	c.JSON(http.StatusOK, gin.H{
		"message":      "Sandbox bank account linked",
		"access_token": accessToken,
	})
}

// POST /plaid/import-csv — import bank CSV (b1Bank format)
func (h *PlaidHandler) ImportCSV(c *gin.Context) {
	file, _, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file is required"})
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "failed to read CSV headers"})
		return
	}

	colIdx := map[string]int{}
	for i, h := range headers {
		colIdx[strings.TrimSpace(h)] = i
	}

	var rows [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}

	imported := 0
	var importErrors []string
	accountID := "b1bank-3920"
	accountOwner := "MAX COMMERCIAL MM 3920"
	accountName := "MAX COMMERCIAL MM"
	accountType := "depository"
	source := "csv_import"

	for i, row := range rows {
		rowNum := i + 2

		postDateStr := csvGet(row, colIdx, "Post Date")
		description := csvGet(row, colIdx, "Description")
		debitStr := csvGet(row, colIdx, "Debit")
		creditStr := csvGet(row, colIdx, "Credit")
		statusStr := csvGet(row, colIdx, "Status")
		balanceStr := csvGet(row, colIdx, "Balance")
		acctNum := csvGet(row, colIdx, "Account Number")
		checkNum := csvGet(row, colIdx, "Check")

		if postDateStr == "" {
			importErrors = append(importErrors, fmt.Sprintf("row %d: missing Post Date", rowNum))
			continue
		}

		// Parse date in M/D/YYYY format
		parsedDate, err := time.Parse("1/2/2006", postDateStr)
		if err != nil {
			importErrors = append(importErrors, fmt.Sprintf("row %d: invalid date %q", rowNum, postDateStr))
			continue
		}
		dateStr := parsedDate.Format("2006-01-02")

		// Amount: Debit positive (money out), Credit negative (money in) — Plaid convention
		var amount *float64
		if debitStr != "" {
			amount = csvParseFloat(debitStr)
		} else if creditStr != "" {
			if v := csvParseFloat(creditStr); v != nil {
				neg := -(*v)
				amount = &neg
			}
		}

		pending := false
		if strings.EqualFold(statusStr, "Posted") {
			pending = false
		}

		currentBalance := csvParseFloat(balanceStr)

		plaidID := fmt.Sprintf("b1bank-%s-%d", acctNum, i)

		tx := models.Transaction{
			ID:             uuid.New().String(),
			PlaidID:        &plaidID,
			AccountID:      &accountID,
			Date:           &dateStr,
			Amount:         amount,
			Name:           csvNilIfEmpty(description),
			MerchantName:   csvNilIfEmpty(description),
			Pending:        &pending,
			CurrentBalance: currentBalance,
			AccountOwner:   &accountOwner,
			AccountName:    &accountName,
			AccountType:    &accountType,
			Source:         &source,
			CheckNumber:    csvNilIfEmpty(checkNum),
		}

		if err := h.GormDB.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "plaid_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"account_id", "date", "amount", "name", "merchant_name",
				"pending", "current_balance", "account_owner", "account_name",
				"account_type", "source", "check_number", "updated_at",
			}),
		}).Create(&tx).Error; err != nil {
			importErrors = append(importErrors, fmt.Sprintf("row %d: %v", rowNum, err))
			continue
		}

		// Insert daily balance snapshot if balance is present
		if currentBalance != nil {
			snap := models.DailyBalanceSnapshot{
				AccountID:      accountID,
				AccountName:    accountName,
				AccountType:    accountType,
				CurrentBalance: currentBalance,
				SnapshotDate:   dateStr,
				Source:         source,
			}
			h.GormDB.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "account_id"}, {Name: "snapshot_date"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"current_balance", "source",
				}),
			}).Create(&snap)
		}

		imported++
	}

	log.Printf("CSV import: imported %d rows, %d errors", imported, len(importErrors))

	c.JSON(http.StatusOK, gin.H{
		"imported": imported,
		"errors":   importErrors,
	})
}

