package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/xero"
)

type XeroSyncHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

// ── Value extraction helpers ────────────────────────────────────

var xeroDateRe = regexp.MustCompile(`/Date\((-?\d+)([+-]\d{4})?\)/`)

func parseXeroDate(v interface{}) *time.Time {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	if matches := xeroDateRe.FindStringSubmatch(s); len(matches) >= 2 {
		ms, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil
		}
		t := time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond)).UTC()
		return &t
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

func xeroStr(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func xeroFloat(m map[string]interface{}, key string) *float64 {
	if v, ok := m[key].(float64); ok {
		return &v
	}
	return nil
}

func xeroBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func xeroInt(m map[string]interface{}, key string) *int {
	if v, ok := m[key].(float64); ok {
		i := int(v)
		return &i
	}
	return nil
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func toJSON(v interface{}) models.JSONB {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return models.JSONB(b)
}

// ── Connection & Auth helpers ───────────────────────────────────

func (h *XeroSyncHandler) GetActiveConnection() (*models.XeroConnection, error) {
	var conn models.XeroConnection
	if err := h.GormDB.Order("updated_at DESC").First(&conn).Error; err != nil {
		return nil, fmt.Errorf("no xero connection found: %w", err)
	}

	// Refresh if expired or expiring within 60 seconds
	if time.Now().After(conn.ExpiresAt.Add(-60 * time.Second)) {
		tokenData, err := h.doRefreshToken(conn.RefreshToken)
		if err != nil {
			return nil, fmt.Errorf("token refresh failed: %w", err)
		}

		accessToken, _ := tokenData["access_token"].(string)
		newRefresh, _ := tokenData["refresh_token"].(string)
		expiresIn, _ := tokenData["expires_in"].(float64)

		if accessToken == "" {
			return nil, fmt.Errorf("no access_token in refresh response")
		}

		conn.AccessToken = accessToken
		conn.RefreshToken = newRefresh
		conn.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)

		if err := h.GormDB.Save(&conn).Error; err != nil {
			return nil, fmt.Errorf("failed to save refreshed token: %w", err)
		}
	}

	return &conn, nil
}

// refreshIfNeeded checks if the token expires within 5 minutes and refreshes if so.
// Called mid-sync to prevent token expiry during long paginated syncs.
func (h *XeroSyncHandler) refreshIfNeeded(conn *models.XeroConnection) error {
	if time.Now().After(conn.ExpiresAt.Add(-5 * time.Minute)) {
		tokenData, err := h.doRefreshToken(conn.RefreshToken)
		if err != nil {
			return fmt.Errorf("mid-sync token refresh failed: %w", err)
		}

		accessToken, _ := tokenData["access_token"].(string)
		newRefresh, _ := tokenData["refresh_token"].(string)
		expiresIn, _ := tokenData["expires_in"].(float64)

		if accessToken == "" {
			return fmt.Errorf("no access_token in mid-sync refresh response")
		}

		conn.AccessToken = accessToken
		conn.RefreshToken = newRefresh
		conn.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)

		if err := h.GormDB.Save(conn).Error; err != nil {
			return fmt.Errorf("failed to save refreshed token: %w", err)
		}
		log.Printf("Mid-sync token refresh successful, new expiry: %v", conn.ExpiresAt)
	}
	return nil
}

func (h *XeroSyncHandler) doRefreshToken(refreshTok string) (map[string]interface{}, error) {
	data := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshTok},
	}

	creds := h.Cfg.XeroClientID + ":" + h.Cfg.XeroClientSecret
	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))

	req, err := http.NewRequest("POST", "https://identity.xero.com/connect/token", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", authHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("xero token refresh failed (%d): %s", resp.StatusCode, string(body))
	}

	return result, nil
}

// ── Incremental sync helpers ───────────────────────────────────

// incrementalFilter returns a Xero where clause to only fetch records
// updated since the last successful sync. Returns "" for first-time syncs.
func (h *XeroSyncHandler) incrementalFilter(endpoint string) string {
	var state models.XeroSyncState
	if err := h.GormDB.Where("endpoint = ?", endpoint).First(&state).Error; err != nil {
		return ""
	}
	if state.LastSuccessfulAt == nil {
		return ""
	}
	t := state.LastSuccessfulAt.UTC()
	return fmt.Sprintf("UpdatedDateUTC>=DateTime(%d,%d,%d,%d,%d,%d)",
		t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second())
}

// updateSyncState records a successful sync completion for incremental tracking.
func (h *XeroSyncHandler) updateSyncState(endpoint string, recordsSynced int) {
	now := time.Now()
	state := models.XeroSyncState{
		Endpoint:           endpoint,
		LastSyncAt:         &now,
		LastSuccessfulAt:   &now,
		TotalRecordsSynced: recordsSynced,
	}
	h.GormDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "endpoint"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_sync_at", "last_successful_at", "total_records_synced", "updated_at"}),
	}).Create(&state)
}

// ── Xero API helpers ────────────────────────────────────────────

func (h *XeroSyncHandler) xeroGetRaw(rawURL, accessToken, tenantID string) ([]byte, error) {
	if !xero.GetRateLimiter().WaitForSlot() {
		return nil, fmt.Errorf("daily API limit approaching (4800 calls), sync paused")
	}

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Xero-Tenant-Id", tenantID)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("xero API error (%d): %s", resp.StatusCode, string(body))
	}

	return body, nil
}

func (h *XeroSyncHandler) xeroAPIGet(accessToken, tenantID, endpoint string) (map[string]interface{}, error) {
	body, err := h.xeroGetRaw("https://api.xero.com/api.xro/2.0/"+endpoint, accessToken, tenantID)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (h *XeroSyncHandler) xeroAssetsAPIGet(accessToken, tenantID, endpoint string) (map[string]interface{}, error) {
	body, err := h.xeroGetRaw("https://api.xero.com/assets.xro/1.0/"+endpoint, accessToken, tenantID)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (h *XeroSyncHandler) logSync(tenantID, endpoint, status string, count int, errMsg string) {
	entry := models.XeroSyncLog{
		TenantID:      tenantID,
		Endpoint:      endpoint,
		RecordsSynced: count,
		Status:        status,
		ErrorMessage:  errMsg,
	}
	if err := h.GormDB.Create(&entry).Error; err != nil {
		log.Printf("ERROR writing sync log: %v", err)
	}
}

// ── Sync: Contacts ──────────────────────────────────────────────

func (h *XeroSyncHandler) SyncContacts(conn *models.XeroConnection) error {
	whereClause := h.incrementalFilter("contacts")
	totalSynced := 0
	page := 1

	for {
		endpoint := fmt.Sprintf("Contacts?page=%d", page)
		if whereClause != "" {
			endpoint += "&where=" + url.QueryEscape(whereClause)
		}
		result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, endpoint)
		if err != nil {
			h.logSync(conn.TenantID, "contacts", "error", totalSynced, err.Error())
			return err
		}

		contacts, ok := result["Contacts"].([]interface{})
		if !ok || len(contacts) == 0 {
			break
		}

		for _, c := range contacts {
			cm, ok := c.(map[string]interface{})
			if !ok {
				continue
			}

			phone := ""
			if phones, ok := cm["Phones"].([]interface{}); ok && len(phones) > 0 {
				if p, ok := phones[0].(map[string]interface{}); ok {
					phone = xeroStr(p, "PhoneNumber")
				}
			}

			record := models.XeroContact{
				XeroID:         xeroStr(cm, "ContactID"),
				TenantID:       conn.TenantID,
				Name:           xeroStr(cm, "Name"),
				FirstName:      strPtr(xeroStr(cm, "FirstName")),
				LastName:       strPtr(xeroStr(cm, "LastName")),
				Email:          strPtr(xeroStr(cm, "EmailAddress")),
				Phone:          strPtr(phone),
				AccountNumber:  strPtr(xeroStr(cm, "AccountNumber")),
				TaxNumber:      strPtr(xeroStr(cm, "TaxNumber")),
				IsSupplier:     xeroBool(cm, "IsSupplier"),
				IsCustomer:     xeroBool(cm, "IsCustomer"),
				ContactStatus:  strPtr(xeroStr(cm, "ContactStatus")),
				UpdatedDateUTC: parseXeroDate(cm["UpdatedDateUTC"]),
			}

			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "xero_id"}},
				UpdateAll: true,
			}).Create(&record)
			totalSynced++
		}

		if len(contacts) < 100 {
			break
		}
		page++
	}

	h.updateSyncState("contacts", totalSynced)
	h.logSync(conn.TenantID, "contacts", "success", totalSynced, "")
	return nil
}

// ── Sync: BankTransactions ──────────────────────────────────────

func (h *XeroSyncHandler) SyncBankTransactions(conn *models.XeroConnection) error {
	whereClause := h.incrementalFilter("bank-transactions")
	totalSynced := 0
	page := 1

	for {
		endpoint := fmt.Sprintf("BankTransactions?page=%d", page)
		if whereClause != "" {
			endpoint += "&where=" + url.QueryEscape(whereClause)
		}
		result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, endpoint)
		if err != nil {
			h.logSync(conn.TenantID, "bank-transactions", "error", totalSynced, err.Error())
			return err
		}

		txns, ok := result["BankTransactions"].([]interface{})
		if !ok || len(txns) == 0 {
			break
		}

		for _, t := range txns {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}

			contactID, contactName := "", ""
			if contact, ok := tm["Contact"].(map[string]interface{}); ok {
				contactID = xeroStr(contact, "ContactID")
				contactName = xeroStr(contact, "Name")
			}

			bankAcctID, bankAcctName := "", ""
			if ba, ok := tm["BankAccount"].(map[string]interface{}); ok {
				bankAcctID = xeroStr(ba, "AccountID")
				bankAcctName = xeroStr(ba, "Name")
			}

			record := models.XeroBankTransaction{
				XeroID:          xeroStr(tm, "BankTransactionID"),
				TenantID:        conn.TenantID,
				Type:            strPtr(xeroStr(tm, "Type")),
				ContactID:       strPtr(contactID),
				ContactName:     strPtr(contactName),
				BankAccountID:   strPtr(bankAcctID),
				BankAccountName: strPtr(bankAcctName),
				Date:            parseXeroDate(tm["Date"]),
				Reference:       strPtr(xeroStr(tm, "Reference")),
				Status:          strPtr(xeroStr(tm, "Status")),
				SubTotal:        xeroFloat(tm, "SubTotal"),
				TotalTax:        xeroFloat(tm, "TotalTax"),
				Total:           xeroFloat(tm, "Total"),
				IsReconciled:    xeroBool(tm, "IsReconciled"),
				LineItems:       toJSON(tm["LineItems"]),
				UpdatedDateUTC:  parseXeroDate(tm["UpdatedDateUTC"]),
			}

			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "xero_id"}},
				UpdateAll: true,
			}).Create(&record)
			totalSynced++
		}

		if len(txns) < 100 {
			break
		}
		page++
		if page%25 == 0 {
			if err := h.refreshIfNeeded(conn); err != nil {
				log.Printf("WARN: bank-transactions mid-sync refresh failed: %v", err)
			}
		}
	}

	h.updateSyncState("bank-transactions", totalSynced)
	h.logSync(conn.TenantID, "bank-transactions", "success", totalSynced, "")
	return nil
}

// ── Sync: Invoices ──────────────────────────────────────────────

func (h *XeroSyncHandler) SyncInvoices(conn *models.XeroConnection) error {
	whereClause := h.incrementalFilter("invoices")
	totalSynced := 0
	page := 1

	for {
		endpoint := fmt.Sprintf("Invoices?page=%d", page)
		if whereClause != "" {
			endpoint += "&where=" + url.QueryEscape(whereClause)
		}
		result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, endpoint)
		if err != nil {
			h.logSync(conn.TenantID, "invoices", "error", totalSynced, err.Error())
			return err
		}

		invoices, ok := result["Invoices"].([]interface{})
		if !ok || len(invoices) == 0 {
			break
		}

		for _, inv := range invoices {
			im, ok := inv.(map[string]interface{})
			if !ok {
				continue
			}

			contactID, contactName := "", ""
			if contact, ok := im["Contact"].(map[string]interface{}); ok {
				contactID = xeroStr(contact, "ContactID")
				contactName = xeroStr(contact, "Name")
			}

			record := models.XeroInvoice{
				XeroID:         xeroStr(im, "InvoiceID"),
				TenantID:       conn.TenantID,
				Type:           strPtr(xeroStr(im, "Type")),
				ContactID:      strPtr(contactID),
				ContactName:    strPtr(contactName),
				InvoiceNumber:  strPtr(xeroStr(im, "InvoiceNumber")),
				Reference:      strPtr(xeroStr(im, "Reference")),
				Date:           parseXeroDate(im["Date"]),
				DueDate:        parseXeroDate(im["DueDate"]),
				Status:         strPtr(xeroStr(im, "Status")),
				SubTotal:       xeroFloat(im, "SubTotal"),
				TotalTax:       xeroFloat(im, "TotalTax"),
				Total:          xeroFloat(im, "Total"),
				AmountDue:      xeroFloat(im, "AmountDue"),
				AmountPaid:     xeroFloat(im, "AmountPaid"),
				AmountCredited: xeroFloat(im, "AmountCredited"),
				LineItems:      toJSON(im["LineItems"]),
				UpdatedDateUTC: parseXeroDate(im["UpdatedDateUTC"]),
			}

			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "xero_id"}},
				UpdateAll: true,
			}).Create(&record)
			totalSynced++
		}

		if len(invoices) < 100 {
			break
		}
		page++
		if page%25 == 0 {
			if err := h.refreshIfNeeded(conn); err != nil {
				log.Printf("WARN: invoices mid-sync refresh failed: %v", err)
			}
		}
	}

	h.updateSyncState("invoices", totalSynced)
	h.logSync(conn.TenantID, "invoices", "success", totalSynced, "")
	return nil
}

// ── Sync: Payments ──────────────────────────────────────────────

func (h *XeroSyncHandler) SyncPayments(conn *models.XeroConnection) error {
	whereClause := h.incrementalFilter("payments")
	totalSynced := 0
	page := 1

	for {
		endpoint := fmt.Sprintf("Payments?page=%d", page)
		if whereClause != "" {
			endpoint += "&where=" + url.QueryEscape(whereClause)
		}
		result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, endpoint)
		if err != nil {
			h.logSync(conn.TenantID, "payments", "error", totalSynced, err.Error())
			return err
		}

		payments, ok := result["Payments"].([]interface{})
		if !ok || len(payments) == 0 {
			break
		}

		for _, p := range payments {
			pm, ok := p.(map[string]interface{})
			if !ok {
				continue
			}

			invoiceID := ""
			if inv, ok := pm["Invoice"].(map[string]interface{}); ok {
				invoiceID = xeroStr(inv, "InvoiceID")
			}

			accountID := ""
			if acct, ok := pm["Account"].(map[string]interface{}); ok {
				accountID = xeroStr(acct, "AccountID")
			}

			record := models.XeroPayment{
				XeroID:         xeroStr(pm, "PaymentID"),
				TenantID:       conn.TenantID,
				InvoiceID:      strPtr(invoiceID),
				AccountID:      strPtr(accountID),
				Date:           parseXeroDate(pm["Date"]),
				Amount:         xeroFloat(pm, "Amount"),
				Reference:      strPtr(xeroStr(pm, "Reference")),
				Status:         strPtr(xeroStr(pm, "Status")),
				PaymentType:    strPtr(xeroStr(pm, "PaymentType")),
				UpdatedDateUTC: parseXeroDate(pm["UpdatedDateUTC"]),
			}

			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "xero_id"}},
				UpdateAll: true,
			}).Create(&record)
			totalSynced++
		}

		if len(payments) < 100 {
			break
		}
		page++
	}

	h.updateSyncState("payments", totalSynced)
	h.logSync(conn.TenantID, "payments", "success", totalSynced, "")
	return nil
}

// ── Sync: ManualJournals ────────────────────────────────────────

func (h *XeroSyncHandler) SyncManualJournals(conn *models.XeroConnection) error {
	whereClause := h.incrementalFilter("journals")
	totalSynced := 0
	page := 1

	for {
		endpoint := fmt.Sprintf("ManualJournals?page=%d", page)
		if whereClause != "" {
			endpoint += "&where=" + url.QueryEscape(whereClause)
		}
		result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, endpoint)
		if err != nil {
			h.logSync(conn.TenantID, "journals", "error", totalSynced, err.Error())
			return err
		}

		journals, ok := result["ManualJournals"].([]interface{})
		if !ok || len(journals) == 0 {
			break
		}

		for _, j := range journals {
			jm, ok := j.(map[string]interface{})
			if !ok {
				continue
			}

			record := models.XeroJournal{
				XeroID:         xeroStr(jm, "ManualJournalID"),
				TenantID:       conn.TenantID,
				JournalDate:    parseXeroDate(jm["Date"]),
				JournalNumber:  xeroInt(jm, "JournalNumber"),
				Reference:      strPtr(xeroStr(jm, "Reference")),
				JournalLines:   toJSON(jm["JournalLines"]),
				CreatedDateUTC: parseXeroDate(jm["CreatedDateUTC"]),
			}

			h.GormDB.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "xero_id"}},
				UpdateAll: true,
			}).Create(&record)
			totalSynced++
		}

		if len(journals) < 100 {
			break
		}
		page++
	}

	h.updateSyncState("journals", totalSynced)
	h.logSync(conn.TenantID, "journals", "success", totalSynced, "")
	return nil
}

// ── Sync: TrackingCategories ────────────────────────────────────

func (h *XeroSyncHandler) SyncTrackingCategories(conn *models.XeroConnection) error {
	result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, "TrackingCategories")
	if err != nil {
		h.logSync(conn.TenantID, "tracking-categories", "error", 0, err.Error())
		return err
	}


	categories, ok := result["TrackingCategories"].([]interface{})
	if !ok {
		h.logSync(conn.TenantID, "tracking-categories", "success", 0, "")
		return nil
	}

	totalSynced := 0
	for _, c := range categories {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		record := models.XeroTrackingCategory{
			XeroID:   xeroStr(cm, "TrackingCategoryID"),
			TenantID: conn.TenantID,
			Name:     xeroStr(cm, "Name"),
			Status:   strPtr(xeroStr(cm, "Status")),
			Options:  toJSON(cm["Options"]),
		}

		h.GormDB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "xero_id"}},
			UpdateAll: true,
		}).Create(&record)
		totalSynced++
	}

	h.updateSyncState("tracking-categories", totalSynced)
	h.logSync(conn.TenantID, "tracking-categories", "success", totalSynced, "")
	return nil
}

// ── Sync: Accounts (Chart of Accounts) ─────────────────────────

func (h *XeroSyncHandler) SyncAccounts(conn *models.XeroConnection) error {
	log.Printf("SyncAccounts: starting, tenant=%s, token_expires=%v", conn.TenantID, conn.ExpiresAt)

	log.Printf("SyncAccounts: calling Xero GET /Accounts API")
	result, err := h.xeroAPIGet(conn.AccessToken, conn.TenantID, "Accounts")
	if err != nil {
		log.Printf("SyncAccounts: ERROR from Xero API: %v", err)
		h.logSync(conn.TenantID, "accounts", "error", 0, err.Error())
		return err
	}

	// Log the top-level keys to see what Xero returned
	keys := make([]string, 0, len(result))
	for k := range result {
		keys = append(keys, k)
	}
	log.Printf("SyncAccounts: response keys: %v", keys)

	accounts, ok := result["Accounts"].([]interface{})
	if !ok {
		log.Printf("SyncAccounts: WARNING 'Accounts' key missing or not array, got type %T", result["Accounts"])
		h.logSync(conn.TenantID, "accounts", "success", 0, "no Accounts array in response")
		return nil
	}

	log.Printf("SyncAccounts: got %d accounts from Xero", len(accounts))

	totalSynced := 0
	for _, a := range accounts {
		am, ok := a.(map[string]interface{})
		if !ok {
			continue
		}

		record := models.XeroAccount{
			XeroID:              xeroStr(am, "AccountID"),
			TenantID:            conn.TenantID,
			Code:                xeroStr(am, "Code"),
			Name:                xeroStr(am, "Name"),
			Type:                xeroStr(am, "Type"),
			Class:               xeroStr(am, "Class"),
			Status:              xeroStr(am, "Status"),
			Description:         xeroStr(am, "Description"),
			EnablePayments:      xeroBool(am, "EnablePaymentsToAccount"),
			ShowInExpenseClaims: xeroBool(am, "ShowInExpenseClaims"),
			BankAccountNumber:   xeroStr(am, "BankAccountNumber"),
			CurrencyCode:        xeroStr(am, "CurrencyCode"),
			TaxType:             xeroStr(am, "TaxType"),
		}

		if err := h.GormDB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "xero_id"}},
			UpdateAll: true,
		}).Create(&record).Error; err != nil {
			log.Printf("SyncAccounts: ERROR inserting account %s (%s): %v", record.Code, record.Name, err)
			continue
		}
		totalSynced++
	}

	h.updateSyncState("accounts", totalSynced)
	h.logSync(conn.TenantID, "accounts", "success", totalSynced, "")
	log.Printf("SyncAccounts: done, inserted %d accounts", totalSynced)
	return nil
}

// ── Sync: Assets ────────────────────────────────────────────────

func (h *XeroSyncHandler) SyncAssets(conn *models.XeroConnection) error {
	totalSynced := 0

	for _, status := range []string{"REGISTERED", "DRAFT", "DISPOSED"} {
		page := 1
		for {
			endpoint := fmt.Sprintf("Assets?status=%s&page=%d", status, page)
			result, err := h.xeroAssetsAPIGet(conn.AccessToken, conn.TenantID, endpoint)
			if err != nil {
				log.Printf("WARN: failed to sync assets (status=%s): %v", status, err)
				break
			}
		

			items, ok := result["items"].([]interface{})
			if !ok || len(items) == 0 {
				break
			}

			for _, item := range items {
				am, ok := item.(map[string]interface{})
				if !ok {
					continue
				}

				var assetTypeID, assetTypeName string
				if at, ok := am["assetType"].(map[string]interface{}); ok {
					assetTypeID = xeroStr(at, "assetTypeId")
					assetTypeName = xeroStr(at, "assetTypeName")
				}

				var deprecMethod, avgMethod string
				var deprecRate *float64
				var effectiveLife *int
				var costLimit, residualValue *float64
				if bds, ok := am["bookDepreciationSetting"].(map[string]interface{}); ok {
					deprecMethod = xeroStr(bds, "depreciationMethod")
					avgMethod = xeroStr(bds, "averagingMethod")
					deprecRate = xeroFloat(bds, "depreciationRate")
					effectiveLife = xeroInt(bds, "effectiveLifeYears")
					costLimit = xeroFloat(bds, "costLimit")
					residualValue = xeroFloat(bds, "residualValue")
				}

				var bookValue, currAccumDeprec, priorAccumDeprec, currDeprec *float64
				if bdd, ok := am["bookDepreciationDetail"].(map[string]interface{}); ok {
					bookValue = xeroFloat(bdd, "currentBookValue")
					currAccumDeprec = xeroFloat(bdd, "currentAccumulatedDepreciationAmount")
					priorAccumDeprec = xeroFloat(bdd, "priorAccumulatedDepreciationAmount")
					currDeprec = xeroFloat(bdd, "currentDepreciationAmount")
				}

				record := models.XeroAsset{
					XeroID:                   xeroStr(am, "assetId"),
					TenantID:                 conn.TenantID,
					AssetName:                xeroStr(am, "assetName"),
					AssetNumber:              strPtr(xeroStr(am, "assetNumber")),
					AssetTypeID:              strPtr(assetTypeID),
					AssetTypeName:            strPtr(assetTypeName),
					Status:                   strPtr(xeroStr(am, "assetStatus")),
					PurchaseDate:             parseXeroDate(am["purchaseDate"]),
					PurchasePrice:            xeroFloat(am, "purchasePrice"),
					DisposalDate:             parseXeroDate(am["disposalDate"]),
					DisposalPrice:            xeroFloat(am, "disposalPrice"),
					DepreciationMethod:       strPtr(deprecMethod),
					AveragingMethod:          strPtr(avgMethod),
					DepreciationRate:         deprecRate,
					EffectiveLifeYears:       effectiveLife,
					CostLimit:                costLimit,
					ResidualValue:            residualValue,
					BookValue:                bookValue,
					CurrentAccumDepreciation: currAccumDeprec,
					PriorAccumDepreciation:   priorAccumDeprec,
					CurrentDepreciation:      currDeprec,
				}

				h.GormDB.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "xero_id"}},
					UpdateAll: true,
				}).Create(&record)
				totalSynced++
			}

			// Check pagination
			pagination, _ := result["pagination"].(map[string]interface{})
			pageCount := 0
			if pc, ok := pagination["pageCount"].(float64); ok {
				pageCount = int(pc)
			}
			if page >= pageCount {
				break
			}
			page++
		}
	}

	h.logSync(conn.TenantID, "assets", "success", totalSynced, "")
	return nil
}

// ── Sync: AssetTypes ────────────────────────────────────────────

func (h *XeroSyncHandler) SyncAssetTypes(conn *models.XeroConnection) error {
	// AssetTypes returns a JSON array directly
	body, err := h.xeroGetRaw("https://api.xero.com/assets.xro/1.0/AssetTypes", conn.AccessToken, conn.TenantID)
	if err != nil {
		h.logSync(conn.TenantID, "asset-types", "error", 0, err.Error())
		return err
	}


	var assetTypes []interface{}
	if err := json.Unmarshal(body, &assetTypes); err != nil {
		h.logSync(conn.TenantID, "asset-types", "error", 0, "failed to parse response: "+err.Error())
		return err
	}

	totalSynced := 0
	for _, at := range assetTypes {
		atm, ok := at.(map[string]interface{})
		if !ok {
			continue
		}

		var deprecMethod, avgMethod string
		var deprecRate *float64
		var effectiveLife *int
		if bds, ok := atm["bookDepreciationSetting"].(map[string]interface{}); ok {
			deprecMethod = xeroStr(bds, "depreciationMethod")
			avgMethod = xeroStr(bds, "averagingMethod")
			deprecRate = xeroFloat(bds, "depreciationRate")
			effectiveLife = xeroInt(bds, "effectiveLifeYears")
		}

		record := models.XeroAssetType{
			XeroID:                           xeroStr(atm, "assetTypeId"),
			TenantID:                         conn.TenantID,
			AssetTypeName:                    xeroStr(atm, "assetTypeName"),
			FixedAssetAccountID:              strPtr(xeroStr(atm, "fixedAssetAccountId")),
			DepreciationExpenseAccountID:     strPtr(xeroStr(atm, "depreciationExpenseAccountId")),
			AccumulatedDepreciationAccountID: strPtr(xeroStr(atm, "accumulatedDepreciationAccountId")),
			DepreciationMethod:               strPtr(deprecMethod),
			AveragingMethod:                  strPtr(avgMethod),
			DepreciationRate:                 deprecRate,
			EffectiveLifeYears:               effectiveLife,
		}

		h.GormDB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "xero_id"}},
			UpdateAll: true,
		}).Create(&record)
		totalSynced++
	}

	h.logSync(conn.TenantID, "asset-types", "success", totalSynced, "")
	return nil
}

// ── SyncAll ─────────────────────────────────────────────────────

func (h *XeroSyncHandler) SyncAll(conn *models.XeroConnection, fullResync bool) {
	if fullResync {
		h.GormDB.Where("1 = 1").Delete(&models.XeroSyncState{})
		log.Println("Full resync: cleared xero_sync_state for complete re-fetch")
	}

	type syncMethod struct {
		name string
		fn   func(*models.XeroConnection) error
	}

	methods := []syncMethod{
		{"contacts", h.SyncContacts},
		{"bank-transactions", h.SyncBankTransactions},
		{"invoices", h.SyncInvoices},
		{"payments", h.SyncPayments},
		{"journals", h.SyncManualJournals},
		{"tracking-categories", h.SyncTrackingCategories},
		{"accounts", h.SyncAccounts},
		{"assets", h.SyncAssets},
		{"asset-types", h.SyncAssetTypes},
	}

	for i, m := range methods {
		log.Printf("SyncAll: starting %s (fullResync=%v, apiCallsToday=%d)",
			m.name, fullResync, xero.GetRateLimiter().CallsToday())
		if err := m.fn(conn); err != nil {
			log.Printf("Xero sync %s failed: %v", m.name, err)
		}
		if i < len(methods)-1 {
			time.Sleep(2 * time.Second)
		}
	}
}

// ── Report (cached) ─────────────────────────────────────────────

func (h *XeroSyncHandler) GetCachedOrFetchReport(tenantID, accessToken, reportType, queryParams string) ([]byte, error) {
	// Check cache (within last hour)
	var cached models.XeroReportCache
	if err := h.GormDB.Where(
		"tenant_id = ? AND report_type = ? AND params = ? AND cached_at > ?",
		tenantID, reportType, queryParams, time.Now().Add(-1*time.Hour),
	).First(&cached).Error; err == nil {
		return cached.ReportData, nil
	}

	// Fetch from Xero
	endpoint := "Reports/" + reportType
	if queryParams != "" {
		endpoint += "?" + queryParams
	}
	result, err := h.xeroAPIGet(accessToken, tenantID, endpoint)
	if err != nil {
		return nil, err
	}

	resultJSON, _ := json.Marshal(result)

	// Cache result (upsert by tenant_id + report_type + params)
	cache := models.XeroReportCache{
		TenantID:   tenantID,
		ReportType: reportType,
		Params:     queryParams,
		ReportData: models.JSONB(resultJSON),
		CachedAt:   time.Now(),
	}
	h.GormDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "report_type"}, {Name: "params"}},
		DoUpdates: clause.AssignmentColumns([]string{"report_data", "cached_at"}),
	}).Create(&cache)

	return resultJSON, nil
}

// ── HTTP Handlers ───────────────────────────────────────────────

// POST /xero/sync
func (h *XeroSyncHandler) TriggerSyncAll(c *gin.Context) {
	conn, err := h.GetActiveConnection()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	go h.SyncAll(conn, false)
	c.JSON(http.StatusOK, gin.H{"status": "started"})
}

// POST /xero/sync/:endpoint
func (h *XeroSyncHandler) TriggerSyncEndpoint(c *gin.Context) {
	endpoint := c.Param("endpoint")
	conn, err := h.GetActiveConnection()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	syncMap := map[string]func(*models.XeroConnection) error{
		"contacts":            h.SyncContacts,
		"bank-transactions":   h.SyncBankTransactions,
		"invoices":            h.SyncInvoices,
		"payments":            h.SyncPayments,
		"journals":            h.SyncManualJournals,
		"tracking-categories": h.SyncTrackingCategories,
		"accounts":            h.SyncAccounts,
		"assets":              h.SyncAssets,
		"asset-types":         h.SyncAssetTypes,
	}

	fn, ok := syncMap[endpoint]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "unknown endpoint: " + endpoint})
		return
	}

	go fn(conn)
	c.JSON(http.StatusOK, gin.H{"status": "started", "endpoint": endpoint})
}

// GET /xero/sync-status
func (h *XeroSyncHandler) GetSyncStatus(c *gin.Context) {
	var logs []models.XeroSyncLog
	h.GormDB.Raw(`
		SELECT DISTINCT ON (endpoint) *
		FROM xero_sync_log
		ORDER BY endpoint, created_at DESC
	`).Scan(&logs)
	c.JSON(http.StatusOK, logs)
}
