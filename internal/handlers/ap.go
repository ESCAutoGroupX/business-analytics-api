package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type APHandler struct {
	GormDB *gorm.DB
}

func (h *APHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// AutoMigrate creates the AP tables if they do not exist.
func (h *APHandler) AutoMigrate() {
	db := h.sqlDB()
	if db == nil {
		return
	}

	migrations := []string{
		`CREATE TABLE IF NOT EXISTS ap_entries (
            id SERIAL PRIMARY KEY,
            vendor_id VARCHAR,
            vendor_name VARCHAR,
            statement_document_id INTEGER,
            period_start DATE,
            period_end DATE,
            total_amount NUMERIC(12,2),
            matched_amount NUMERIC(12,2) DEFAULT 0,
            unmatched_amount NUMERIC(12,2) DEFAULT 0,
            invoice_count INTEGER DEFAULT 0,
            matched_invoice_count INTEGER DEFAULT 0,
            status VARCHAR DEFAULT 'open',
            authorized_by VARCHAR,
            authorized_at TIMESTAMP,
            paid_at TIMESTAMP,
            payment_method VARCHAR,
            payment_reference VARCHAR,
            bank_account VARCHAR DEFAULT 'Regions Advantage',
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )`,
		`CREATE TABLE IF NOT EXISTS statement_line_items (
            id SERIAL PRIMARY KEY,
            statement_document_id INTEGER,
            invoice_number VARCHAR,
            invoice_date DATE,
            amount NUMERIC(12,2),
            description VARCHAR,
            linked_document_id INTEGER,
            linked_transaction_id VARCHAR,
            status VARCHAR DEFAULT 'unmatched',
            created_at TIMESTAMP DEFAULT NOW()
        )`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			log.Printf("AP migration error: %v", err)
		}
	}
	log.Printf("AP tables migration complete")
}

// ListEntries handles GET /ap — list AP entries with optional filters.
func (h *APHandler) ListEntries(c *gin.Context) {
	db := h.sqlDB()

	query := `SELECT e.id, e.vendor_id, e.vendor_name, e.statement_document_id,
        e.period_start, e.period_end, e.total_amount, e.matched_amount, e.unmatched_amount,
        e.invoice_count, e.matched_invoice_count, e.status,
        e.authorized_by, e.authorized_at, e.paid_at,
        e.payment_method, e.payment_reference, e.bank_account,
        e.notes, e.created_at, e.updated_at
        FROM ap_entries e WHERE 1=1`

	var args []interface{}
	argIdx := 1

	if status := c.Query("status"); status != "" {
		query += fmt.Sprintf(" AND e.status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if vendorID := c.Query("vendor_id"); vendorID != "" {
		query += fmt.Sprintf(" AND e.vendor_id = $%d", argIdx)
		args = append(args, vendorID)
		argIdx++
	}

	query += " ORDER BY e.created_at DESC"

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query AP entries"})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var id, invoiceCount, matchedInvoiceCount int
		var vendorID, vendorName, status, bankAccount sql.NullString
		var statementDocID sql.NullInt64
		var periodStart, periodEnd sql.NullTime
		var totalAmount, matchedAmount, unmatchedAmount sql.NullFloat64
		var authorizedBy, paymentMethod, paymentRef, notes sql.NullString
		var authorizedAt, paidAt sql.NullTime
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &vendorID, &vendorName, &statementDocID,
			&periodStart, &periodEnd, &totalAmount, &matchedAmount, &unmatchedAmount,
			&invoiceCount, &matchedInvoiceCount, &status,
			&authorizedBy, &authorizedAt, &paidAt,
			&paymentMethod, &paymentRef, &bankAccount,
			&notes, &createdAt, &updatedAt); err != nil {
			log.Printf("AP scan error: %v", err)
			continue
		}

		entry := gin.H{
			"id":                    id,
			"vendor_id":             vendorID.String,
			"vendor_name":           vendorName.String,
			"statement_document_id": nil,
			"period_start":          nil,
			"period_end":            nil,
			"total_amount":          totalAmount.Float64,
			"matched_amount":        matchedAmount.Float64,
			"unmatched_amount":      unmatchedAmount.Float64,
			"invoice_count":         invoiceCount,
			"matched_invoice_count": matchedInvoiceCount,
			"status":                status.String,
			"authorized_by":         authorizedBy.String,
			"authorized_at":         nil,
			"paid_at":               nil,
			"payment_method":        paymentMethod.String,
			"payment_reference":     paymentRef.String,
			"bank_account":          bankAccount.String,
			"notes":                 notes.String,
			"created_at":            createdAt,
			"updated_at":            updatedAt,
		}
		if statementDocID.Valid {
			entry["statement_document_id"] = statementDocID.Int64
		}
		if periodStart.Valid {
			entry["period_start"] = periodStart.Time.Format("2006-01-02")
		}
		if periodEnd.Valid {
			entry["period_end"] = periodEnd.Time.Format("2006-01-02")
		}
		if authorizedAt.Valid {
			entry["authorized_at"] = authorizedAt.Time
		}
		if paidAt.Valid {
			entry["paid_at"] = paidAt.Time
		}

		results = append(results, entry)
	}
	if results == nil {
		results = []gin.H{}
	}
	c.JSON(http.StatusOK, results)
}

// GetEntry handles GET /ap/:id — single AP entry with line items.
func (h *APHandler) GetEntry(c *gin.Context) {
	id := c.Param("id")
	db := h.sqlDB()

	// Get entry
	var entryID int
	var vendorID, vendorName, status, bankAccount sql.NullString
	var statementDocID sql.NullInt64
	var periodStart, periodEnd sql.NullTime
	var totalAmount, matchedAmount, unmatchedAmount sql.NullFloat64
	var invoiceCount, matchedInvoiceCount int
	var authorizedBy, paymentMethod, paymentRef, notes sql.NullString
	var authorizedAt, paidAt sql.NullTime
	var createdAt, updatedAt time.Time

	err := db.QueryRow(`SELECT id, vendor_id, vendor_name, statement_document_id,
        period_start, period_end, total_amount, matched_amount, unmatched_amount,
        invoice_count, matched_invoice_count, status,
        authorized_by, authorized_at, paid_at,
        payment_method, payment_reference, bank_account,
        notes, created_at, updated_at
        FROM ap_entries WHERE id = $1`, id).Scan(
		&entryID, &vendorID, &vendorName, &statementDocID,
		&periodStart, &periodEnd, &totalAmount, &matchedAmount, &unmatchedAmount,
		&invoiceCount, &matchedInvoiceCount, &status,
		&authorizedBy, &authorizedAt, &paidAt,
		&paymentMethod, &paymentRef, &bankAccount,
		&notes, &createdAt, &updatedAt)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "AP entry not found"})
		return
	}

	entry := gin.H{
		"id":                    entryID,
		"vendor_id":             vendorID.String,
		"vendor_name":           vendorName.String,
		"total_amount":          totalAmount.Float64,
		"matched_amount":        matchedAmount.Float64,
		"unmatched_amount":      unmatchedAmount.Float64,
		"invoice_count":         invoiceCount,
		"matched_invoice_count": matchedInvoiceCount,
		"status":                status.String,
		"bank_account":          bankAccount.String,
		"authorized_by":         authorizedBy.String,
		"payment_method":        paymentMethod.String,
		"payment_reference":     paymentRef.String,
		"notes":                 notes.String,
		"created_at":            createdAt,
		"updated_at":            updatedAt,
		"statement_document_id": nil,
		"period_start":          nil,
		"period_end":            nil,
		"authorized_at":         nil,
		"paid_at":               nil,
	}
	if statementDocID.Valid {
		entry["statement_document_id"] = statementDocID.Int64
	}
	if periodStart.Valid {
		entry["period_start"] = periodStart.Time.Format("2006-01-02")
	}
	if periodEnd.Valid {
		entry["period_end"] = periodEnd.Time.Format("2006-01-02")
	}
	if authorizedAt.Valid {
		entry["authorized_at"] = authorizedAt.Time
	}
	if paidAt.Valid {
		entry["paid_at"] = paidAt.Time
	}

	// Get statement line items
	lineRows, err := db.Query(`SELECT id, invoice_number, invoice_date, amount, description,
        linked_document_id, linked_transaction_id, status, created_at
        FROM statement_line_items WHERE statement_document_id = $1
        ORDER BY invoice_date, id`, statementDocID)

	var lines []gin.H
	if err == nil {
		defer lineRows.Close()
		for lineRows.Next() {
			var lineID int
			var invNum, desc, lineStatus sql.NullString
			var invDate sql.NullTime
			var amt sql.NullFloat64
			var linkedDocID sql.NullInt64
			var linkedTxID sql.NullString
			var lineCreatedAt time.Time

			if err := lineRows.Scan(&lineID, &invNum, &invDate, &amt, &desc,
				&linkedDocID, &linkedTxID, &lineStatus, &lineCreatedAt); err != nil {
				continue
			}

			line := gin.H{
				"id":                      lineID,
				"invoice_number":          invNum.String,
				"invoice_date":            nil,
				"amount":                  amt.Float64,
				"description":             desc.String,
				"linked_document_id":      nil,
				"linked_transaction_id":   linkedTxID.String,
				"status":                  lineStatus.String,
				"created_at":              lineCreatedAt,
			}
			if invDate.Valid {
				line["invoice_date"] = invDate.Time.Format("2006-01-02")
			}
			if linkedDocID.Valid {
				line["linked_document_id"] = linkedDocID.Int64
			}
			lines = append(lines, line)
		}
	}
	if lines == nil {
		lines = []gin.H{}
	}
	entry["line_items"] = lines

	c.JSON(http.StatusOK, entry)
}

// Authorize handles POST /ap/:id/authorize.
func (h *APHandler) Authorize(c *gin.Context) {
	id := c.Param("id")
	db := h.sqlDB()

	var req struct {
		AuthorizedBy string `json:"authorized_by"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "authorized_by is required"})
		return
	}

	// Check current status
	var currentStatus string
	err := db.QueryRow("SELECT status FROM ap_entries WHERE id = $1", id).Scan(&currentStatus)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "AP entry not found"})
		return
	}
	if currentStatus != "ready_to_pay" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "can only authorize entries with status ready_to_pay"})
		return
	}

	_, err = db.Exec(`UPDATE ap_entries SET status = 'authorized', authorized_by = $1, authorized_at = NOW(), updated_at = NOW() WHERE id = $2`, req.AuthorizedBy, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to authorize"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "authorized", "id": id})
}

// MarkPaid handles POST /ap/:id/mark-paid.
func (h *APHandler) MarkPaid(c *gin.Context) {
	id := c.Param("id")
	db := h.sqlDB()

	var req struct {
		PaymentMethod    string `json:"payment_method"`
		PaymentReference string `json:"payment_reference"`
		PaidAt           string `json:"paid_at"`
		BankAccount      string `json:"bank_account"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid request"})
		return
	}

	paidAt := time.Now()
	if req.PaidAt != "" {
		if t, err := time.Parse("2006-01-02", req.PaidAt); err == nil {
			paidAt = t
		}
	}

	bankAccount := "Regions Advantage"
	if req.BankAccount != "" {
		bankAccount = req.BankAccount
	}

	_, err := db.Exec(`UPDATE ap_entries SET status = 'paid', payment_method = $1, payment_reference = $2, paid_at = $3, bank_account = $4, updated_at = NOW() WHERE id = $5`,
		req.PaymentMethod, req.PaymentReference, paidAt, bankAccount, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to mark paid"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "marked as paid", "id": id})
}

// AgingReport handles GET /ap/aging — aging report.
func (h *APHandler) AgingReport(c *gin.Context) {
	db := h.sqlDB()

	rows, err := db.Query(`SELECT
        e.vendor_name,
        SUM(CASE WHEN e.period_end >= CURRENT_DATE - INTERVAL '30 days' OR e.period_end IS NULL THEN e.unmatched_amount ELSE 0 END) as current_amt,
        SUM(CASE WHEN e.period_end < CURRENT_DATE - INTERVAL '30 days' AND e.period_end >= CURRENT_DATE - INTERVAL '60 days' THEN e.unmatched_amount ELSE 0 END) as days_1_30,
        SUM(CASE WHEN e.period_end < CURRENT_DATE - INTERVAL '60 days' AND e.period_end >= CURRENT_DATE - INTERVAL '90 days' THEN e.unmatched_amount ELSE 0 END) as days_31_60,
        SUM(CASE WHEN e.period_end < CURRENT_DATE - INTERVAL '90 days' AND e.period_end >= CURRENT_DATE - INTERVAL '120 days' THEN e.unmatched_amount ELSE 0 END) as days_61_90,
        SUM(CASE WHEN e.period_end < CURRENT_DATE - INTERVAL '120 days' THEN e.unmatched_amount ELSE 0 END) as days_90_plus,
        SUM(e.unmatched_amount) as total
        FROM ap_entries e
        WHERE e.status NOT IN ('paid')
        GROUP BY e.vendor_name
        ORDER BY SUM(e.unmatched_amount) DESC`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "aging query failed"})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var vendor string
		var current, d130, d3160, d6190, d90plus, total float64
		if err := rows.Scan(&vendor, &current, &d130, &d3160, &d6190, &d90plus, &total); err != nil {
			continue
		}
		results = append(results, gin.H{
			"vendor":      vendor,
			"current":     current,
			"days_1_30":   d130,
			"days_31_60":  d3160,
			"days_61_90":  d6190,
			"days_90_plus": d90plus,
			"total":       total,
		})
	}
	if results == nil {
		results = []gin.H{}
	}
	c.JSON(http.StatusOK, results)
}
