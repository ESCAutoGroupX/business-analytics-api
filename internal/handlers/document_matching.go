package handlers

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
)

// ── Match-All batch state (atomic) ──────────────────────────────
var (
	matchAllRunning   int32
	matchAllTotal     int32
	matchAllCompleted int32
	matchAllMatched   int32
	matchAllSuspect   int32
)

// AutoMigrate adds document match columns to transactions table.
func (h *DocumentMatchHandler) AutoMigrate() {
	db := h.sqlDB()
	if db == nil {
		return
	}
	migrations := []string{
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS matched_document_id INTEGER`,
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS document_match_score INTEGER`,
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS document_match_status VARCHAR DEFAULT 'none'`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS is_financial BOOLEAN DEFAULT true`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS customer_name VARCHAR`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_folder_path VARCHAR`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_keywords JSONB`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_category VARCHAR`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS matched_transaction_ids JSONB`,
	}
	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			log.Printf("[DocumentMatchHandler] migration warning: %v", err)
		}
	}
}

// scoreDocToTransaction computes a match score between a document and transaction.
// Used by the batch MatchDocumentsToTransactions job.
func scoreDocToTransaction(doc models.Document, tx models.Transaction, dateStr, vendorName string) int {
	score := 0

	// Amount scoring
	if doc.TotalAmount != nil && tx.Amount != nil {
		docAmt := *doc.TotalAmount
		txAmt := math.Abs(*tx.Amount)
		diff := math.Abs(docAmt - txAmt)

		if diff < 0.01 {
			score += 40
		} else if docAmt > 0 && diff < docAmt*0.01 {
			score += 35
		} else if docAmt > 0 && diff < docAmt*0.05 {
			score += 25
		}
	}

	// Vendor scoring
	if vendorName != "" {
		vnLower := strings.ToLower(vendorName)
		txName := ""
		if tx.Name != nil {
			txName = strings.ToLower(*tx.Name)
		}
		txMerchant := ""
		if tx.MerchantName != nil {
			txMerchant = strings.ToLower(*tx.MerchantName)
		}
		txVendor := ""
		if tx.Vendor != nil {
			txVendor = strings.ToLower(*tx.Vendor)
		}

		if txName == vnLower || txMerchant == vnLower || txVendor == vnLower {
			score += 35
		} else if strings.Contains(txName, vnLower) || strings.Contains(txMerchant, vnLower) || strings.Contains(txVendor, vnLower) {
			score += 25
		} else {
			sim := services.BidirectionalSimilarity(vnLower, txName)
			if sim2 := services.BidirectionalSimilarity(vnLower, txMerchant); sim2 > sim {
				sim = sim2
			}
			if sim >= 0.80 {
				score += 25
			} else if sim >= 0.60 {
				score += 15
			}
		}
	}

	// Date scoring
	if dateStr != "" && tx.Date != nil {
		docDate, err1 := time.Parse("2006-01-02", dateStr)
		txDate, err2 := time.Parse("2006-01-02", *tx.Date)
		if err1 == nil && err2 == nil {
			daysDiff := math.Abs(docDate.Sub(txDate).Hours() / 24)
			if daysDiff <= 3 {
				score += 20
			} else if daysDiff <= 7 {
				score += 15
			} else if daysDiff <= 14 {
				score += 10
			}
		}
	}

	return score
}

// MatchAll handles POST /documents/match-all — triggers batch matching.
func (h *DocumentMatchHandler) MatchAll(c *gin.Context) {
	if !atomic.CompareAndSwapInt32(&matchAllRunning, 0, 1) {
		c.JSON(http.StatusConflict, gin.H{
			"detail":    "match job already in progress",
			"completed": atomic.LoadInt32(&matchAllCompleted),
			"total":     atomic.LoadInt32(&matchAllTotal),
		})
		return
	}

	atomic.StoreInt32(&matchAllCompleted, 0)
	atomic.StoreInt32(&matchAllMatched, 0)
	atomic.StoreInt32(&matchAllSuspect, 0)

	var docs []models.Document
	h.GormDB.Where("(is_financial IS NULL OR is_financial = true)").
		Where("is_deleted = false").
		Where("status IN ('pending', 'unmatched', 'auto_matched', 'needs_review')").
		Where("matched_transaction_id IS NULL").
		Find(&docs)

	total := int32(len(docs))
	atomic.StoreInt32(&matchAllTotal, total)

	c.JSON(http.StatusOK, gin.H{"status": "started", "total": total})

	go h.runMatchAll(docs)
}

// MatchStatus handles GET /documents/match-status.
func (h *DocumentMatchHandler) MatchStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"completed": atomic.LoadInt32(&matchAllCompleted),
		"total":     atomic.LoadInt32(&matchAllTotal),
		"running":   atomic.LoadInt32(&matchAllRunning) == 1,
		"matched":   atomic.LoadInt32(&matchAllMatched),
		"suspect":   atomic.LoadInt32(&matchAllSuspect),
	})
}

// runMatchAll is the goroutine that processes all unmatched documents.
func (h *DocumentMatchHandler) runMatchAll(docs []models.Document) {
	defer atomic.StoreInt32(&matchAllRunning, 0)
	log.Println("[DocMatch] Starting batch match job")

	for _, doc := range docs {
		h.matchSingleDoc(doc)
		atomic.AddInt32(&matchAllCompleted, 1)
	}

	log.Printf("[DocMatch] Batch match complete: %d matched, %d suspect out of %d documents",
		atomic.LoadInt32(&matchAllMatched), atomic.LoadInt32(&matchAllSuspect), len(docs))
}

// MatchDocumentsToTransactions is the background auto-match job (no progress tracking).
// Called from cron / Plaid sync.
func (h *DocumentMatchHandler) MatchDocumentsToTransactions() {
	log.Println("[DocMatch] Starting auto-match job")

	var docs []models.Document
	h.GormDB.Where("(is_financial IS NULL OR is_financial = true)").
		Where("is_deleted = false").
		Where("status IN ('pending', 'unmatched', 'auto_matched', 'needs_review')").
		Where("matched_transaction_id IS NULL").
		Find(&docs)

	matched := 0
	suspect := 0
	for _, doc := range docs {
		result := h.matchSingleDoc(doc)
		if result == "matched" {
			matched++
		} else if result == "suspect" {
			suspect++
		}
	}
	log.Printf("[DocMatch] Auto-match complete: %d matched, %d suspect out of %d documents", matched, suspect, len(docs))
}

// matchSingleDoc attempts to match one document using vendor payment behavior. Returns status string.
func (h *DocumentMatchHandler) matchSingleDoc(doc models.Document) string {
	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}

	// Look up vendor config for payment behavior
	var vendor models.Vendor
	behavior := "PER_INVOICE"

	if vendorName != "" {
		if err := h.GormDB.Where("LOWER(name) = LOWER(?)", vendorName).First(&vendor).Error; err == nil {
			if vendor.PaymentBehavior != nil && *vendor.PaymentBehavior != "" {
				behavior = *vendor.PaymentBehavior
			}
		}
	}

	docType := ""
	if doc.DocumentType != nil {
		docType = strings.ToUpper(*doc.DocumentType)
	}

	switch behavior {
	case "PER_INVOICE":
		return h.matchPerInvoice(doc)

	case "SINGLE_PAYMENT":
		if docType == "STATEMENT" {
			return h.matchStatementSinglePayment(doc, vendor)
		}
		return h.matchInvoiceToStatement(doc)

	case "MULTIPLE_PAYMENTS":
		if docType == "STATEMENT" {
			return h.matchStatementMultiplePayments(doc, vendor)
		}
		return h.matchInvoiceToStatement(doc)

	default:
		return h.matchPerInvoice(doc)
	}
}

// matchPerInvoice finds a single transaction matching the invoice amount (within 1%),
// date (within 14 days), and vendor name. Uses scoreDocToTransaction for scoring.
// Thresholds: >=75 matched, 40-74 suspect.
func (h *DocumentMatchHandler) matchPerInvoice(doc models.Document) string {
	if doc.TotalAmount == nil || *doc.TotalAmount == 0 {
		return ""
	}
	amt := *doc.TotalAmount
	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}
	dateStr := ""
	if doc.DocumentDate != nil {
		dateStr = *doc.DocumentDate
	}

	query := h.GormDB.Model(&models.Transaction{}).
		Where("(document_match_status IS NULL OR document_match_status = 'none')").
		Where("ABS(amount - ?) < ? OR ABS(amount + ?) < ?",
			amt, amt*0.05+0.01, amt, amt*0.05+0.01)

	if dateStr != "" {
		if docDate, err := time.Parse("2006-01-02", dateStr); err == nil {
			from := docDate.AddDate(0, 0, -14).Format("2006-01-02")
			to := docDate.AddDate(0, 0, 14).Format("2006-01-02")
			query = query.Where("date >= ? AND date <= ?", from, to)
		}
	}

	if vendorName != "" {
		vLike := "%" + strings.ToLower(vendorName) + "%"
		query = query.Where("LOWER(name) LIKE ? OR LOWER(merchant_name) LIKE ? OR LOWER(vendor) LIKE ?",
			vLike, vLike, vLike)
	}

	var candidates []models.Transaction
	query.Limit(5).Find(&candidates)

	bestScore := 0
	var bestTx *models.Transaction
	for i := range candidates {
		s := scoreDocToTransaction(doc, candidates[i], dateStr, vendorName)
		if s > bestScore {
			bestScore = s
			bestTx = &candidates[i]
		}
	}

	return h.applyMatch(doc, bestTx, bestScore)
}

// matchStatementSinglePayment finds ONE bank transaction matching the statement total.
// Pattern: WorldPac statement $2,656 -> one ACH payment $2,656.
// Scoring: amount_exact(+40) + vendor_match(+35) + date_within_cycle(+20) = 95 max.
func (h *DocumentMatchHandler) matchStatementSinglePayment(doc models.Document, vendor models.Vendor) string {
	if doc.TotalAmount == nil || *doc.TotalAmount == 0 {
		return ""
	}
	docAmount := *doc.TotalAmount
	dateStr := ""
	if doc.DocumentDate != nil {
		dateStr = *doc.DocumentDate
	}
	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}

	// Payment cycle days: default 30 if nil
	cycleDays := 30
	if vendor.PaymentCycleDays != nil && *vendor.PaymentCycleDays > 0 {
		cycleDays = *vendor.PaymentCycleDays
	}

	// Query transactions within 2% of statement total
	query := h.GormDB.Model(&models.Transaction{}).
		Where("(document_match_status IS NULL OR document_match_status = 'none')").
		Where("ABS(ABS(amount) - ?) / ? < 0.02", docAmount, docAmount)

	// Date must be AFTER doc.DocumentDate and within payment_cycle_days
	if dateStr != "" {
		if docDate, err := time.Parse("2006-01-02", dateStr); err == nil {
			from := docDate.Format("2006-01-02")
			to := docDate.AddDate(0, 0, cycleDays).Format("2006-01-02")
			query = query.Where("date >= ? AND date <= ?", from, to)
		}
	}

	var candidates []models.Transaction
	query.Limit(10).Find(&candidates)

	bestScore := 0
	var bestTx *models.Transaction

	for i := range candidates {
		tx := candidates[i]
		score := 0

		// Amount scoring (+40 for exact/near match within 2%)
		if tx.Amount != nil {
			txAmt := math.Abs(*tx.Amount)
			diff := math.Abs(txAmt - docAmount)
			if docAmount > 0 && diff/docAmount < 0.02 {
				score += 40
			}
		}

		// Vendor scoring (+35 for fuzzy >= 0.6)
		if vendorName != "" {
			vnLower := strings.ToLower(vendorName)
			txFields := []string{}
			if tx.Name != nil {
				txFields = append(txFields, strings.ToLower(*tx.Name))
			}
			if tx.MerchantName != nil {
				txFields = append(txFields, strings.ToLower(*tx.MerchantName))
			}
			if tx.Vendor != nil {
				txFields = append(txFields, strings.ToLower(*tx.Vendor))
			}

			bestSim := 0.0
			for _, field := range txFields {
				if field == vnLower {
					bestSim = 1.0
					break
				}
				sim := services.BidirectionalSimilarity(vnLower, field)
				if sim > bestSim {
					bestSim = sim
				}
			}
			if bestSim >= 0.6 {
				score += 35
			}
		}

		// Date scoring (+20 for within cycle)
		if dateStr != "" && tx.Date != nil {
			docDate, err1 := time.Parse("2006-01-02", dateStr)
			txDate, err2 := time.Parse("2006-01-02", *tx.Date)
			if err1 == nil && err2 == nil {
				daysDiff := txDate.Sub(docDate).Hours() / 24
				if daysDiff >= 0 && daysDiff <= float64(cycleDays) {
					score += 20
				}
			}
		}

		if score > bestScore {
			bestScore = score
			bestTx = &candidates[i]
		}
	}

	if bestTx != nil && bestScore >= 40 {
		status := "suspect"
		if bestScore >= 75 {
			status = "matched"
			atomic.AddInt32(&matchAllMatched, 1)
		} else {
			atomic.AddInt32(&matchAllSuspect, 1)
		}

		h.GormDB.Model(bestTx).Updates(map[string]interface{}{
			"matched_document_id":   doc.ID,
			"document_match_score":  bestScore,
			"document_match_status": status,
		})

		if status == "matched" {
			h.GormDB.Model(&doc).Update("status", "auto_matched")
		}
		return status
	}

	return ""
}

// matchStatementMultiplePayments finds multiple bank transactions that sum to statement total.
// Pattern: Amex statement $15,000 -> 8 payments summing to $15,000.
// Uses a greedy approach: sort by absolute amount descending, accumulate until sum >= target * 0.98.
func (h *DocumentMatchHandler) matchStatementMultiplePayments(doc models.Document, vendor models.Vendor) string {
	if doc.TotalAmount == nil || *doc.TotalAmount == 0 {
		return ""
	}
	stmtTotal := *doc.TotalAmount
	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}

	// Payment cycle days: default 30 if nil
	cycleDays := 30
	if vendor.PaymentCycleDays != nil && *vendor.PaymentCycleDays > 0 {
		cycleDays = *vendor.PaymentCycleDays
	}

	// Build date range: period_start (from ocr_raw if available, else doc_date - cycle_days)
	// to period_end + 30 days
	fromDate := ""
	toDate := ""
	if doc.DocumentDate != nil {
		if docDate, err := time.Parse("2006-01-02", *doc.DocumentDate); err == nil {
			fromDate = docDate.AddDate(0, 0, -cycleDays).Format("2006-01-02")
			toDate = docDate.AddDate(0, 0, 30).Format("2006-01-02")
		}
	}

	query := h.GormDB.Model(&models.Transaction{}).
		Where("(document_match_status IS NULL OR document_match_status = 'none')")

	if fromDate != "" && toDate != "" {
		query = query.Where("date >= ? AND date <= ?", fromDate, toDate)
	}

	if vendorName != "" {
		vLike := "%" + strings.ToLower(vendorName) + "%"
		query = query.Where("LOWER(name) LIKE ? OR LOWER(merchant_name) LIKE ? OR LOWER(vendor) LIKE ?",
			vLike, vLike, vLike)
	}

	var candidates []models.Transaction
	query.Limit(50).Find(&candidates)

	if len(candidates) == 0 {
		return ""
	}

	// Sort by absolute amount descending (greedy approach)
	sort.Slice(candidates, func(i, j int) bool {
		ai := 0.0
		aj := 0.0
		if candidates[i].Amount != nil {
			ai = math.Abs(*candidates[i].Amount)
		}
		if candidates[j].Amount != nil {
			aj = math.Abs(*candidates[j].Amount)
		}
		return ai > aj
	})

	// Greedy accumulation: add transactions until sum >= target * 0.98
	target := stmtTotal
	threshold := target * 0.98
	var matchedTxs []models.Transaction
	runningSum := 0.0

	for i := range candidates {
		if candidates[i].Amount == nil {
			continue
		}
		txAmt := math.Abs(*candidates[i].Amount)
		runningSum += txAmt
		matchedTxs = append(matchedTxs, candidates[i])
		if runningSum >= threshold {
			break
		}
	}

	// Check if accumulated sum is within 2% of target
	diff := math.Abs(runningSum - target)
	if target > 0 && diff/target <= 0.02 && len(matchedTxs) > 0 {
		// Match found -- store all transaction IDs as JSON array in doc.MatchedTransactionIDs
		var txIDs []string
		for _, tx := range matchedTxs {
			txIDs = append(txIDs, tx.ID)
			h.GormDB.Model(&tx).Updates(map[string]interface{}{
				"matched_document_id":   doc.ID,
				"document_match_score":  85,
				"document_match_status": "matched",
			})
		}

		txIDsJSON, _ := json.Marshal(txIDs)
		txIDsStr := string(txIDsJSON)
		h.GormDB.Model(&doc).Updates(map[string]interface{}{
			"matched_transaction_ids": txIDsStr,
			"status":                  "matched",
		})

		atomic.AddInt32(&matchAllMatched, 1)
		log.Printf("[DocMatch] Multi-payment match: doc %d matched to %d transactions (sum=%.2f, statement=%.2f)",
			doc.ID, len(matchedTxs), runningSum, stmtTotal)
		return "matched"
	}

	return ""
}

// matchInvoiceToStatement links an individual invoice to an existing matched statement
// from the same vendor. Searches statement documents by vendor, then checks
// statement_line_items for the invoice number. If found, links the invoice to
// the statement's matched transaction.
func (h *DocumentMatchHandler) matchInvoiceToStatement(doc models.Document) string {
	vendorName := ""
	if doc.VendorName != nil {
		vendorName = *doc.VendorName
	}
	if vendorName == "" {
		return ""
	}

	invoiceNumber := ""
	if doc.VendorInvoiceNumber != nil {
		invoiceNumber = *doc.VendorInvoiceNumber
	}
	if invoiceNumber == "" && doc.DocumentNumber != nil {
		invoiceNumber = *doc.DocumentNumber
	}
	if invoiceNumber == "" {
		return ""
	}

	// Find recent matched statement documents from the same vendor
	var statements []models.Document
	h.GormDB.Where("document_type = 'STATEMENT' AND LOWER(vendor_name) = LOWER(?) AND matched_transaction_id IS NOT NULL",
		vendorName).Order("created_at DESC").Limit(5).Find(&statements)

	for _, stmt := range statements {
		// Check if this invoice number appears in the statement's line items
		var sli models.StatementLineItem
		if err := h.GormDB.Where("statement_document_id = ? AND invoice_number = ?",
			stmt.ID, invoiceNumber).First(&sli).Error; err == nil {
			// Found -- link the invoice to the statement's matched transaction
			if stmt.MatchedTransactionID != nil && *stmt.MatchedTransactionID != "" {
				h.GormDB.Model(&doc).Updates(map[string]interface{}{
					"matched_transaction_id": *stmt.MatchedTransactionID,
					"status":                 "matched",
				})
				atomic.AddInt32(&matchAllMatched, 1)
				log.Printf("[DocMatch] Invoice-to-statement: doc %d linked to statement %d (txn %s)",
					doc.ID, stmt.ID, *stmt.MatchedTransactionID)
				return "matched"
			}
		}
	}

	return ""
}

// applyMatch updates both transaction and document with match result.
func (h *DocumentMatchHandler) applyMatch(doc models.Document, bestTx *models.Transaction, bestScore int) string {
	if bestTx == nil || bestScore < 40 {
		return ""
	}

	status := "suspect"
	if bestScore >= 75 {
		status = "matched"
		atomic.AddInt32(&matchAllMatched, 1)
	} else {
		atomic.AddInt32(&matchAllSuspect, 1)
	}

	txUpdates := map[string]interface{}{
		"matched_document_id":   doc.ID,
		"document_match_score":  bestScore,
		"document_match_status": status,
	}

	// Fill ocr_vendor_name when the transaction's own name/merchant does not
	// already carry the vendor. Helps the ledger show the real vendor even
	// when the Plaid description is a generic "NET SETLMT …" line.
	if doc.VendorName != nil && *doc.VendorName != "" {
		vn := strings.ToLower(*doc.VendorName)
		hasVendor := false
		if bestTx.MerchantName != nil && strings.Contains(strings.ToLower(*bestTx.MerchantName), vn) {
			hasVendor = true
		}
		if bestTx.Name != nil && strings.Contains(strings.ToLower(*bestTx.Name), vn) {
			hasVendor = true
		}
		if !hasVendor {
			txUpdates["ocr_vendor_name"] = *doc.VendorName
			txUpdates["ocr_vendor_source"] = "document_match"
		}
	}

	h.GormDB.Model(bestTx).Updates(txUpdates)

	if status == "matched" {
		h.GormDB.Model(&doc).Update("status", "auto_matched")
	}
	return status
}
