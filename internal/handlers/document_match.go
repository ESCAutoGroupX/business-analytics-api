package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
)

// DocumentMatchHandler handles document-to-transaction matching endpoints.
type DocumentMatchHandler struct {
	GormDB *gorm.DB
}

func (h *DocumentMatchHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// ──────────────────────────────────────────────
// Request / response types
// ──────────────────────────────────────────────

type docMatchRequest struct {
	DocumentID int    `json:"document_id"`
	Action     string `json:"action" binding:"required"`
}

type documentStatusResponse struct {
	Status       string   `json:"status"`
	Score        *int     `json:"score"`
	DocumentID   *int     `json:"document_id"`
	DocumentType *string  `json:"document_type"`
	VendorName   *string  `json:"vendor_name"`
	Amount       *float64 `json:"amount"`
	ThumbnailURL *string  `json:"thumbnail_url"`
}

type closestMatchDoc struct {
	ID           int      `json:"id"`
	VendorName   *string  `json:"vendor_name"`
	DocumentType *string  `json:"document_type"`
	TotalAmount  *float64 `json:"total_amount"`
	DocumentDate *string  `json:"document_date"`
}

type closestMatchTxn struct {
	ID     string   `json:"id"`
	Name   *string  `json:"name"`
	Amount *float64 `json:"amount"`
	Date   *string  `json:"date"`
}

type closestMatchEntry struct {
	Document             closestMatchDoc `json:"document"`
	CandidateTransaction closestMatchTxn `json:"candidate_transaction"`
	Score                int             `json:"score"`
	Rule                 string          `json:"rule"`
}

type unmatchedDocEntry struct {
	ID           int      `json:"id"`
	VendorName   *string  `json:"vendor_name"`
	DocumentType *string  `json:"document_type"`
	TotalAmount  *float64 `json:"total_amount"`
	DocumentDate *string  `json:"document_date"`
	Status       string   `json:"status"`
}

// ──────────────────────────────────────────────
// GET /transactions/:id/document-status
// ──────────────────────────────────────────────

// GetDocumentStatus returns the document match status for a transaction.
func (h *DocumentMatchHandler) GetDocumentStatus(c *gin.Context) {
	txnID := c.Param("transaction_id")

	var txn models.Transaction
	if err := h.GormDB.Where("id = ?", txnID).First(&txn).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	resp := documentStatusResponse{
		Status: "none",
	}

	if txn.DocumentMatchStatus != nil {
		resp.Status = *txn.DocumentMatchStatus
	}
	resp.Score = txn.DocumentMatchScore

	if txn.MatchedDocumentID != nil {
		resp.DocumentID = txn.MatchedDocumentID

		var doc models.Document
		if err := h.GormDB.Where("id = ?", *txn.MatchedDocumentID).First(&doc).Error; err == nil {
			resp.DocumentType = doc.DocumentType
			resp.VendorName = doc.VendorName
			resp.Amount = doc.TotalAmount
			thumbURL := fmt.Sprintf("/documents/%d/file?token=auto", doc.ID)
			resp.ThumbnailURL = &thumbURL
		}
	}

	c.JSON(http.StatusOK, resp)
}

// ──────────────────────────────────────────────
// POST /transactions/:id/document-match
// ──────────────────────────────────────────────

// SetDocumentMatch handles manual match, unmatch, and explicit_unmatch actions.
func (h *DocumentMatchHandler) SetDocumentMatch(c *gin.Context) {
	txnID := c.Param("transaction_id")

	var req docMatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"detail": err.Error()})
		return
	}

	// Verify transaction exists
	var txn models.Transaction
	if err := h.GormDB.Where("id = ?", txnID).First(&txn).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Transaction not found"})
		return
	}

	switch req.Action {
	case "match":
		// Verify document exists
		var doc models.Document
		if err := h.GormDB.Where("id = ?", req.DocumentID).First(&doc).Error; err != nil {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Document not found"})
			return
		}

		matchedStatus := "matched"
		manualScore := 100

		// Update transaction
		h.GormDB.Model(&models.Transaction{}).Where("id = ?", txnID).Updates(map[string]interface{}{
			"matched_document_id":   req.DocumentID,
			"document_match_status": matchedStatus,
			"document_match_score":  manualScore,
		})

		// Update document
		h.GormDB.Model(&models.Document{}).Where("id = ?", req.DocumentID).Updates(map[string]interface{}{
			"matched_transaction_id": txnID,
			"status":                 "matched",
		})

		c.JSON(http.StatusOK, gin.H{"message": "Document matched successfully", "status": matchedStatus})

	case "unmatch":
		// Clear document link if it pointed to this transaction
		if txn.MatchedDocumentID != nil {
			h.GormDB.Model(&models.Document{}).
				Where("id = ? AND matched_transaction_id = ?", *txn.MatchedDocumentID, txnID).
				Updates(map[string]interface{}{
					"matched_transaction_id": nil,
					"status":                 "pending",
				})
		}

		// Clear transaction fields
		h.GormDB.Model(&models.Transaction{}).Where("id = ?", txnID).Updates(map[string]interface{}{
			"matched_document_id":   nil,
			"document_match_status": "none",
			"document_match_score":  nil,
		})

		c.JSON(http.StatusOK, gin.H{"message": "Document unmatched successfully", "status": "none"})

	case "explicit_unmatch":
		h.GormDB.Model(&models.Transaction{}).Where("id = ?", txnID).Updates(map[string]interface{}{
			"document_match_status": "unmatched_explicit",
		})

		c.JSON(http.StatusOK, gin.H{"message": "Transaction explicitly unmatched", "status": "unmatched_explicit"})

	default:
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("invalid action: %s", req.Action)})
	}
}

// MatchDocument is the legacy POST handler name referenced by the router.
// It delegates to SetDocumentMatch.
func (h *DocumentMatchHandler) MatchDocument(c *gin.Context) {
	h.SetDocumentMatch(c)
}

// ──────────────────────────────────────────────
// GET /documents/unmatched-bucket
// ──────────────────────────────────────────────

// UnmatchedBucket returns closest_matches and all_unmatched document lists.
func (h *DocumentMatchHandler) UnmatchedBucket(c *gin.Context) {
	db := h.sqlDB()

	// ── closest_matches ──
	// Fetch unmatched financial documents (limit 50)
	docRows, err := db.QueryContext(context.Background(), `
		SELECT id, vendor_name, document_type, total_amount, document_date
		FROM documents
		WHERE matched_transaction_id IS NULL
		  AND is_deleted = false
		  AND (is_financial IS NULL OR is_financial = true)
		  AND status NOT IN ('passthrough', 'ocr_failed', 'split')
		ORDER BY created_at DESC
		LIMIT 50
	`)
	if err != nil {
		log.Printf("document_match: failed to query unmatched documents: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query documents"})
		return
	}
	defer docRows.Close()

	type unmatchedDoc struct {
		ID           int
		VendorName   *string
		DocumentType *string
		TotalAmount  *float64
		DocumentDate *string
	}

	var docs []unmatchedDoc
	for docRows.Next() {
		var d unmatchedDoc
		if err := docRows.Scan(&d.ID, &d.VendorName, &d.DocumentType, &d.TotalAmount, &d.DocumentDate); err != nil {
			continue
		}
		docs = append(docs, d)
	}

	closestMatches := []closestMatchEntry{}

	for _, doc := range docs {
		if doc.TotalAmount == nil {
			continue
		}

		docAmount := *doc.TotalAmount
		docDate := ""
		if doc.DocumentDate != nil {
			docDate = *doc.DocumentDate
		}

		// Query candidate transactions (amount within 10%, date within 14 days)
		candidateRows, err := db.QueryContext(context.Background(), `
			SELECT id, amount, date, name, merchant_name, vendor
			FROM transactions
			WHERE ABS(COALESCE(amount, 0) + COALESCE($1, 0)) < (ABS(COALESCE($1, 0)) * 0.10 + 0.01)
			  AND CASE WHEN $2 = '' THEN true
			       ELSE date BETWEEN ($2::date - interval '14 days')::date AND ($2::date + interval '14 days')::date
			  END
			  AND document_match_status IS DISTINCT FROM 'unmatched_explicit'
			  AND matched_document_id IS NULL
			ORDER BY ABS(COALESCE(amount, 0) + COALESCE($1, 0)) ASC
			LIMIT 5
		`, docAmount, docDate)
		if err != nil {
			log.Printf("document_match: candidate query error for doc %d: %v", doc.ID, err)
			continue
		}

		var bestScore int
		var bestEntry closestMatchEntry

		for candidateRows.Next() {
			var txnID string
			var txnAmount *float64
			var txnDate, txnName, txnMerchant, txnVendor *string

			if err := candidateRows.Scan(&txnID, &txnAmount, &txnDate, &txnName, &txnMerchant, &txnVendor); err != nil {
				continue
			}

			score, rule := scoreMatch(docAmount, docDate, dmSafeStr(doc.VendorName), txnAmount, dmSafeStr(txnDate), txnName, txnMerchant, txnVendor, "")

			if score > bestScore {
				bestScore = score
				bestEntry = closestMatchEntry{
					Document: closestMatchDoc{
						ID:           doc.ID,
						VendorName:   doc.VendorName,
						DocumentType: doc.DocumentType,
						TotalAmount:  doc.TotalAmount,
						DocumentDate: doc.DocumentDate,
					},
					CandidateTransaction: closestMatchTxn{
						ID:     txnID,
						Name:   txnName,
						Amount: txnAmount,
						Date:   txnDate,
					},
					Score: score,
					Rule:  rule,
				}
			}
		}
		candidateRows.Close()

		if bestScore > 0 {
			closestMatches = append(closestMatches, bestEntry)
		}
	}

	// ── all_unmatched ──
	allRows, err := db.QueryContext(context.Background(), `
		SELECT id, vendor_name, document_type, total_amount, document_date, status
		FROM documents
		WHERE matched_transaction_id IS NULL
		  AND is_deleted = false
		  AND status IN ('pending', 'unmatched', 'needs_review', 'auto_matched')
		  AND (is_financial IS NULL OR is_financial = true)
		ORDER BY created_at DESC
		LIMIT 100
	`)
	if err != nil {
		log.Printf("document_match: failed to query all_unmatched: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to query unmatched documents"})
		return
	}
	defer allRows.Close()

	allUnmatched := []unmatchedDocEntry{}
	for allRows.Next() {
		var e unmatchedDocEntry
		if err := allRows.Scan(&e.ID, &e.VendorName, &e.DocumentType, &e.TotalAmount, &e.DocumentDate, &e.Status); err != nil {
			continue
		}
		allUnmatched = append(allUnmatched, e)
	}

	c.JSON(http.StatusOK, gin.H{
		"closest_matches": closestMatches,
		"all_unmatched":   allUnmatched,
	})
}

// ──────────────────────────────────────────────
// Auto-match background function (single document)
// ──────────────────────────────────────────────

// MatchDocumentToTransactions scores a single document against candidate
// transactions and auto-matches or marks suspect. Called as a goroutine
// after document creation.
func (h *DocumentMatchHandler) MatchDocumentToTransactions(doc *models.Document) {
	if doc == nil {
		return
	}

	// Only match financial documents without an existing match
	if doc.MatchedTransactionID != nil {
		return
	}
	if doc.IsFinancial != nil && !*doc.IsFinancial {
		return
	}
	if doc.TotalAmount == nil {
		return
	}

	docAmount := *doc.TotalAmount
	docDate := ""
	if doc.DocumentDate != nil {
		docDate = *doc.DocumentDate
	}
	docVendor := ""
	if doc.VendorName != nil {
		docVendor = *doc.VendorName
	}

	db := h.sqlDB()
	if db == nil {
		log.Printf("document_match: cannot get sql.DB for auto-match doc %d", doc.ID)
		return
	}

	// Guard: if docDate is empty or unparseable, skip the raw query that
	// requires a castable date parameter.
	if docDate == "" {
		log.Printf("document_match: skipping auto-match for doc %d (no document_date)", doc.ID)
		return
	}

	rows, err := db.QueryContext(context.Background(), `
		SELECT id, amount, date, name, merchant_name, vendor
		FROM transactions
		WHERE ABS(COALESCE(amount, 0) + COALESCE($1, 0)) < (ABS(COALESCE($1, 0)) * 0.05 + 0.01)
		  AND date BETWEEN ($2::date - interval '14 days')::date AND ($2::date + interval '14 days')::date
		  AND document_match_status IS DISTINCT FROM 'unmatched_explicit'
		ORDER BY ABS(COALESCE(amount, 0) + COALESCE($1, 0)) ASC
		LIMIT 5
	`, docAmount, docDate)
	if err != nil {
		log.Printf("document_match: auto-match query error for doc %d: %v", doc.ID, err)
		return
	}
	defer rows.Close()

	// Look up parent_brand from the vendor table for extra vendor matching
	parentBrand := ""
	if docVendor != "" {
		var pb *string
		err := db.QueryRowContext(context.Background(),
			`SELECT parent_brand FROM vendors WHERE LOWER(name) = LOWER($1) LIMIT 1`, docVendor,
		).Scan(&pb)
		if err == nil && pb != nil {
			parentBrand = *pb
		}
	}

	type candidate struct {
		ID           string
		Amount       *float64
		Date         *string
		Name         *string
		MerchantName *string
		Vendor       *string
		Score        int
	}

	var best candidate

	for rows.Next() {
		var cand candidate
		if err := rows.Scan(&cand.ID, &cand.Amount, &cand.Date, &cand.Name, &cand.MerchantName, &cand.Vendor); err != nil {
			continue
		}

		score, _ := scoreMatch(docAmount, docDate, docVendor,
			cand.Amount, dmSafeStr(cand.Date),
			cand.Name, cand.MerchantName, cand.Vendor, parentBrand)
		cand.Score = score

		if score > best.Score {
			best = cand
		}
	}

	if best.Score < 40 {
		return
	}

	if best.Score >= 75 {
		// Strong match
		matchedStatus := "matched"
		h.GormDB.Model(&models.Transaction{}).Where("id = ?", best.ID).Updates(map[string]interface{}{
			"matched_document_id":   doc.ID,
			"document_match_score":  best.Score,
			"document_match_status": matchedStatus,
		})
		h.GormDB.Model(&models.Document{}).Where("id = ?", doc.ID).Updates(map[string]interface{}{
			"matched_transaction_id": best.ID,
			"status":                 "matched",
		})
		log.Printf("document_match: auto-matched doc %d -> txn %s (score %d)", doc.ID, best.ID, best.Score)
	} else {
		// Suspect match (40-74)
		suspectStatus := "suspect"
		h.GormDB.Model(&models.Transaction{}).Where("id = ?", best.ID).Updates(map[string]interface{}{
			"matched_document_id":   doc.ID,
			"document_match_score":  best.Score,
			"document_match_status": suspectStatus,
		})
		log.Printf("document_match: suspect match doc %d -> txn %s (score %d)", doc.ID, best.ID, best.Score)
	}
}

// matchDocumentToTransactions is a helper on DocumentHandler so the upload flow
// can trigger auto-matching without importing the DocumentMatchHandler directly.
func (h *DocumentHandler) matchDocumentToTransactions(doc *models.Document) {
	dmh := &DocumentMatchHandler{GormDB: h.GormDB}
	dmh.MatchDocumentToTransactions(doc)
}

// ──────────────────────────────────────────────
// Scoring helpers
// ──────────────────────────────────────────────

// scoreMatch computes a match score and human-readable rule string for a
// document/transaction pair. parentBrand is an optional vendor parent brand
// looked up from the vendors table.
func scoreMatch(
	docAmount float64, docDate string, docVendor string,
	txnAmount *float64, txnDate string,
	txnName, txnMerchant, txnVendor *string,
	parentBrand string,
) (int, string) {
	score := 0
	var parts []string

	// ── Amount scoring ──
	if txnAmount != nil {
		// Plaid amounts are negative for debits; doc amounts are positive.
		diff := math.Abs(*txnAmount + docAmount)
		absDoc := math.Abs(docAmount)

		if diff <= 0.01 {
			score += 40
			parts = append(parts, "amount_exact")
		} else if absDoc > 0 && diff/absDoc <= 0.01 {
			score += 35
			parts = append(parts, "amount_1pct")
		} else if absDoc > 0 && diff/absDoc <= 0.05 {
			score += 25
			parts = append(parts, "amount_5pct")
		}
	}

	// ── Vendor scoring ──
	if docVendor != "" {
		vendorScore, vendorRule := bestVendorScore(docVendor, txnName, txnMerchant, txnVendor, parentBrand)
		score += vendorScore
		if vendorRule != "" {
			parts = append(parts, vendorRule)
		}
	}

	// ── Date scoring ──
	if docDate != "" && txnDate != "" {
		dDoc, errDoc := time.Parse("2006-01-02", docDate)
		dTxn, errTxn := time.Parse("2006-01-02", txnDate)
		if errDoc == nil && errTxn == nil {
			daysDiff := math.Abs(dDoc.Sub(dTxn).Hours() / 24)
			if daysDiff <= 3 {
				score += 20
				parts = append(parts, "date_3d")
			} else if daysDiff <= 7 {
				score += 15
				parts = append(parts, "date_7d")
			} else if daysDiff <= 14 {
				score += 10
				parts = append(parts, "date_14d")
			}
		}
	}

	rule := strings.Join(parts, "+")
	return score, rule
}

// bestVendorScore checks the document vendor against all available transaction
// vendor fields (name, merchant_name, vendor) and the parent_brand from the
// vendors table. Returns the best score and the corresponding rule tag.
func bestVendorScore(docVendor string, txnName, txnMerchant, txnVendor *string, parentBrand string) (int, string) {
	bestScore := 0
	bestRule := ""

	candidates := []string{}
	if txnName != nil {
		candidates = append(candidates, *txnName)
	}
	if txnMerchant != nil {
		candidates = append(candidates, *txnMerchant)
	}
	if txnVendor != nil {
		candidates = append(candidates, *txnVendor)
	}
	if parentBrand != "" {
		candidates = append(candidates, parentBrand)
	}

	docVendorLower := strings.ToLower(strings.TrimSpace(docVendor))

	for _, c := range candidates {
		cLower := strings.ToLower(strings.TrimSpace(c))
		if cLower == "" {
			continue
		}

		if cLower == docVendorLower {
			return 35, "vendor_exact"
		}

		sim := services.StringSimilarity(docVendorLower, cLower)
		if sim >= 0.80 && 25 > bestScore {
			bestScore = 25
			bestRule = "vendor_fuzzy"
		}
	}

	return bestScore, bestRule
}

// dmSafeStr dereferences a *string, returning "" if nil.
func dmSafeStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
