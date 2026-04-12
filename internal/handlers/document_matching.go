package handlers

import (
	"log"
	"math"
	"strings"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/services"
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

// MatchDocumentsToTransactions is the background auto-match job.
// It runs after document upload and on Plaid sync.
func (h *DocumentMatchHandler) MatchDocumentsToTransactions() {
	log.Println("[DocMatch] Starting auto-match job")

	var docs []models.Document
	h.GormDB.Where("(is_financial IS NULL OR is_financial = true)").
		Where("matched_transaction_id IS NULL").
		Where("status IN ('pending', 'unmatched', 'auto_matched', 'needs_review')").
		Where("total_amount IS NOT NULL AND total_amount > 0").
		Find(&docs)

	matched := 0
	suspect := 0

	for _, doc := range docs {
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

		if bestTx != nil && bestScore >= 40 {
			status := "suspect"
			if bestScore >= 75 {
				status = "matched"
				matched++
			} else {
				suspect++
			}

			h.GormDB.Model(bestTx).Updates(map[string]interface{}{
				"matched_document_id":   doc.ID,
				"document_match_score":  bestScore,
				"document_match_status": status,
			})

			if status == "matched" {
				h.GormDB.Model(&doc).Update("status", "auto_matched")
			}
		}
	}

	log.Printf("[DocMatch] Auto-match complete: %d matched, %d suspect out of %d documents", matched, suspect, len(docs))
}
