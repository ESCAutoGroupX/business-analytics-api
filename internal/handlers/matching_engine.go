package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// MatchingEngineHandler implements the WickedFile reconciliation matching engine.
type MatchingEngineHandler struct {
	GormDB *gorm.DB
}

func (h *MatchingEngineHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// ── Row types ───────────────────────────────────────────────────

type wfDoc struct {
	ID                string
	VendorID          sql.NullString
	LocationName      sql.NullString
	DocType           sql.NullString
	DocDate           sql.NullTime
	Amount            sql.NullFloat64
	InvoiceNumber     sql.NullString
	MatchedRMAID      sql.NullString
	MatchedCreditID   sql.NullString
	VendorNorm        sql.NullString // vendors.normalized_name
	StmtFreq          sql.NullString // vendors.statement_frequency
	MultiPayment      sql.NullBool   // vendors.multi_payment
	IsStatementVendor sql.NullBool   // derived: vendor_type = 'statement'
}

type plaidTx struct {
	ID              string
	LocationName    sql.NullString
	TransactionDate sql.NullTime
	Amount          sql.NullFloat64
	Description     sql.NullString
	MerchantName    sql.NullString
	NormalizedMerch sql.NullString
}

type scoreBreakdown struct {
	Amount   float64
	Date     float64
	Vendor   float64
	Invoice  float64
	Location float64
	Total    float64
}

type vendorAlias struct {
	Alias        string
	LocationName string
}

// stmtLineItem represents a line item from a statement's line_items JSONB.
type stmtLineItem struct {
	InvoiceID string  `json:"invoiceId"`
	Amount    float64 `json:"amount"`
	Date      string  `json:"date"`
}

const matchThreshold = 55.0 // TODO: raise back to 75 after tuning
const docBatchSize = 500
const txBatchLimit = 2000

// ── POST /admin/run-matching ────────────────────────────────────

func (h *MatchingEngineHandler) RunMatching(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[MatchEngine] PANIC: %v", r)
			debug.PrintStack()
		}
	}()
	log.Printf("[MatchEngine] Starting RunMatching")

	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	// ── Pre-flight migrations ───────────────────────────────────
	db.Exec(`ALTER TABLE vendor_aliases ADD COLUMN IF NOT EXISTS location_name VARCHAR`)
	db.Exec(`ALTER TABLE match_results DROP CONSTRAINT IF EXISTS match_results_match_type_check`)
	db.Exec(`ALTER TABLE match_results ADD CONSTRAINT match_results_match_type_check CHECK (match_type IN ('auto', 'manual', 'ai_tiebreak', 'auto_multi_pay', 'auto_multi_pay_partial', 'auto_statement_group'))`)
	db.Exec(`ALTER TABLE statement_periods DROP CONSTRAINT IF EXISTS statement_periods_status_check`)
	db.Exec(`ALTER TABLE statement_periods ADD CONSTRAINT statement_periods_status_check CHECK (status IN ('open', 'reconciled', 'discrepancy', 'fully_matched', 'partial'))`)
	db.Exec(`ALTER TABLE statement_periods ADD COLUMN IF NOT EXISTS location_name VARCHAR(100)`)
	db.Exec(`DROP INDEX IF EXISTS idx_stmt_periods_vendor_start`)
	db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stmt_periods_vendor_location_start ON statement_periods (vendor_id, location_name, period_start)`)
	db.Exec(`ALTER TABLE wf_documents ADD COLUMN IF NOT EXISTS matched_statement_id UUID`)

	if _, err := db.Exec(`INSERT INTO vendor_aliases (id, vendor_id, alias, source, location_name)
		SELECT gen_random_uuid(), v.id, 'rsr auto parts', 'manual', 'Highlands'
		FROM vendors v WHERE v.normalized_name = 'carquest auto parts'
		ON CONFLICT DO NOTHING`); err != nil {
		log.Printf("[MatchEngine] RSR alias INSERT error: %v", err)
	}
	if _, err := db.Exec(`UPDATE vendors SET vendor_type = 'statement', statement_frequency = 'weekly'
		WHERE normalized_name = 'rsr auto parts'`); err != nil {
		log.Printf("[MatchEngine] RSR vendor UPDATE error: %v", err)
	}

	// ── IMC Parts Authority aliases ─────────────────────────────
	db.Exec(`INSERT INTO vendor_aliases (id, vendor_id, alias, source)
		SELECT gen_random_uuid(), v.id, 'parts authority', 'manual'
		FROM vendors v WHERE v.normalized_name = 'imc parts authority'
		ON CONFLICT DO NOTHING`)

	// ── SSF Auto Parts aliases ──────────────────────────────────
	db.Exec(`INSERT INTO vendor_aliases (id, vendor_id, alias, source)
		SELECT gen_random_uuid(), v.id, 'ssf imported auto parts', 'manual'
		FROM vendors v WHERE v.normalized_name = 'ssf auto parts'
		ON CONFLICT DO NOTHING`)
	db.Exec(`INSERT INTO vendor_aliases (id, vendor_id, alias, source)
		SELECT gen_random_uuid(), v.id, 'ssf imported auto pasouth', 'manual'
		FROM vendors v WHERE v.normalized_name = 'ssf auto parts'
		ON CONFLICT DO NOTHING`)

	h.logTableColumns(db, "wf_documents")
	h.logTableColumns(db, "vendors")

	var linkedCount, unlinkedCount int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE vendor_id IS NOT NULL`).Scan(&linkedCount)
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE vendor_id IS NULL`).Scan(&unlinkedCount)
	log.Printf("[MatchEngine] Vendor coverage: %d linked, %d unlinked", linkedCount, unlinkedCount)

	aliases := h.loadVendorAliases(db)

	consumed := map[string]bool{}
	totalMatched := 0
	totalUnmatched := 0
	totalStmtGroup := 0
	totalMultiPay := 0
	var allErrors []string

	// ═══════════════════════════════════════════════════════════════
	// PHASE 1: STATEMENT MATCHING — invoice number approach
	// ═══════════════════════════════════════════════════════════════
	stmtMatched, stmtUnmatched, stmtErrs := h.matchStatements(db, aliases, consumed)
	totalMatched += stmtMatched
	totalStmtGroup += stmtMatched
	totalUnmatched += stmtUnmatched
	allErrors = append(allErrors, stmtErrs...)

	// ═══════════════════════════════════════════════════════════════
	// PHASE 2: DIRECT VENDORS — batched, amount-first per invoice
	// ═══════════════════════════════════════════════════════════════
	var totalPending int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE d.match_status = 'pending' AND d.doc_date > '2020-01-01'
		AND d.vendor_id IS NOT NULL AND d.amount IS NOT NULL
		AND NOT (v.vendor_type = 'statement' OR COALESCE(v.multi_payment, false) = true
		         OR LOWER(v.statement_frequency) = 'weekly')`).Scan(&totalPending)

	totalBatches := (totalPending + docBatchSize - 1) / docBatchSize
	if totalBatches == 0 {
		totalBatches = 1
	}
	log.Printf("[MatchEngine] Direct vendors: %d pending docs (%d batches)", totalPending, totalBatches)

	batchNum := 0
	offset := 0
	for {
		batchNum++
		docs, err := h.loadDirectDocBatch(db, docBatchSize, offset)
		if err != nil {
			log.Printf("[MatchEngine] batch %d load error: %v", batchNum, err)
			allErrors = append(allErrors, err.Error())
			break
		}
		if len(docs) == 0 {
			break
		}

		minDate, maxDate := batchDateRange(docs)
		txns, err := h.loadTxnsForDateRange(db, minDate, maxDate)
		if err != nil {
			log.Printf("[MatchEngine] batch %d txn load error: %v", batchNum, err)
			allErrors = append(allErrors, err.Error())
			break
		}

		log.Printf("[MatchEngine] Batch %d/%d: docs %d-%d, %d transactions",
			batchNum, totalBatches, offset+1, offset+len(docs), len(txns))

		for _, doc := range docs {
			isCOD := isCODVendor(doc)
			dateWindow := 3
			if isCOD {
				dateWindow = 0
			}
			best, bestScore := h.findDirectMatch(doc, txns, consumed, aliases, isCOD, dateWindow)
			if best != nil && bestScore.Total >= matchThreshold {
				if err := h.recordMatch(db, doc, *best, bestScore, "auto"); err != nil {
					allErrors = append(allErrors, err.Error())
					continue
				}
				consumed[best.ID] = true
				totalMatched++
			} else {
				totalUnmatched++
			}
		}

		offset += len(docs)
		if len(docs) < docBatchSize {
			break
		}
	}

	log.Printf("[MatchEngine] Done: %d processed, %d matched (stmt=%d, multi=%d), %d unmatched, %d errors",
		totalMatched+totalUnmatched, totalMatched, totalStmtGroup, totalMultiPay, totalUnmatched, len(allErrors))

	c.JSON(http.StatusOK, gin.H{
		"processed":         totalMatched + totalUnmatched,
		"matched":           totalMatched,
		"unmatched":         totalUnmatched,
		"statement_grouped": totalStmtGroup,
		"multi_pay_matched": totalMultiPay,
		"errors":            allErrors,
	})
}

// ── GET /admin/match-stats ──────────────────────────────────────

func (h *MatchingEngineHandler) MatchStats(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	var docTotal, docMatched, docPending int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents`).Scan(&docTotal)
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE match_status = 'matched'`).Scan(&docMatched)
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE match_status = 'pending'`).Scan(&docPending)

	var txTotal, txMatched, txPending int
	db.QueryRow(`SELECT COUNT(*) FROM plaid_transactions`).Scan(&txTotal)
	db.QueryRow(`SELECT COUNT(*) FROM plaid_transactions WHERE match_status = 'matched'`).Scan(&txMatched)
	db.QueryRow(`SELECT COUNT(*) FROM plaid_transactions WHERE match_status = 'pending'`).Scan(&txPending)

	matchRate := 0.0
	if docTotal > 0 {
		matchRate = float64(docMatched) / float64(docTotal) * 100
	}

	c.JSON(http.StatusOK, gin.H{
		"documents": gin.H{
			"total":     docTotal,
			"matched":   docMatched,
			"pending":   docPending,
			"unmatched": docTotal - docMatched - docPending,
		},
		"transactions": gin.H{
			"total":   txTotal,
			"matched": txMatched,
			"pending": txPending,
		},
		"match_rate_pct": math.Round(matchRate*100) / 100,
	})
}

// ── Diagnostics ─────────────────────────────────────────────────

func (h *MatchingEngineHandler) logTableColumns(db *sql.DB, tableName string) {
	rows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`, tableName)
	if err != nil {
		log.Printf("[MatchEngine] schema query error for %s: %v", tableName, err)
		return
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if rows.Scan(&col) == nil {
			cols = append(cols, col)
		}
	}
	log.Printf("[MatchEngine] %s columns: %s", tableName, strings.Join(cols, ", "))
}

// ═══════════════════════════════════════════════════════════════
// STATEMENT MATCHING — invoice number approach
// ═══════════════════════════════════════════════════════════════

// matchStatements loads all pending statement docs, matches their line items
// to invoice docs by invoice number, then matches the statement to a Plaid transaction.
func (h *MatchingEngineHandler) matchStatements(
	db *sql.DB, aliases map[string][]vendorAlias, consumed map[string]bool,
) (matched int, unmatched int, errors []string) {

	// Step 1: Load pending statement documents
	stmts, err := h.loadPendingStatements(db)
	if err != nil {
		log.Printf("[MatchEngine] load statements error: %v", err)
		errors = append(errors, err.Error())
		return
	}
	log.Printf("[MatchEngine] Statement matching: %d pending statements", len(stmts))

	for si, stmt := range stmts {
		// Step 2: Parse line_items JSONB and match to invoice docs
		lineItems := h.parseStatementLineItems(db, stmt)
		if len(lineItems) == 0 && si < 3 {
			log.Printf("[MatchEngine] Statement %s: no line items found", stmt.ID)
		}

		var matchedInvIDs []string
		matchedInvTotal := 0.0
		for _, li := range lineItems {
			if li.InvoiceID == "" {
				continue
			}
			// Find invoice doc by vendor_id + invoice_number
			var invID string
			err := db.QueryRow(`
				SELECT id FROM wf_documents
				WHERE vendor_id = $1
				  AND invoice_number = $2
				  AND doc_type IN ('invoice', 'credit', 'credit_memo')
				LIMIT 1`,
				stmt.VendorID.String, li.InvoiceID).Scan(&invID)
			if err == nil && invID != "" {
				matchedInvIDs = append(matchedInvIDs, invID)
				matchedInvTotal += math.Abs(li.Amount)
			}
		}

		// Step 3: Calculate confidence
		stmtAmount := 0.0
		if stmt.Amount.Valid {
			stmtAmount = math.Abs(stmt.Amount.Float64)
		}
		matchAmount := stmtAmount
		if matchAmount == 0 {
			matchAmount = matchedInvTotal
		}

		if si < 3 {
			log.Printf("[MatchEngine] Statement %s: vendor=%s amount=%.2f lineItems=%d matchedInvoices=%d matchedTotal=%.2f",
				stmt.ID, stmt.VendorNorm.String, stmtAmount, len(lineItems), len(matchedInvIDs), matchedInvTotal)
		}

		if matchAmount == 0 {
			unmatched++
			continue
		}

		// Step 5: Find matching Plaid transaction for the statement total
		vendorNorm := ""
		if stmt.VendorNorm.Valid {
			vendorNorm = stmt.VendorNorm.String
		}
		txns := h.loadTxnsForStatement(db, stmt, vendorNorm, aliases)

		var bestTx *plaidTx
		var bestScore scoreBreakdown
		for i := range txns {
			tx := &txns[i]
			if consumed[tx.ID] || !tx.Amount.Valid || !tx.TransactionDate.Valid {
				continue
			}
			txAmt := math.Abs(tx.Amount.Float64)
			s := scoreBreakdown{
				Amount:   scoreAmount(matchAmount, txAmt),
				Vendor:   scoreVendorWithAliases(stmt, *tx, aliases),
				Location: scoreLocationMatch(stmt, *tx),
			}
			if stmt.DocDate.Valid {
				s.Date = scoreDate(stmt.DocDate.Time, tx.TransactionDate.Time)
			}
			s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location

			if si < 3 {
				txDate := ""
				if tx.TransactionDate.Valid {
					txDate = tx.TransactionDate.Time.Format("2006-01-02")
				}
				merchant := ""
				if tx.NormalizedMerch.Valid {
					merchant = tx.NormalizedMerch.String
				}
				log.Printf("[MatchEngine] Stmt %s candidate: txID=%s merchant=%s amount=%.2f date=%s scores: amt=%.1f date=%.1f vendor=%.1f loc=%.1f TOTAL=%.1f",
					stmt.ID, tx.ID, merchant, txAmt, txDate,
					s.Amount, s.Date, s.Vendor, s.Location, s.Total)
			}

			if s.Total > bestScore.Total {
				bestScore = s
				bestTx = tx
			}
		}

		if bestTx == nil || bestScore.Total < matchThreshold {
			if si < 3 {
				best := 0.0
				if bestTx != nil {
					best = bestScore.Total
				}
				log.Printf("[MatchEngine] Statement %s: NO TX MATCH (best=%.1f threshold=%.0f)", stmt.ID, best, matchThreshold)
			}
			unmatched++
			continue
		}

		// Step 6: Mark everything matched
		consumed[bestTx.ID] = true
		now := time.Now()

		// Mark statement doc as matched
		if err := h.recordMatch(db, stmt, *bestTx, bestScore, "auto_statement_group"); err != nil {
			errors = append(errors, err.Error())
			unmatched++
			continue
		}
		matched++

		// Mark all matched invoices as linked to this statement + transaction
		for _, invID := range matchedInvIDs {
			db.Exec(`UPDATE wf_documents SET match_status = 'matched',
				matched_transaction_id = $1, matched_statement_id = $2,
				match_confidence = $3, updated_at = $4
				WHERE id = $5 AND match_status = 'pending'`,
				bestTx.ID, stmt.ID, bestScore.Total, now, invID)
			matched++
		}

		if si < 3 {
			log.Printf("[MatchEngine] Statement %s: MATCHED tx=%s score=%.1f, linked %d invoices",
				stmt.ID, bestTx.ID, bestScore.Total, len(matchedInvIDs))
		}
	}
	return
}

// loadPendingStatements loads statement docs with vendor info.
func (h *MatchingEngineHandler) loadPendingStatements(db *sql.DB) ([]wfDoc, error) {
	query := fmt.Sprintf(`SELECT %s
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE %s
		  AND d.doc_type = 'statement'
		ORDER BY d.doc_date ASC`, docSelectCols, docBaseWhere)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var docs []wfDoc
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, rows.Err()
}

// parseStatementLineItems reads line_items JSONB from the statement doc.
func (h *MatchingEngineHandler) parseStatementLineItems(db *sql.DB, stmt wfDoc) []stmtLineItem {
	var rawJSON sql.NullString
	db.QueryRow(`SELECT line_items FROM wf_documents WHERE id = $1`, stmt.ID).Scan(&rawJSON)
	if !rawJSON.Valid || rawJSON.String == "" || rawJSON.String == "null" {
		return nil
	}
	var items []stmtLineItem
	if err := json.Unmarshal([]byte(rawJSON.String), &items); err != nil {
		log.Printf("[MatchEngine] parse line_items for stmt %s: %v", stmt.ID, err)
		return nil
	}
	return items
}

// loadTxnsForStatement loads vendor-matching transactions in the statement's date window.
func (h *MatchingEngineHandler) loadTxnsForStatement(
	db *sql.DB, stmt wfDoc, vendorNorm string, aliases map[string][]vendorAlias,
) []plaidTx {
	from := ""
	to := ""
	if stmt.DocDate.Valid {
		from = stmt.DocDate.Time.Format("2006-01-02")
		to = stmt.DocDate.Time.AddDate(0, 0, 16).Format("2006-01-02")
	} else {
		return nil
	}

	var patterns []string
	if vendorNorm != "" {
		patterns = append(patterns, strings.ToLower(strings.TrimSpace(vendorNorm)))
	}
	if stmt.VendorID.Valid {
		for _, a := range aliases[stmt.VendorID.String] {
			patterns = append(patterns, a.Alias)
		}
	}
	if len(patterns) == 0 {
		return nil
	}

	var args []interface{}
	args = append(args, from, to)
	var likeConds []string
	for _, pat := range patterns {
		args = append(args, "%"+pat+"%")
		likeConds = append(likeConds, fmt.Sprintf("normalized_merchant LIKE $%d", len(args)))
	}
	vendorCond := "(" + strings.Join(likeConds, " OR ") + ")"

	args = append(args, txBatchLimit)
	limitIdx := len(args)

	query := fmt.Sprintf(`
		SELECT id, location_name, transaction_date, amount,
		       description, merchant_name, normalized_merchant
		FROM plaid_transactions
		WHERE match_status = 'pending'
		  AND transaction_date BETWEEN $1 AND $2
		  AND %s
		ORDER BY transaction_date ASC
		LIMIT $%d`, vendorCond, limitIdx)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("[MatchEngine] loadTxnsForStatement error: %v", err)
		return nil
	}
	defer rows.Close()
	txns, _ := scanTxns(rows)
	return txns
}

// ═══════════════════════════════════════════════════════════════
// DIRECT VENDOR MATCHING (batched)
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) findDirectMatch(
	doc wfDoc, txns []plaidTx, consumed map[string]bool,
	aliases map[string][]vendorAlias, isCOD bool, dateWindow int,
) (*plaidTx, scoreBreakdown) {
	var bestTx *plaidTx
	var bestScore scoreBreakdown

	for i := range txns {
		tx := &txns[i]
		if consumed[tx.ID] {
			continue
		}
		if !passesDateRule(doc, *tx, isCOD, dateWindow) {
			continue
		}
		score := computeFullScore(doc, *tx, aliases)
		if score.Total > bestScore.Total {
			bestScore = score
			bestTx = tx
		}
	}
	return bestTx, bestScore
}

// ═══════════════════════════════════════════════════════════════
// DATA LOADERS
// ═══════════════════════════════════════════════════════════════

const docSelectCols = `d.id, d.vendor_id, d.location_name, d.doc_type, d.doc_date,
	d.amount, d.invoice_number, d.matched_rma_id, d.matched_credit_id,
	v.normalized_name AS vendor_normalized_name,
	v.statement_frequency AS stmt_freq,
	COALESCE(v.multi_payment, false) AS multi_payment,
	CASE WHEN v.vendor_type = 'statement' THEN true ELSE false END AS is_statement_vendor`

const docBaseWhere = `d.match_status = 'pending'
	AND d.doc_date > '2020-01-01'
	AND d.vendor_id IS NOT NULL
	AND d.amount IS NOT NULL`

func scanDoc(rows *sql.Rows) (wfDoc, error) {
	var d wfDoc
	err := rows.Scan(&d.ID, &d.VendorID, &d.LocationName, &d.DocType,
		&d.DocDate, &d.Amount, &d.InvoiceNumber, &d.MatchedRMAID, &d.MatchedCreditID,
		&d.VendorNorm, &d.StmtFreq, &d.MultiPayment, &d.IsStatementVendor)
	return d, err
}

func (h *MatchingEngineHandler) loadDirectDocBatch(db *sql.DB, limit, offset int) ([]wfDoc, error) {
	query := fmt.Sprintf(`SELECT %s
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE %s
		  AND NOT (v.vendor_type = 'statement' OR COALESCE(v.multi_payment, false) = true
		           OR LOWER(v.statement_frequency) = 'weekly')
		ORDER BY d.doc_date ASC
		LIMIT $1 OFFSET $2`, docSelectCols, docBaseWhere)
	rows, err := db.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var docs []wfDoc
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, rows.Err()
}

func (h *MatchingEngineHandler) loadTxnsForDateRange(db *sql.DB, minDate, maxDate time.Time) ([]plaidTx, error) {
	from := minDate.AddDate(0, 0, -30).Format("2006-01-02")
	to := maxDate.AddDate(0, 0, 30).Format("2006-01-02")
	rows, err := db.Query(`
		SELECT id, location_name, transaction_date, amount,
		       description, merchant_name, normalized_merchant
		FROM plaid_transactions
		WHERE match_status = 'pending'
		  AND transaction_date BETWEEN $1 AND $2
		ORDER BY transaction_date ASC
		LIMIT $3`, from, to, txBatchLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTxns(rows)
}

func scanTxns(rows *sql.Rows) ([]plaidTx, error) {
	var txns []plaidTx
	for rows.Next() {
		var t plaidTx
		if err := rows.Scan(&t.ID, &t.LocationName, &t.TransactionDate,
			&t.Amount, &t.Description, &t.MerchantName, &t.NormalizedMerch); err != nil {
			return nil, err
		}
		txns = append(txns, t)
	}
	return txns, rows.Err()
}

func (h *MatchingEngineHandler) loadVendorAliases(db *sql.DB) map[string][]vendorAlias {
	aliases := map[string][]vendorAlias{}
	rows, err := db.Query(`SELECT vendor_id::text, LOWER(TRIM(alias)), COALESCE(location_name, '') FROM vendor_aliases`)
	if err != nil {
		log.Printf("[MatchEngine] load aliases error: %v", err)
		return aliases
	}
	defer rows.Close()
	for rows.Next() {
		var vid, alias, loc string
		if rows.Scan(&vid, &alias, &loc) == nil && alias != "" {
			aliases[vid] = append(aliases[vid], vendorAlias{Alias: alias, LocationName: loc})
		}
	}
	return aliases
}

func batchDateRange(docs []wfDoc) (time.Time, time.Time) {
	minDate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, d := range docs {
		if d.DocDate.Valid {
			if d.DocDate.Time.Before(minDate) {
				minDate = d.DocDate.Time
			}
			if d.DocDate.Time.After(maxDate) {
				maxDate = d.DocDate.Time
			}
		}
	}
	return minDate, maxDate
}

// ═══════════════════════════════════════════════════════════════
// CLASSIFICATION HELPERS
// ═══════════════════════════════════════════════════════════════

func isStatementVendor(doc wfDoc) bool {
	if doc.IsStatementVendor.Valid && doc.IsStatementVendor.Bool {
		return true
	}
	if doc.StmtFreq.Valid && doc.StmtFreq.String != "" {
		freq := strings.ToLower(strings.TrimSpace(doc.StmtFreq.String))
		return freq != "cod" && freq != ""
	}
	return false
}

func isCODVendor(doc wfDoc) bool {
	if isStatementVendor(doc) {
		return false
	}
	if !doc.StmtFreq.Valid || strings.TrimSpace(doc.StmtFreq.String) == "" {
		return true
	}
	return strings.EqualFold(doc.StmtFreq.String, "cod")
}

// ═══════════════════════════════════════════════════════════════
// CHRONOLOGICAL PRE-FILTERS
// ═══════════════════════════════════════════════════════════════

func passesDateRule(doc wfDoc, tx plaidTx, isCOD bool, dateWindow int) bool {
	if !doc.DocDate.Valid || !tx.TransactionDate.Valid {
		return true
	}
	docDate := doc.DocDate.Time
	txDate := tx.TransactionDate.Time

	if isCOD {
		return sameDay(docDate, txDate)
	}

	docType := ""
	if doc.DocType.Valid {
		docType = strings.ToUpper(doc.DocType.String)
	}
	switch docType {
	case "INVOICE":
		if txDate.Before(docDate) {
			return false
		}
		if dateWindow > 0 {
			return !txDate.After(docDate.AddDate(0, 0, dateWindow))
		}
		return true
	case "CREDIT", "CREDIT_MEMO":
		return !txDate.Before(docDate)
	}
	return true
}

func sameDay(a, b time.Time) bool {
	return a.Year() == b.Year() && a.YearDay() == b.YearDay()
}

// ═══════════════════════════════════════════════════════════════
// SCORING FUNCTIONS
// ═══════════════════════════════════════════════════════════════

func scoreAmount(docAmt, txAmt float64) float64 {
	docAmt = math.Abs(docAmt)
	txAmt = math.Abs(txAmt)
	if docAmt == 0 {
		return 0
	}
	pctDiff := math.Abs(docAmt-txAmt) / docAmt
	if pctDiff < 0.001 {
		return 25
	}
	if pctDiff <= 0.01 {
		return 20
	}
	if pctDiff <= 0.05 {
		return 10
	}
	return 0
}

func scoreAmountDoc(doc wfDoc, tx plaidTx) float64 {
	if !doc.Amount.Valid || !tx.Amount.Valid {
		return 0
	}
	return scoreAmount(doc.Amount.Float64, tx.Amount.Float64)
}

func scoreDate(docTime, txTime time.Time) float64 {
	hours := math.Abs(docTime.Sub(txTime).Hours())
	dayOffset := hours / 24.0
	if dayOffset < 0.5 {
		return 30
	}
	pts := 30.0 * math.Pow(0.6, dayOffset)
	if pts < 0.5 {
		return 0
	}
	return math.Round(pts*10) / 10
}

func scoreDateDoc(doc wfDoc, tx plaidTx) float64 {
	if !doc.DocDate.Valid || !tx.TransactionDate.Valid {
		return 0
	}
	return scoreDate(doc.DocDate.Time, tx.TransactionDate.Time)
}

func scoreVendorWithAliases(doc wfDoc, tx plaidTx, aliases map[string][]vendorAlias) float64 {
	txMerch := ""
	if tx.NormalizedMerch.Valid {
		txMerch = strings.ToLower(strings.TrimSpace(tx.NormalizedMerch.String))
	}
	if txMerch == "" {
		return 0
	}
	if doc.VendorNorm.Valid {
		vnDoc := strings.ToLower(strings.TrimSpace(doc.VendorNorm.String))
		if vnDoc != "" {
			if vnDoc == txMerch || strings.Contains(txMerch, vnDoc) || strings.Contains(vnDoc, txMerch) {
				return 25
			}
		}
	}
	if doc.VendorID.Valid {
		docLoc := ""
		if doc.LocationName.Valid {
			docLoc = strings.ToLower(strings.TrimSpace(doc.LocationName.String))
		}
		for _, a := range aliases[doc.VendorID.String] {
			if a.LocationName != "" {
				if docLoc == "" || !strings.EqualFold(a.LocationName, docLoc) {
					continue
				}
			}
			if a.Alias == txMerch || strings.Contains(txMerch, a.Alias) || strings.Contains(a.Alias, txMerch) {
				return 25
			}
		}
	}
	return 0
}

func scoreInvoice(doc wfDoc, tx plaidTx) float64 {
	if !doc.InvoiceNumber.Valid || !tx.Description.Valid {
		return 0
	}
	inv := strings.TrimSpace(doc.InvoiceNumber.String)
	if inv == "" {
		return 0
	}
	if strings.Contains(strings.ToLower(tx.Description.String), strings.ToLower(inv)) {
		return 10
	}
	return 0
}

func scoreLocationMatch(doc wfDoc, tx plaidTx) float64 {
	if !doc.LocationName.Valid || !tx.LocationName.Valid {
		return 0
	}
	docLoc := strings.TrimSpace(doc.LocationName.String)
	txLoc := strings.TrimSpace(tx.LocationName.String)
	if docLoc == "" || txLoc == "" ||
		strings.EqualFold(docLoc, "Unknown") || strings.EqualFold(txLoc, "Unknown") {
		return 0
	}
	if strings.EqualFold(docLoc, txLoc) {
		return 10
	}
	return 0
}

func computeFullScore(doc wfDoc, tx plaidTx, aliases map[string][]vendorAlias) scoreBreakdown {
	s := scoreBreakdown{
		Amount:   scoreAmountDoc(doc, tx),
		Date:     scoreDateDoc(doc, tx),
		Vendor:   scoreVendorWithAliases(doc, tx, aliases),
		Invoice:  scoreInvoice(doc, tx),
		Location: scoreLocationMatch(doc, tx),
	}
	s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location
	return s
}

// ═══════════════════════════════════════════════════════════════
// STATEMENT PERIODS TRACKING
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) upsertStatementPeriod(db *sql.DB, vendorID, locationName string, periodStart, periodEnd time.Time, expectedTotal, matchedTotal float64, status string) {
	if vendorID == "" {
		return
	}
	if locationName == "" {
		locationName = "Unknown"
	}
	_, err := db.Exec(`
		INSERT INTO statement_periods (id, vendor_id, location_name, period_start, period_end, expected_total, matched_total, status, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (vendor_id, location_name, period_start) DO UPDATE SET
			expected_total = EXCLUDED.expected_total,
			matched_total = EXCLUDED.matched_total,
			status = EXCLUDED.status`,
		vendorID, locationName, periodStart, periodEnd, expectedTotal, matchedTotal, status)
	if err != nil {
		log.Printf("[MatchEngine] upsert statement_period error: %v", err)
		db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stmt_periods_vendor_location_start ON statement_periods (vendor_id, location_name, period_start)`)
		db.Exec(`
			INSERT INTO statement_periods (id, vendor_id, location_name, period_start, period_end, expected_total, matched_total, status, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, NOW())
			ON CONFLICT (vendor_id, location_name, period_start) DO UPDATE SET
				expected_total = EXCLUDED.expected_total,
				matched_total = EXCLUDED.matched_total,
				status = EXCLUDED.status`,
			vendorID, locationName, periodStart, periodEnd, expectedTotal, matchedTotal, status)
	}
}

// ═══════════════════════════════════════════════════════════════
// RECORD MATCH
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) recordMatch(db *sql.DB, doc wfDoc, tx plaidTx, score scoreBreakdown, matchType string) error {
	now := time.Now()

	_, err := db.Exec(`
		INSERT INTO match_results (id, document_id, transaction_id,
			score_amount, score_date, score_vendor, score_invoice, score_location,
			total_score, passed_threshold, date_rule_violated, match_type, matched_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, true, false, $9, $10)`,
		doc.ID, tx.ID,
		score.Amount, score.Date, score.Vendor, score.Invoice, score.Location,
		score.Total, matchType, now)
	if err != nil {
		return fmt.Errorf("insert match_results: %w", err)
	}

	_, err = db.Exec(`
		UPDATE wf_documents SET match_status = 'matched', matched_transaction_id = $1,
			match_confidence = $2, updated_at = $3
		WHERE id = $4`, tx.ID, score.Total, now, doc.ID)
	if err != nil {
		return fmt.Errorf("update wf_documents: %w", err)
	}

	_, err = db.Exec(`
		UPDATE plaid_transactions SET match_status = 'matched', matched_document_id = $1,
			match_confidence = $2
		WHERE id = $3`, doc.ID, score.Total, tx.ID)
	if err != nil {
		return fmt.Errorf("update plaid_transactions: %w", err)
	}
	return nil
}
