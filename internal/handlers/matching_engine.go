package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"regexp"
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

// statementGroup represents pages of a multi-page statement grouped together.
type statementGroup struct {
	VendorID       string
	VendorName     string
	LocationName   string
	CustomerNumber string
	DocDate        time.Time
	Pages          []wfDoc
	AllLineItems   []stmtLineItem
	ComputedTotal  float64
	DocumentTotal  float64
	HasDocTotal    bool
	Completeness   string // "complete", "missing_pages", "no_total_found"
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
	db.Exec(`ALTER TABLE match_results ADD CONSTRAINT match_results_match_type_check CHECK (match_type IN ('auto', 'manual', 'ai_tiebreak', 'auto_multi_pay', 'auto_multi_pay_partial', 'auto_statement_group', 'auto_utility'))`)
	db.Exec(`ALTER TABLE statement_periods DROP CONSTRAINT IF EXISTS statement_periods_status_check`)
	db.Exec(`ALTER TABLE statement_periods ADD CONSTRAINT statement_periods_status_check CHECK (status IN ('open', 'reconciled', 'discrepancy', 'fully_matched', 'partial'))`)
	db.Exec(`ALTER TABLE statement_periods ADD COLUMN IF NOT EXISTS location_name VARCHAR(100)`)
	db.Exec(`DROP INDEX IF EXISTS idx_stmt_periods_vendor_start`)
	db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stmt_periods_vendor_location_start ON statement_periods (vendor_id, location_name, period_start)`)
	db.Exec(`ALTER TABLE wf_documents ADD COLUMN IF NOT EXISTS matched_statement_id UUID`)

	// vendor_location_accounts table
	db.Exec(`CREATE TABLE IF NOT EXISTS vendor_location_accounts (
		id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		vendor_id       UUID NOT NULL REFERENCES vendors(id),
		customer_number VARCHAR(50) NOT NULL,
		location_name   VARCHAR(100) NOT NULL,
		notes           VARCHAR(255),
		created_at      TIMESTAMP DEFAULT NOW(),
		UNIQUE(vendor_id, customer_number)
	)`)
	// statement_periods new columns for page grouping
	db.Exec(`ALTER TABLE statement_periods ADD COLUMN IF NOT EXISTS completeness_status VARCHAR(20) DEFAULT 'unknown'`)
	db.Exec(`ALTER TABLE statement_periods ADD COLUMN IF NOT EXISTS customer_number VARCHAR(50)`)
	db.Exec(`ALTER TABLE statement_periods ADD COLUMN IF NOT EXISTS page_count INTEGER`)
	db.Exec(`ALTER TABLE statement_periods ADD COLUMN IF NOT EXISTS computed_total DECIMAL(12,2)`)

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
	// PRE-STEP: STATEMENT PAGE GROUPING
	// ═══════════════════════════════════════════════════════════════
	groups := h.groupStatementPages(db)
	completeGroups := 0
	missingGroups := 0
	noTotalGroups := 0
	for _, g := range groups {
		switch g.Completeness {
		case "complete":
			completeGroups++
		case "missing_pages":
			missingGroups++
		case "no_total_found":
			noTotalGroups++
		}
	}
	log.Printf("[MatchEngine] Statement page grouping: %d groups (%d complete, %d missing_pages, %d no_total_found)",
		len(groups), completeGroups, missingGroups, noTotalGroups)

	// ═══════════════════════════════════════════════════════════════
	// PHASE 1: STATEMENT MATCHING — grouped pages, invoice number approach
	// ═══════════════════════════════════════════════════════════════
	stmtMatched, stmtUnmatched, stmtErrs := h.matchStatementGroups(db, aliases, consumed, groups)
	totalMatched += stmtMatched
	totalStmtGroup += stmtMatched
	totalUnmatched += stmtUnmatched
	allErrors = append(allErrors, stmtErrs...)

	// ═══════════════════════════════════════════════════════════════
	// PHASE 2: DIRECT VENDORS — batched, amount-first per invoice
	// ═══════════════════════════════════════════════════════════════
	var totalPending int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents d
		JOIN vendors v ON v.id = d.vendor_id
		WHERE d.match_status = 'pending' AND d.doc_date > '2020-01-01'
		AND d.amount IS NOT NULL AND d.vendor_id IS NOT NULL
		AND v.vendor_type != 'statement'`).Scan(&totalPending)

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

		batchMatches := 0
		for _, doc := range docs {
			vendorNorm := ""
			if doc.VendorNorm.Valid {
				vendorNorm = strings.ToLower(strings.TrimSpace(doc.VendorNorm.String))
			}
			if vendorNorm == "" || !doc.DocDate.Valid || !doc.Amount.Valid {
				totalUnmatched++
				continue
			}

			txns := h.loadTxnsForDirectDoc(db, doc, vendorNorm, aliases)

			var bestTx *plaidTx
			var bestScore scoreBreakdown
			for i := range txns {
				tx := &txns[i]
				if consumed[tx.ID] || !tx.Amount.Valid || !tx.TransactionDate.Valid {
					continue
				}
				s := scoreBreakdown{
					Amount:   scoreAmount(doc.Amount.Float64, math.Abs(tx.Amount.Float64)),
					Date:     scoreDate(doc.DocDate.Time, tx.TransactionDate.Time),
					Vendor:   25, // guaranteed by LIKE filter
					Location: scoreLocationMatch(doc, *tx),
				}
				s.Total = s.Amount + s.Date + s.Vendor + s.Location
				if s.Total > bestScore.Total {
					bestScore = s
					bestTx = tx
				}
			}

			if bestTx != nil && bestScore.Total >= matchThreshold {
				if err := h.recordMatch(db, doc, *bestTx, bestScore, "auto"); err != nil {
					allErrors = append(allErrors, err.Error())
					continue
				}
				consumed[bestTx.ID] = true
				totalMatched++
				batchMatches++
			} else {
				totalUnmatched++
			}
		}

		log.Printf("[MatchEngine] Direct batch %d: %d docs, found %d matches", batchNum, len(docs), batchMatches)

		offset += len(docs)
		if len(docs) < docBatchSize {
			break
		}
	}

	// ═══════════════════════════════════════════════════════════════
	// PHASE 3: UTILITY/DIRECT-PAY VENDORS — no completeness check
	// ═══════════════════════════════════════════════════════════════
	totalUtility := 0
	utilMatched, utilUnmatched, utilErrs := h.matchUtilityVendors(db, aliases, consumed)
	totalMatched += utilMatched
	totalUtility = utilMatched
	totalUnmatched += utilUnmatched
	allErrors = append(allErrors, utilErrs...)

	// ═══════════════════════════════════════════════════════════════
	// PHASE 4: CASCADE CHILD DOCUMENTS (invoices, RMAs, credits)
	// ═══════════════════════════════════════════════════════════════
	invCascaded, rmaCascaded, creditCascaded, cascadeErrs := h.cascadeChildDocuments(db)
	totalMatched += invCascaded + rmaCascaded + creditCascaded
	allErrors = append(allErrors, cascadeErrs...)

	log.Printf("[MatchEngine] Done: %d matched (stmt=%d, utility=%d, inv_cascade=%d, rma_cascade=%d, credit_cascade=%d), %d unmatched, %d errors",
		totalMatched, totalStmtGroup, totalUtility, invCascaded, rmaCascaded, creditCascaded, totalUnmatched, len(allErrors))

	c.JSON(http.StatusOK, gin.H{
		"processed":          totalMatched + totalUnmatched,
		"matched":            totalMatched,
		"unmatched":          totalUnmatched,
		"statement_grouped":  totalStmtGroup,
		"multi_pay_matched":  totalMultiPay,
		"utility_matched":    totalUtility,
		"invoices_cascaded":  invCascaded,
		"rmas_cascaded":      rmaCascaded,
		"credits_cascaded":   creditCascaded,
		"errors":             allErrors,
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

	for _, stmt := range stmts {
		vendorLabel := ""
		if stmt.VendorNorm.Valid {
			vendorLabel = stmt.VendorNorm.String
		}
		locationLabel := ""
		if stmt.LocationName.Valid {
			locationLabel = stmt.LocationName.String
		}

		// Step 2: Parse line_items JSONB and match to invoice docs by invoice number
		lineItems := h.parseStatementLineItems(db, stmt)

		var matchedInvIDs []string
		matchedInvTotal := 0.0
		for _, li := range lineItems {
			if li.InvoiceID == "" {
				continue
			}
			var invID string
			var invAmt sql.NullFloat64
			err := db.QueryRow(`
				SELECT id, amount FROM wf_documents
				WHERE vendor_id = $1
				  AND invoice_number = $2
				  AND doc_type IN ('invoice', 'credit', 'credit_memo')
				LIMIT 1`,
				stmt.VendorID.String, li.InvoiceID).Scan(&invID, &invAmt)
			if err == nil && invID != "" {
				matchedInvIDs = append(matchedInvIDs, invID)
				matchedInvTotal += math.Abs(li.Amount)
			}
		}

		// Fallback: if invoice number matching linked < 50% of line items
		if len(matchedInvIDs)*2 < len(lineItems) {
			fallbackIDs, fallbackTotal := h.fallbackInvoicesByDateRange(db, stmt)
			if len(fallbackIDs) > 0 {
				matchedInvIDs = append(matchedInvIDs, fallbackIDs...)
				matchedInvTotal += fallbackTotal
				log.Printf("[MatchEngine] Statement fallback: vendor=%s location=%s found %d invoices by date range total=%.2f",
					vendorLabel, locationLabel, len(fallbackIDs), fallbackTotal)
			}
		}

		// Determine the amount to use for Plaid transaction lookup:
		// a) statement.Amount if present and > 0
		// b) matchedInvTotal from invoice matches
		stmtAmount := 0.0
		if stmt.Amount.Valid && stmt.Amount.Float64 != 0 {
			stmtAmount = math.Abs(stmt.Amount.Float64)
		}
		lookupAmount := stmtAmount // preferred
		if lookupAmount == 0 {
			lookupAmount = matchedInvTotal // fallback
		}

		if lookupAmount == 0 && len(matchedInvIDs) == 0 {
			log.Printf("[MatchEngine] Stmt SKIPPED: no usable amount for vendor=%s", vendorLabel)
			unmatched++
			continue
		}

		// Step 5: Find matching Plaid transaction by vendor + date window
		vendorNorm := ""
		if stmt.VendorNorm.Valid {
			vendorNorm = stmt.VendorNorm.String
		}

		if stmt.DocDate.Valid {
			log.Printf("[MatchEngine] Stmt tx lookup: vendor=%s stmtAmt=%.2f matchedTotal=%.2f lookupAmt=%.2f window=%s to %s",
				vendorLabel, stmtAmount, matchedInvTotal, lookupAmount,
				stmt.DocDate.Time.Format("2006-01-02"),
				stmt.DocDate.Time.AddDate(0, 0, 16).Format("2006-01-02"))
		}

		txns := h.loadTxnsForStatement(db, stmt, vendorNorm, aliases)

		if len(txns) == 0 {
			log.Printf("[MatchEngine] Stmt NO TRANSACTIONS FOUND for vendor=%s", vendorLabel)
		} else {
			log.Printf("[MatchEngine] Stmt tx lookup result: found %d transactions", len(txns))
		}

		var bestTx *plaidTx
		var bestScore scoreBreakdown
		for i := range txns {
			tx := &txns[i]
			if consumed[tx.ID] || !tx.Amount.Valid || !tx.TransactionDate.Valid {
				continue
			}
			txAmt := math.Abs(tx.Amount.Float64)

			// Amount scoring: compare tx amount to lookupAmount
			s := scoreBreakdown{
				Amount:   scoreAmount(lookupAmount, txAmt),
				Vendor:   scoreVendorWithAliases(stmt, *tx, aliases),
				Location: scoreLocationMatch(stmt, *tx),
			}
			if stmt.DocDate.Valid {
				s.Date = scoreDate(stmt.DocDate.Time, tx.TransactionDate.Time)
			}
			s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location

			if s.Total > bestScore.Total {
				bestScore = s
				bestTx = tx
			}
		}

		if bestTx == nil || bestScore.Total < matchThreshold {
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

		log.Printf("[MatchEngine] Statement %s: MATCHED tx=%s score=%.1f, linked %d invoices",
			stmt.ID, bestTx.ID, bestScore.Total, len(matchedInvIDs))
	}
	return
}

// loadPendingStatements loads statement docs with vendor info.
func (h *MatchingEngineHandler) loadPendingStatements(db *sql.DB) ([]wfDoc, error) {
	// Statements can have NULL amounts — use relaxed WHERE (no amount filter)
	query := fmt.Sprintf(`SELECT %s
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE d.match_status = 'pending'
		  AND d.doc_date > '2020-01-01'
		  AND d.vendor_id IS NOT NULL
		  AND d.doc_type = 'statement'
		ORDER BY d.doc_date ASC`, docSelectCols)
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

// fallbackInvoicesByDateRange finds pending invoices/credits for the same
// vendor + location within the statement's date window (doc_date - 7 to doc_date).
func (h *MatchingEngineHandler) fallbackInvoicesByDateRange(db *sql.DB, stmt wfDoc) ([]string, float64) {
	if !stmt.DocDate.Valid || !stmt.VendorID.Valid {
		return nil, 0
	}
	dateTo := stmt.DocDate.Time.Format("2006-01-02")
	dateFrom := stmt.DocDate.Time.AddDate(0, 0, -7).Format("2006-01-02")

	// Build location filter — match if both have location, skip if stmt has none
	locFilter := ""
	var args []interface{}
	args = append(args, stmt.VendorID.String, dateFrom, dateTo) // $1, $2, $3
	if stmt.LocationName.Valid && stmt.LocationName.String != "" &&
		!strings.EqualFold(stmt.LocationName.String, "Unknown") {
		locFilter = "AND location_name = $4"
		args = append(args, stmt.LocationName.String)
	}

	query := fmt.Sprintf(`
		SELECT id, COALESCE(ABS(amount), 0) FROM wf_documents
		WHERE vendor_id = $1
		  AND doc_date BETWEEN $2 AND $3
		  AND doc_type IN ('invoice', 'credit', 'credit_memo')
		  AND match_status = 'pending'
		  AND matched_statement_id IS NULL
		  %s
		ORDER BY doc_date ASC`, locFilter)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("[MatchEngine] fallbackInvoicesByDateRange error: %v", err)
		return nil, 0
	}
	defer rows.Close()

	var ids []string
	total := 0.0
	for rows.Next() {
		var id string
		var amt float64
		if rows.Scan(&id, &amt) == nil {
			ids = append(ids, id)
			total += amt
		}
	}
	return ids, total
}

// ═══════════════════════════════════════════════════════════════
// STATEMENT PAGE GROUPING
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) groupStatementPages(db *sql.DB) []statementGroup {
	stmts, err := h.loadPendingStatements(db)
	if err != nil {
		log.Printf("[MatchEngine] groupStatementPages load error: %v", err)
		return nil
	}

	type gKey struct {
		VendorID, Location, Date string
	}
	groupMap := map[gKey]*statementGroup{}
	var groupOrder []gKey

	for _, stmt := range stmts {
		if !stmt.VendorID.Valid {
			continue
		}
		loc := ""
		if stmt.LocationName.Valid {
			loc = stmt.LocationName.String
		}
		dateStr := ""
		if stmt.DocDate.Valid {
			dateStr = stmt.DocDate.Time.Format("2006-01-02")
		}
		key := gKey{stmt.VendorID.String, loc, dateStr}

		g, exists := groupMap[key]
		if !exists {
			vendorName := ""
			if stmt.VendorNorm.Valid {
				vendorName = stmt.VendorNorm.String
			}
			docDate := time.Time{}
			if stmt.DocDate.Valid {
				docDate = stmt.DocDate.Time
			}
			g = &statementGroup{
				VendorID:     stmt.VendorID.String,
				VendorName:   vendorName,
				LocationName: loc,
				DocDate:      docDate,
			}
			groupMap[key] = g
			groupOrder = append(groupOrder, key)
		}
		g.Pages = append(g.Pages, stmt)

		if stmt.InvoiceNumber.Valid && stmt.InvoiceNumber.String != "" && g.CustomerNumber == "" {
			g.CustomerNumber = stmt.InvoiceNumber.String
		}

		items := h.parseStatementLineItems(db, stmt)
		g.AllLineItems = append(g.AllLineItems, items...)

		if stmt.Amount.Valid {
			g.DocumentTotal = stmt.Amount.Float64 // keep sign — credits are negative
			g.HasDocTotal = true
		}
	}

	var groups []statementGroup
	for _, key := range groupOrder {
		g := groupMap[key]

		for _, li := range g.AllLineItems {
			g.ComputedTotal += li.Amount // signed sum: credits are negative, invoices positive
		}
		g.ComputedTotal = math.Round(g.ComputedTotal*100) / 100

		if !g.HasDocTotal {
			g.Completeness = "no_total_found"
		} else if math.Abs(g.ComputedTotal-g.DocumentTotal) <= 0.01 {
			g.Completeness = "complete"
		} else if g.DocumentTotal == 0 && len(g.AllLineItems) > 0 {
			// Zero-balance statement (e.g., SSF): activity shown but net = $0
			// No Plaid transaction to match — mark complete so child linking works
			g.Completeness = "complete"
		} else {
			g.Completeness = "missing_pages"
		}

		// Resolve location from vendor_location_accounts
		if g.CustomerNumber != "" {
			var resolvedLoc sql.NullString
			db.QueryRow(`SELECT location_name FROM vendor_location_accounts
				WHERE vendor_id = $1 AND customer_number = $2`,
				g.VendorID, g.CustomerNumber).Scan(&resolvedLoc)
			if resolvedLoc.Valid && resolvedLoc.String != "" {
				g.LocationName = resolvedLoc.String
			}
		}

		// Record in statement_periods
		if !g.DocDate.IsZero() {
			periodEnd := g.DocDate.AddDate(0, 0, 7)
			h.upsertStatementPeriod(db, g.VendorID, g.LocationName,
				g.DocDate, periodEnd, g.DocumentTotal, g.ComputedTotal, g.Completeness)
			db.Exec(`UPDATE statement_periods SET completeness_status = $1,
				customer_number = $2, page_count = $3, computed_total = $4
				WHERE vendor_id = $5 AND location_name = $6 AND period_start = $7`,
				g.Completeness, g.CustomerNumber, len(g.Pages), g.ComputedTotal,
				g.VendorID, g.LocationName, g.DocDate)
		}

		groups = append(groups, *g)
	}
	return groups
}

// matchStatementGroups matches complete statement groups to Plaid transactions.
// Only groups with Completeness == "complete" are matched.
func (h *MatchingEngineHandler) matchStatementGroups(
	db *sql.DB, aliases map[string][]vendorAlias, consumed map[string]bool, groups []statementGroup,
) (matched int, unmatched int, errors []string) {

	for _, g := range groups {
		if g.Completeness != "complete" {
			continue
		}

		// Zero-balance statements: mark all pages as matched (no Plaid tx needed)
		if g.DocumentTotal == 0 && len(g.AllLineItems) > 0 {
			now := time.Now()
			for _, page := range g.Pages {
				db.Exec(`UPDATE wf_documents SET match_status = 'matched',
					match_confidence = 100, updated_at = $1
					WHERE id = $2 AND match_status = 'pending'`,
					now, page.ID)
			}
			matched += len(g.Pages)
			log.Printf("[MatchEngine] Zero-balance statement: vendor=%s location=%s date=%s, marked %d pages matched",
				g.VendorName, g.LocationName, g.DocDate.Format("2006-01-02"), len(g.Pages))
			continue
		}

		// Pick the page with the amount as the representative doc
		var repDoc wfDoc
		for _, p := range g.Pages {
			if p.Amount.Valid && p.Amount.Float64 != 0 {
				repDoc = p
				break
			}
		}
		if repDoc.ID == "" && len(g.Pages) > 0 {
			repDoc = g.Pages[0]
		}
		if repDoc.ID == "" {
			continue
		}

		// Override amount with computed_total from all pages
		repDoc.Amount = sql.NullFloat64{Float64: g.ComputedTotal, Valid: true}
		if g.LocationName != "" {
			repDoc.LocationName = sql.NullString{String: g.LocationName, Valid: true}
		}

		vendorLabel := g.VendorName
		locationLabel := g.LocationName
		lineItems := g.AllLineItems

		var matchedInvIDs []string
		matchedInvTotal := 0.0
		for _, li := range lineItems {
			if li.InvoiceID == "" {
				continue
			}
			var invID string
			var invAmt sql.NullFloat64
			err := db.QueryRow(`
				SELECT id, amount FROM wf_documents
				WHERE vendor_id = $1
				  AND invoice_number = $2
				  AND doc_type IN ('invoice', 'credit', 'credit_memo')
				LIMIT 1`,
				repDoc.VendorID.String, li.InvoiceID).Scan(&invID, &invAmt)
			if err == nil && invID != "" {
				matchedInvIDs = append(matchedInvIDs, invID)
				matchedInvTotal += math.Abs(li.Amount)
			}
		}

		// Fallback: if invoice number matching linked < 50% of line items
		if len(matchedInvIDs)*2 < len(lineItems) {
			fallbackIDs, fallbackTotal := h.fallbackInvoicesByDateRange(db, repDoc)
			if len(fallbackIDs) > 0 {
				matchedInvIDs = append(matchedInvIDs, fallbackIDs...)
				matchedInvTotal += fallbackTotal
				log.Printf("[MatchEngine] Statement group fallback: vendor=%s location=%s found %d invoices",
					vendorLabel, locationLabel, len(fallbackIDs))
			}
		}

		lookupAmount := math.Abs(g.ComputedTotal)
		if lookupAmount == 0 {
			lookupAmount = matchedInvTotal
		}
		if lookupAmount == 0 && len(matchedInvIDs) == 0 {
			log.Printf("[MatchEngine] Stmt group SKIPPED: no usable amount for vendor=%s", vendorLabel)
			unmatched++
			continue
		}

		vendorNorm := ""
		if repDoc.VendorNorm.Valid {
			vendorNorm = repDoc.VendorNorm.String
		}
		if repDoc.DocDate.Valid {
			log.Printf("[MatchEngine] Stmt group tx lookup: vendor=%s location=%s computed=%.2f lookupAmt=%.2f pages=%d",
				vendorLabel, locationLabel, g.ComputedTotal, lookupAmount, len(g.Pages))
		}

		txns := h.loadTxnsForStatement(db, repDoc, vendorNorm, aliases)

		var bestTx *plaidTx
		var bestScore scoreBreakdown
		for i := range txns {
			tx := &txns[i]
			if consumed[tx.ID] || !tx.Amount.Valid || !tx.TransactionDate.Valid {
				continue
			}
			txAmt := math.Abs(tx.Amount.Float64)
			s := scoreBreakdown{
				Amount:   scoreAmount(lookupAmount, txAmt),
				Vendor:   scoreVendorWithAliases(repDoc, *tx, aliases),
				Location: scoreLocationMatch(repDoc, *tx),
			}
			if repDoc.DocDate.Valid {
				s.Date = scoreDate(repDoc.DocDate.Time, tx.TransactionDate.Time)
			}
			s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location
			if s.Total > bestScore.Total {
				bestScore = s
				bestTx = tx
			}
		}

		if bestTx == nil || bestScore.Total < matchThreshold {
			unmatched++
			continue
		}

		consumed[bestTx.ID] = true
		now := time.Now()

		if err := h.recordMatch(db, repDoc, *bestTx, bestScore, "auto_statement_group"); err != nil {
			errors = append(errors, err.Error())
			unmatched++
			continue
		}
		matched++

		// Mark all other pages in the group as matched
		for _, page := range g.Pages {
			if page.ID == repDoc.ID {
				continue
			}
			db.Exec(`UPDATE wf_documents SET match_status = 'matched',
				matched_transaction_id = $1, matched_statement_id = $2,
				match_confidence = $3, updated_at = $4
				WHERE id = $5 AND match_status = 'pending'`,
				bestTx.ID, repDoc.ID, bestScore.Total, now, page.ID)
		}

		// Mark matched invoices as linked
		for _, invID := range matchedInvIDs {
			db.Exec(`UPDATE wf_documents SET match_status = 'matched',
				matched_transaction_id = $1, matched_statement_id = $2,
				match_confidence = $3, updated_at = $4
				WHERE id = $5 AND match_status = 'pending'`,
				bestTx.ID, repDoc.ID, bestScore.Total, now, invID)
			matched++
		}

		log.Printf("[MatchEngine] Statement group: MATCHED vendor=%s location=%s tx=%s score=%.1f, %d pages, linked %d invoices",
			vendorLabel, locationLabel, bestTx.ID, bestScore.Total, len(g.Pages), len(matchedInvIDs))
	}
	return
}

// ═══════════════════════════════════════════════════════════════
// PHASE 3: UTILITY / DIRECT-PAY VENDOR MATCHING
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) matchUtilityVendors(
	db *sql.DB, aliases map[string][]vendorAlias, consumed map[string]bool,
) (matched int, unmatched int, errors []string) {

	rows, err := db.Query(fmt.Sprintf(`SELECT %s
		FROM wf_documents d
		JOIN vendors v ON v.id = d.vendor_id
		WHERE d.match_status = 'pending'
		  AND d.doc_date > '2020-01-01'
		  AND d.amount IS NOT NULL
		  AND d.vendor_id IS NOT NULL
		  AND v.payment_type IN ('utility', 'subscription')
		ORDER BY d.doc_date ASC`, docSelectCols))
	if err != nil {
		log.Printf("[MatchEngine] utility vendor load error: %v", err)
		errors = append(errors, err.Error())
		return
	}
	defer rows.Close()
	var docs []wfDoc
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			errors = append(errors, err.Error())
			return
		}
		docs = append(docs, d)
	}
	log.Printf("[MatchEngine] Phase 3 utility vendors: %d pending docs", len(docs))

	for _, doc := range docs {
		if !doc.Amount.Valid || !doc.DocDate.Valid {
			unmatched++
			continue
		}
		vendorNorm := ""
		if doc.VendorNorm.Valid {
			vendorNorm = strings.ToLower(strings.TrimSpace(doc.VendorNorm.String))
		}
		if vendorNorm == "" {
			unmatched++
			continue
		}

		// Wider date window for utility: doc_date - 5 to doc_date + 30
		from := doc.DocDate.Time.AddDate(0, 0, -5).Format("2006-01-02")
		to := doc.DocDate.Time.AddDate(0, 0, 30).Format("2006-01-02")

		var patterns []string
		patterns = append(patterns, vendorNorm)
		if doc.VendorID.Valid {
			for _, a := range aliases[doc.VendorID.String] {
				patterns = append(patterns, a.Alias)
			}
		}

		var args []interface{}
		args = append(args, from, to)
		var likeConds []string
		for _, pat := range patterns {
			args = append(args, "%"+pat+"%")
			likeConds = append(likeConds, fmt.Sprintf("normalized_merchant LIKE $%d", len(args)))
		}
		vendorCond := "(" + strings.Join(likeConds, " OR ") + ")"

		query := fmt.Sprintf(`
			SELECT id, location_name, transaction_date, amount,
			       description, merchant_name, normalized_merchant
			FROM plaid_transactions
			WHERE match_status = 'pending'
			  AND transaction_date BETWEEN $1 AND $2
			  AND %s
			ORDER BY transaction_date ASC
			LIMIT 50`, vendorCond)

		txRows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("[MatchEngine] utility tx load error: %v", err)
			unmatched++
			continue
		}
		txns, _ := scanTxns(txRows)
		txRows.Close()

		var bestTx *plaidTx
		var bestScore scoreBreakdown
		for i := range txns {
			tx := &txns[i]
			if consumed[tx.ID] || !tx.Amount.Valid || !tx.TransactionDate.Valid {
				continue
			}
			// Utility scoring: amount within 2%, wider date window, vendor match
			docAmt := math.Abs(doc.Amount.Float64)
			txAmt := math.Abs(tx.Amount.Float64)
			var amtScore float64
			if docAmt > 0 {
				pctDiff := math.Abs(docAmt-txAmt) / docAmt
				if pctDiff < 0.001 {
					amtScore = 25
				} else if pctDiff <= 0.02 {
					amtScore = 20
				} else if pctDiff <= 0.05 {
					amtScore = 10
				}
			}

			s := scoreBreakdown{
				Amount:   amtScore,
				Date:     scoreDate(doc.DocDate.Time, tx.TransactionDate.Time),
				Vendor:   scoreVendorWithAliases(doc, *tx, aliases),
				Location: scoreLocationMatch(doc, *tx),
			}
			s.Total = s.Amount + s.Date + s.Vendor + s.Location
			if s.Total > bestScore.Total {
				bestScore = s
				bestTx = tx
			}
		}

		if bestTx != nil && bestScore.Total >= matchThreshold {
			if err := h.recordMatch(db, doc, *bestTx, bestScore, "auto_utility"); err != nil {
				errors = append(errors, err.Error())
				continue
			}
			consumed[bestTx.ID] = true
			matched++
		} else {
			unmatched++
		}
	}
	log.Printf("[MatchEngine] Phase 3 utility: %d matched, %d unmatched", matched, unmatched)
	return
}

// ═══════════════════════════════════════════════════════════════
// PHASE 4: CASCADE CHILD DOCUMENTS
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) cascadeChildDocuments(db *sql.DB) (invLinked, rmaLinked, creditLinked int, errors []string) {
	now := time.Now()
	digitRe := regexp.MustCompile(`[^0-9]`)

	// ── A. Invoice linking ──────────────────────────────────────
	stmtRows, err := db.Query(`
		SELECT id, vendor_id, location_name, matched_transaction_id, line_items, doc_date
		FROM wf_documents
		WHERE doc_type = 'statement'
		  AND match_status = 'matched'
		  AND line_items IS NOT NULL AND line_items::text NOT IN ('null', '[]', '')`)
	if err != nil {
		log.Printf("[Cascade] stmt query error: %v", err)
		errors = append(errors, err.Error())
		return
	}
	defer stmtRows.Close()

	type stmtInfo struct {
		id, vendorID, location, txID string
		docDate                       time.Time
		lineItems                     []stmtLineItem
	}
	var stmts []stmtInfo
	for stmtRows.Next() {
		var s stmtInfo
		var loc, txID sql.NullString
		var rawJSON sql.NullString
		var docDate sql.NullTime
		if err := stmtRows.Scan(&s.id, &s.vendorID, &loc, &txID, &rawJSON, &docDate); err != nil {
			continue
		}
		if txID.Valid {
			s.txID = txID.String
		}
		if loc.Valid {
			s.location = loc.String
		}
		if docDate.Valid {
			s.docDate = docDate.Time
		}
		if rawJSON.Valid && rawJSON.String != "" && rawJSON.String != "null" {
			json.Unmarshal([]byte(rawJSON.String), &s.lineItems)
		}
		if len(s.lineItems) > 0 {
			stmts = append(stmts, s)
		}
	}

	log.Printf("[Cascade] Processing %d matched statements for invoice linking", len(stmts))

	for _, stmt := range stmts {
		for _, li := range stmt.lineItems {
			if li.InvoiceID == "" {
				continue
			}

			var invID string

			// 1. Exact match on invoice_number
			db.QueryRow(`SELECT id FROM wf_documents
				WHERE doc_type IN ('invoice', 'credit', 'credit_memo')
				  AND vendor_id = $1 AND invoice_number = $2
				  AND match_status = 'pending'
				LIMIT 1`, stmt.vendorID, li.InvoiceID).Scan(&invID)

			// 2. Numeric suffix match (last 6 digits)
			if invID == "" {
				liDigits := digitRe.ReplaceAllString(li.InvoiceID, "")
				if len(liDigits) >= 6 {
					suffix := liDigits[len(liDigits)-6:]
					db.QueryRow(`SELECT id FROM wf_documents
						WHERE doc_type IN ('invoice', 'credit', 'credit_memo')
						  AND vendor_id = $1 AND match_status = 'pending'
						  AND RIGHT(REGEXP_REPLACE(invoice_number, '[^0-9]', '', 'g'), 6) = $2
						LIMIT 1`, stmt.vendorID, suffix).Scan(&invID)
				}
			}

			// 3. Date + amount proximity fallback
			if invID == "" && li.Amount != 0 {
				liDate := stmt.docDate
				if li.Date != "" {
					if t, err := time.Parse("2006-01-02T15:04:05.000Z", li.Date); err == nil {
						liDate = t
					} else if t, err := time.Parse("2006-01-02", li.Date[:10]); err == nil {
						liDate = t
					}
				}
				fromDate := liDate.AddDate(0, 0, -7).Format("2006-01-02")
				toDate := liDate.AddDate(0, 0, 7).Format("2006-01-02")
				absAmt := math.Abs(li.Amount)
				db.QueryRow(`SELECT id FROM wf_documents
					WHERE doc_type IN ('invoice', 'credit', 'credit_memo')
					  AND vendor_id = $1 AND match_status = 'pending'
					  AND doc_date BETWEEN $2 AND $3
					  AND ABS(ABS(amount) - $4) / GREATEST($4, 0.01) <= 0.01
					LIMIT 1`, stmt.vendorID, fromDate, toDate, absAmt).Scan(&invID)
			}

			confidence := 95.0
			txID := stmt.txID
			if invID != "" {
				db.Exec(`UPDATE wf_documents SET match_status = 'matched',
					matched_statement_id = $1, matched_transaction_id = $2,
					match_confidence = $3, updated_at = $4
					WHERE id = $5 AND match_status = 'pending'`,
					stmt.id, sql.NullString{String: txID, Valid: txID != ""}, confidence, now, invID)
				invLinked++
			}
		}
	}
	log.Printf("[Cascade] Invoices linked: %d", invLinked)

	// ── B. RMA linking — by invoice_number then by amount tiers ─

	// Tier 1: RMA invoice_number matches a matched invoice's invoice_number
	result, err := db.Exec(`
		UPDATE wf_documents rma
		SET match_status = 'matched',
		    matched_rma_id = inv.id,
		    matched_transaction_id = inv.matched_transaction_id,
		    match_confidence = 92,
		    updated_at = $1
		FROM wf_documents inv
		WHERE rma.doc_type = 'rma'
		  AND rma.match_status = 'pending'
		  AND rma.invoice_number IS NOT NULL AND rma.invoice_number != ''
		  AND inv.doc_type IN ('invoice', 'credit', 'credit_memo')
		  AND inv.match_status = 'matched'
		  AND inv.vendor_id = rma.vendor_id
		  AND inv.invoice_number = rma.invoice_number`, now)
	if err != nil {
		log.Printf("[Cascade] RMA tier1 error: %v", err)
		errors = append(errors, err.Error())
	} else {
		n, _ := result.RowsAffected()
		rmaLinked += int(n)
		log.Printf("[Cascade] RMA tier1 (invoice_number match): %d", n)
	}

	// Tier 2: Amount within 10%, same vendor+location, within 180 days
	result, err = db.Exec(`
		UPDATE wf_documents rma
		SET match_status = 'matched',
		    matched_rma_id = inv.id,
		    matched_transaction_id = inv.matched_transaction_id,
		    match_confidence = 80,
		    updated_at = $1
		FROM wf_documents inv
		WHERE rma.doc_type = 'rma'
		  AND rma.match_status = 'pending'
		  AND inv.doc_type IN ('invoice', 'credit', 'credit_memo')
		  AND inv.match_status = 'matched'
		  AND inv.vendor_id = rma.vendor_id
		  AND inv.location_name = rma.location_name
		  AND rma.doc_date >= inv.doc_date
		  AND rma.doc_date <= inv.doc_date + INTERVAL '180 days'
		  AND inv.amount IS NOT NULL AND ABS(inv.amount) > 0
		  AND ABS(ABS(rma.amount) - ABS(inv.amount)) / ABS(inv.amount) <= 0.10`, now)
	if err != nil {
		log.Printf("[Cascade] RMA tier2 error: %v", err)
		errors = append(errors, err.Error())
	} else {
		n, _ := result.RowsAffected()
		rmaLinked += int(n)
		log.Printf("[Cascade] RMA tier2 (amount within 10%%): %d", n)
	}

	// Tier 3: Same vendor+location, within 14 days, low confidence
	result, err = db.Exec(`
		UPDATE wf_documents rma
		SET match_status = 'matched',
		    matched_rma_id = inv.id,
		    matched_transaction_id = inv.matched_transaction_id,
		    match_confidence = 65,
		    updated_at = $1
		FROM wf_documents inv
		WHERE rma.doc_type = 'rma'
		  AND rma.match_status = 'pending'
		  AND inv.doc_type IN ('invoice', 'credit', 'credit_memo')
		  AND inv.match_status = 'matched'
		  AND inv.vendor_id = rma.vendor_id
		  AND inv.location_name = rma.location_name
		  AND rma.doc_date >= inv.doc_date
		  AND rma.doc_date <= inv.doc_date + INTERVAL '14 days'`, now)
	if err != nil {
		log.Printf("[Cascade] RMA tier3 error: %v", err)
		errors = append(errors, err.Error())
	} else {
		n, _ := result.RowsAffected()
		rmaLinked += int(n)
		log.Printf("[Cascade] RMA tier3 (date proximity): %d", n)
	}
	log.Printf("[Cascade] RMAs linked total: %d", rmaLinked)

	// ── C. Credit linking ───────────────────────────────────────

	// C1: Credit invoice_number matches a matched RMA or invoice
	result, err = db.Exec(`
		UPDATE wf_documents cred
		SET match_status = 'matched',
		    matched_credit_id = COALESCE(rma.id, inv.id),
		    matched_transaction_id = COALESCE(rma.matched_transaction_id, inv.matched_transaction_id),
		    match_confidence = 88,
		    updated_at = $1
		FROM wf_documents inv
		LEFT JOIN wf_documents rma ON (
			rma.doc_type = 'rma' AND rma.match_status = 'matched'
			AND rma.vendor_id = inv.vendor_id AND rma.invoice_number = inv.invoice_number
		)
		WHERE cred.doc_type IN ('credit', 'credit_memo')
		  AND cred.match_status = 'pending'
		  AND cred.invoice_number IS NOT NULL AND cred.invoice_number != ''
		  AND inv.doc_type IN ('invoice', 'credit', 'credit_memo', 'rma')
		  AND inv.match_status = 'matched'
		  AND inv.vendor_id = cred.vendor_id
		  AND inv.invoice_number = cred.invoice_number
		  AND inv.id != cred.id`, now)
	if err != nil {
		log.Printf("[Cascade] credit C1 error: %v", err)
		errors = append(errors, err.Error())
	} else {
		n, _ := result.RowsAffected()
		creditLinked += int(n)
		log.Printf("[Cascade] Credit C1 (invoice_number match): %d", n)
	}

	// C2: Credit matches a statement line_item directly (amount within 5%, same vendor+location)
	result, err = db.Exec(`
		UPDATE wf_documents cred
		SET match_status = 'matched',
		    matched_statement_id = stmt.id,
		    matched_transaction_id = stmt.matched_transaction_id,
		    match_confidence = 75,
		    updated_at = $1
		FROM wf_documents stmt
		WHERE cred.doc_type IN ('credit', 'credit_memo')
		  AND cred.match_status = 'pending'
		  AND stmt.doc_type = 'statement'
		  AND stmt.match_status = 'matched'
		  AND stmt.vendor_id = cred.vendor_id
		  AND cred.doc_date >= stmt.doc_date - INTERVAL '7 days'
		  AND cred.doc_date <= stmt.doc_date + INTERVAL '7 days'
		  AND EXISTS (
			SELECT 1 FROM jsonb_array_elements(stmt.line_items) li
			WHERE (li->>'amount')::numeric < 0
			  AND ABS(ABS((li->>'amount')::numeric) - ABS(cred.amount)) /
			      GREATEST(ABS(cred.amount), 0.01) <= 0.05
		  )`, now)
	if err != nil {
		log.Printf("[Cascade] credit C2 error: %v", err)
		errors = append(errors, err.Error())
	} else {
		n, _ := result.RowsAffected()
		creditLinked += int(n)
		log.Printf("[Cascade] Credit C2 (statement line_item match): %d", n)
	}
	log.Printf("[Cascade] Credits linked total: %d", creditLinked)

	return
}

// ═══════════════════════════════════════════════════════════════
// MISSING PAGES REVIEW & MANUAL MATCH
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) MissingPagesReview(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	groups := h.groupStatementPages(db)

	type reviewItem struct {
		VendorName     string  `json:"vendor_name"`
		LocationName   string  `json:"location_name"`
		CustomerNumber string  `json:"customer_number"`
		StatementDate  string  `json:"statement_date"`
		ComputedTotal  float64 `json:"computed_total"`
		DocumentTotal  float64 `json:"document_total"`
		Difference     float64 `json:"difference"`
		PageCount      int     `json:"page_count"`
		PageIDs        []string `json:"page_ids"`
	}

	var items []reviewItem
	for _, g := range groups {
		if g.Completeness != "missing_pages" {
			continue
		}
		dateStr := ""
		if !g.DocDate.IsZero() {
			dateStr = g.DocDate.Format("2006-01-02")
		}
		var ids []string
		for _, p := range g.Pages {
			ids = append(ids, p.ID)
		}
		items = append(items, reviewItem{
			VendorName:     g.VendorName,
			LocationName:   g.LocationName,
			CustomerNumber: g.CustomerNumber,
			StatementDate:  dateStr,
			ComputedTotal:  g.ComputedTotal,
			DocumentTotal:  g.DocumentTotal,
			Difference:     math.Abs(g.ComputedTotal - g.DocumentTotal),
			PageCount:      len(g.Pages),
			PageIDs:        ids,
		})
	}

	// Sort by difference descending
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[j].Difference > items[i].Difference {
				items[i], items[j] = items[j], items[i]
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"total":   len(items),
		"groups":  items,
	})
}

func (h *MatchingEngineHandler) ForceMatchStatement(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	docID := c.Param("group_id")
	var req struct {
		TransactionID string `json:"transaction_id"`
		Notes         string `json:"notes"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.TransactionID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "transaction_id is required"})
		return
	}

	// Verify the document exists
	var vendorID sql.NullString
	err := db.QueryRow(`SELECT vendor_id FROM wf_documents WHERE id = $1`, docID).Scan(&vendorID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	// Verify the transaction exists
	var txID string
	err = db.QueryRow(`SELECT id FROM plaid_transactions WHERE id = $1`, req.TransactionID).Scan(&txID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "transaction not found"})
		return
	}

	now := time.Now()

	// Insert match_results
	_, err = db.Exec(`
		INSERT INTO match_results (id, document_id, transaction_id,
			score_amount, score_date, score_vendor, score_invoice, score_location,
			total_score, passed_threshold, date_rule_violated, match_type, matched_at)
		VALUES (gen_random_uuid(), $1, $2, 0, 0, 0, 0, 0, 100, true, false, 'manual', $3)`,
		docID, req.TransactionID, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	// Mark the document as matched
	db.Exec(`UPDATE wf_documents SET match_status = 'matched', matched_transaction_id = $1,
		match_confidence = 100, updated_at = $2 WHERE id = $3`,
		req.TransactionID, now, docID)

	// Mark the transaction as matched
	db.Exec(`UPDATE plaid_transactions SET match_status = 'matched', matched_document_id = $1,
		match_confidence = 100 WHERE id = $2`,
		docID, req.TransactionID)

	// Mark all sibling pages (same vendor + location + date) as matched
	db.Exec(`UPDATE wf_documents SET match_status = 'matched',
		matched_transaction_id = $1, matched_statement_id = $2,
		match_confidence = 100, updated_at = $3
		WHERE id != $2 AND doc_type = 'statement' AND match_status = 'pending'
		  AND vendor_id = $4
		  AND location_name = (SELECT location_name FROM wf_documents WHERE id = $2)
		  AND doc_date = (SELECT doc_date FROM wf_documents WHERE id = $2)`,
		req.TransactionID, docID, now, vendorID.String)

	c.JSON(http.StatusOK, gin.H{
		"status":         "matched",
		"document_id":    docID,
		"transaction_id": req.TransactionID,
		"notes":          req.Notes,
	})
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
		JOIN vendors v ON v.id = d.vendor_id
		WHERE d.match_status = 'pending'
		  AND d.doc_date > '2020-01-01'
		  AND d.amount IS NOT NULL
		  AND d.vendor_id IS NOT NULL
		  AND v.vendor_type != 'statement'
		ORDER BY d.doc_date
		LIMIT $1 OFFSET $2`, docSelectCols)
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

func (h *MatchingEngineHandler) loadTxnsForDirectDoc(db *sql.DB, doc wfDoc, vendorNorm string, aliases map[string][]vendorAlias) []plaidTx {
	if !doc.DocDate.Valid {
		return nil
	}
	from := doc.DocDate.Time.AddDate(0, 0, -15).Format("2006-01-02")
	to := doc.DocDate.Time.AddDate(0, 0, 15).Format("2006-01-02")
	docAmt := 0.0
	if doc.Amount.Valid {
		docAmt = math.Abs(doc.Amount.Float64)
	}

	// Build vendor name patterns: primary name + aliases
	var patterns []string
	patterns = append(patterns, vendorNorm)
	if doc.VendorID.Valid {
		for _, a := range aliases[doc.VendorID.String] {
			patterns = append(patterns, a.Alias)
		}
	}

	var args []interface{}
	args = append(args, from, to)
	var likeConds []string
	for _, pat := range patterns {
		args = append(args, "%"+pat+"%")
		likeConds = append(likeConds, fmt.Sprintf("normalized_merchant LIKE $%d", len(args)))
	}
	vendorCond := "(" + strings.Join(likeConds, " OR ") + ")"

	args = append(args, docAmt)
	amtIdx := len(args)

	query := fmt.Sprintf(`
		SELECT id, location_name, transaction_date, amount,
		       description, merchant_name, normalized_merchant
		FROM plaid_transactions
		WHERE match_status = 'pending'
		  AND %s
		  AND transaction_date BETWEEN $1 AND $2
		ORDER BY ABS(amount - $%d)
		LIMIT 20`, vendorCond, amtIdx)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("[MatchEngine] loadTxnsForDirectDoc error: %v", err)
		return nil
	}
	defer rows.Close()
	txns, _ := scanTxns(rows)
	return txns
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

// ═══════════════════════════════════════════════════════════════
// STATEMENT COMPLETENESS DIAGNOSTIC
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) StatementCompleteness(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	// Ensure tables exist
	db.Exec(`CREATE TABLE IF NOT EXISTS vendor_location_accounts (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		vendor_id UUID NOT NULL REFERENCES vendors(id),
		customer_number VARCHAR(50) NOT NULL,
		location_name VARCHAR(100) NOT NULL,
		notes VARCHAR(255),
		created_at TIMESTAMP DEFAULT NOW(),
		UNIQUE(vendor_id, customer_number)
	)`)

	groups := h.groupStatementPages(db)

	totalGroups := len(groups)
	complete := 0
	missing := 0
	noTotal := 0

	type vendorStats struct {
		Total        int `json:"total"`
		Complete     int `json:"complete"`
		MissingPages int `json:"missing_pages"`
		NoTotalFound int `json:"no_total_found"`
	}
	byVendor := map[string]*vendorStats{}

	type missingDetail struct {
		VendorName     string  `json:"vendor_name"`
		CustomerNumber string  `json:"customer_number"`
		LocationName   string  `json:"location_name"`
		StatementDate  string  `json:"statement_date"`
		ComputedTotal  float64 `json:"computed_total"`
		DocumentTotal  float64 `json:"document_total"`
		PageCount      int     `json:"page_count"`
	}
	var missingDetails []missingDetail

	for _, g := range groups {
		vn := g.VendorName
		if vn == "" {
			vn = "unknown"
		}
		vs, ok := byVendor[vn]
		if !ok {
			vs = &vendorStats{}
			byVendor[vn] = vs
		}
		vs.Total++

		switch g.Completeness {
		case "complete":
			complete++
			vs.Complete++
		case "missing_pages":
			missing++
			vs.MissingPages++
			dateStr := ""
			if !g.DocDate.IsZero() {
				dateStr = g.DocDate.Format("2006-01-02")
			}
			missingDetails = append(missingDetails, missingDetail{
				VendorName:     g.VendorName,
				CustomerNumber: g.CustomerNumber,
				LocationName:   g.LocationName,
				StatementDate:  dateStr,
				ComputedTotal:  g.ComputedTotal,
				DocumentTotal:  g.DocumentTotal,
				PageCount:      len(g.Pages),
			})
		case "no_total_found":
			noTotal++
			vs.NoTotalFound++
		}
	}

	if missingDetails == nil {
		missingDetails = []missingDetail{}
	}

	c.JSON(http.StatusOK, gin.H{
		"total_groups":         totalGroups,
		"complete":             complete,
		"missing_pages":        missing,
		"no_total_found":       noTotal,
		"by_vendor":            byVendor,
		"missing_pages_detail": missingDetails,
	})
}

// ═══════════════════════════════════════════════════════════════
// VENDOR LOCATION ACCOUNTS
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) CreateVendorLocationAccount(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	var req struct {
		VendorName     string `json:"vendor_name"`
		CustomerNumber string `json:"customer_number"`
		LocationName   string `json:"location_name"`
		Notes          string `json:"notes"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	var vendorID string
	err := db.QueryRow(`SELECT id FROM vendors WHERE normalized_name = LOWER(TRIM($1))`, req.VendorName).Scan(&vendorID)
	if err != nil {
		err = db.QueryRow(`SELECT id FROM vendors WHERE LOWER(normalized_name) LIKE LOWER($1) LIMIT 1`,
			"%"+strings.TrimSpace(req.VendorName)+"%").Scan(&vendorID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("vendor '%s' not found", req.VendorName)})
			return
		}
	}

	_, err = db.Exec(`INSERT INTO vendor_location_accounts (id, vendor_id, customer_number, location_name, notes)
		VALUES (gen_random_uuid(), $1, $2, $3, $4)
		ON CONFLICT (vendor_id, customer_number) DO UPDATE SET
			location_name = EXCLUDED.location_name, notes = EXCLUDED.notes`,
		vendorID, req.CustomerNumber, req.LocationName, req.Notes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":          "created",
		"vendor_id":       vendorID,
		"customer_number": req.CustomerNumber,
		"location_name":   req.LocationName,
	})
}

func (h *MatchingEngineHandler) ListVendorLocationAccounts(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database unavailable"})
		return
	}

	rows, err := db.Query(`
		SELECT vla.id, v.normalized_name, vla.customer_number,
		       vla.location_name, COALESCE(vla.notes, ''), vla.created_at
		FROM vendor_location_accounts vla
		JOIN vendors v ON v.id = vla.vendor_id
		ORDER BY v.normalized_name, vla.customer_number`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	defer rows.Close()

	type account struct {
		ID             string `json:"id"`
		VendorName     string `json:"vendor_name"`
		CustomerNumber string `json:"customer_number"`
		LocationName   string `json:"location_name"`
		Notes          string `json:"notes"`
		CreatedAt      string `json:"created_at"`
	}

	byVendor := map[string][]account{}
	for rows.Next() {
		var a account
		var createdAt time.Time
		if err := rows.Scan(&a.ID, &a.VendorName, &a.CustomerNumber,
			&a.LocationName, &a.Notes, &createdAt); err != nil {
			continue
		}
		a.CreatedAt = createdAt.Format(time.RFC3339)
		byVendor[a.VendorName] = append(byVendor[a.VendorName], a)
	}

	c.JSON(http.StatusOK, gin.H{"accounts": byVendor})
}
