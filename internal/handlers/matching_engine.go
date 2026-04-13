package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"runtime/debug"
	"sort"
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

// vendorAlias stores an alias with optional location scoping.
type vendorAlias struct {
	Alias        string
	LocationName string // empty = global alias
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

	// ── RSR Auto Parts alias for Carquest at Highlands ──────────
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

	// ── Log diagnostics ─────────────────────────────────────────
	h.logTableColumns(db, "wf_documents")
	h.logTableColumns(db, "vendors")

	var linkedCount, unlinkedCount int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE vendor_id IS NOT NULL`).Scan(&linkedCount)
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE vendor_id IS NULL`).Scan(&unlinkedCount)
	log.Printf("[MatchEngine] Vendor coverage: %d linked, %d unlinked", linkedCount, unlinkedCount)

	// ── Load aliases (small, load once) ─────────────────────────
	aliases := h.loadVendorAliases(db)

	// ── Count total pending docs for progress ───────────────────
	var totalPending int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents
		WHERE match_status = 'pending' AND doc_date > '2020-01-01'
		AND vendor_id IS NOT NULL AND amount IS NOT NULL`).Scan(&totalPending)

	totalBatches := (totalPending + docBatchSize - 1) / docBatchSize
	if totalBatches == 0 {
		totalBatches = 1
	}
	log.Printf("[MatchEngine] Total pending docs: %d (%d batches of %d)", totalPending, totalBatches, docBatchSize)

	// ── Global consumed set (transaction IDs matched across all batches) ──
	consumed := map[string]bool{}
	totalMatched := 0
	totalUnmatched := 0
	totalStmtGroup := 0
	totalMultiPay := 0
	var allErrors []string

	// ── Process statement vendors first (group by vendor_id) ────
	stmtDocs, err := h.loadStatementVendorDocs(db)
	if err != nil {
		log.Printf("[MatchEngine] load statement docs error: %v", err)
	} else if len(stmtDocs) > 0 {
		stmtWeeklyGroups := map[string][]wfDoc{}
		multiPayGroups := map[string][]wfDoc{}

		for _, doc := range stmtDocs {
			key := ""
			if doc.VendorID.Valid {
				key = doc.VendorID.String
			}
			if isMultiPaymentVendor(doc) {
				multiPayGroups[key] = append(multiPayGroups[key], doc)
			} else {
				stmtWeeklyGroups[key] = append(stmtWeeklyGroups[key], doc)
			}
		}

		log.Printf("[MatchEngine] Statement vendors: %d weekly (%d vendors), %d multi-pay (%d vendors)",
			len(stmtDocs)-func() int { n := 0; for _, g := range multiPayGroups { n += len(g) }; return n }(),
			len(stmtWeeklyGroups),
			func() int { n := 0; for _, g := range multiPayGroups { n += len(g) }; return n }(),
			len(multiPayGroups))

		// Process each statement vendor with its own scoped transactions
		for vendorID, vendorDocs := range stmtWeeklyGroups {
			txns := h.loadTxnsForVendor(db, vendorDocs, vendorID, aliases)
			weekBuckets := groupByWeek(vendorDocs)
			for _, bucket := range weekBuckets {
				weekSum := 0.0
				for _, d := range bucket.docs {
					if d.Amount.Valid {
						weekSum += math.Abs(d.Amount.Float64)
					}
				}
				if weekSum == 0 {
					continue
				}
				bestTx, bestScore := h.findStatementMatch(vendorID, bucket.docs, weekSum, bucket.periodEnd, txns, consumed, aliases)
				if bestTx != nil && bestScore.Total >= matchThreshold {
					for _, d := range bucket.docs {
						perDocScore := scoreBreakdown{
							Amount: bestScore.Amount, Date: bestScore.Date,
							Vendor: bestScore.Vendor, Location: bestScore.Location,
							Total: bestScore.Total,
						}
						if err := h.recordMatch(db, d, *bestTx, perDocScore, "auto_statement_group"); err != nil {
							allErrors = append(allErrors, err.Error())
						} else {
							totalMatched++
							totalStmtGroup++
						}
					}
					consumed[bestTx.ID] = true
					h.upsertStatementPeriod(db, vendorID, bucket.periodStart, bucket.periodEnd, weekSum, weekSum)
				} else {
					totalUnmatched += len(bucket.docs)
				}
			}
		}

		for vendorID, vendorDocs := range multiPayGroups {
			txns := h.loadTxnsForVendor(db, vendorDocs, vendorID, aliases)
			weekBuckets := groupByWeek(vendorDocs)
			for _, bucket := range weekBuckets {
				periodTotal := 0.0
				for _, d := range bucket.docs {
					if d.Amount.Valid {
						periodTotal += math.Abs(d.Amount.Float64)
					}
				}
				m, u, errs := h.matchMultiPaymentPeriod(db, vendorID, bucket, txns, consumed, aliases)
				totalMatched += m
				totalMultiPay += m
				totalUnmatched += u
				allErrors = append(allErrors, errs...)

				matchedAmt := 0.0
				for _, d := range bucket.docs {
					if d.Amount.Valid {
						var ms sql.NullString
						db.QueryRow(`SELECT match_status FROM wf_documents WHERE id = $1`, d.ID).Scan(&ms)
						if ms.Valid && ms.String == "matched" {
							matchedAmt += math.Abs(d.Amount.Float64)
						}
					}
				}
				h.upsertStatementPeriod(db, vendorID, bucket.periodStart, bucket.periodEnd, periodTotal, matchedAmt)
			}
		}

		stmtDocs = nil // free memory
	}

	// ── Process direct/COD vendors in batches ───────────────────
	batchNum := 0
	offset := 0
	for {
		batchNum++
		docs, err := h.loadDocBatch(db, docBatchSize, offset)
		if err != nil {
			log.Printf("[MatchEngine] batch %d load docs error: %v", batchNum, err)
			allErrors = append(allErrors, err.Error())
			break
		}
		if len(docs) == 0 {
			break
		}

		// Determine date range for this batch
		minDate, maxDate := batchDateRange(docs)

		// Load transactions scoped to this batch's date range
		txns, err := h.loadTxnsForDateRange(db, minDate, maxDate)
		if err != nil {
			log.Printf("[MatchEngine] batch %d load txns error: %v", batchNum, err)
			allErrors = append(allErrors, err.Error())
			break
		}

		log.Printf("[MatchEngine] Batch %d/%d: processing docs %d-%d, loaded %d transactions",
			batchNum, totalBatches, offset+1, offset+len(docs), len(txns))

		for _, doc := range docs {
			// Skip statement/multi-pay docs (already processed above)
			if isStatementWeekly(doc) || isMultiPaymentVendor(doc) {
				continue
			}

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
			break // last batch
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

// ── Batched data loaders ────────────────────────────────────────

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

// loadDocBatch loads a batch of direct/COD docs (non-statement) with LIMIT/OFFSET.
func (h *MatchingEngineHandler) loadDocBatch(db *sql.DB, limit, offset int) ([]wfDoc, error) {
	query := fmt.Sprintf(`SELECT %s
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE %s
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

// loadStatementVendorDocs loads all pending docs for statement/multi-pay vendors.
func (h *MatchingEngineHandler) loadStatementVendorDocs(db *sql.DB) ([]wfDoc, error) {
	query := fmt.Sprintf(`SELECT %s
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE %s
		  AND (v.vendor_type = 'statement' OR COALESCE(v.multi_payment, false) = true
		       OR LOWER(v.statement_frequency) = 'weekly')
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

// loadTxnsForDateRange loads pending transactions within a date window.
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

// loadTxnsForVendor loads pending transactions scoped to a vendor's date range and name.
func (h *MatchingEngineHandler) loadTxnsForVendor(db *sql.DB, docs []wfDoc, vendorID string, aliases map[string][]vendorAlias) []plaidTx {
	if len(docs) == 0 {
		return nil
	}

	minDate, maxDate := batchDateRange(docs)
	from := minDate.AddDate(0, 0, -30).Format("2006-01-02")
	to := maxDate.AddDate(0, 0, 30).Format("2006-01-02")

	// Build vendor name patterns for WHERE clause
	var patterns []string
	for _, d := range docs {
		if d.VendorNorm.Valid && d.VendorNorm.String != "" {
			vn := strings.ToLower(strings.TrimSpace(d.VendorNorm.String))
			patterns = append(patterns, vn)
			break // all docs should share same vendor
		}
	}
	for _, a := range aliases[vendorID] {
		patterns = append(patterns, a.Alias)
	}

	if len(patterns) == 0 {
		// No vendor name — load by date only
		rows, err := db.Query(`
			SELECT id, location_name, transaction_date, amount,
			       description, merchant_name, normalized_merchant
			FROM plaid_transactions
			WHERE match_status = 'pending'
			  AND transaction_date BETWEEN $1 AND $2
			ORDER BY transaction_date ASC
			LIMIT $3`, from, to, txBatchLimit)
		if err != nil {
			log.Printf("[MatchEngine] loadTxnsForVendor query error: %v", err)
			return nil
		}
		defer rows.Close()
		txns, _ := scanTxns(rows)
		return txns
	}

	// Build LIKE conditions for vendor matching
	var conds []string
	var args []interface{}
	args = append(args, from, to) // $1, $2
	for i, p := range patterns {
		argIdx := i + 3 // $3, $4, ...
		conds = append(conds, fmt.Sprintf("normalized_merchant LIKE $%d", argIdx))
		args = append(args, "%"+p+"%")
	}
	args = append(args, txBatchLimit) // last arg
	limitIdx := len(args)

	query := fmt.Sprintf(`
		SELECT id, location_name, transaction_date, amount,
		       description, merchant_name, normalized_merchant
		FROM plaid_transactions
		WHERE match_status = 'pending'
		  AND transaction_date BETWEEN $1 AND $2
		  AND (%s)
		ORDER BY transaction_date ASC
		LIMIT $%d`, strings.Join(conds, " OR "), limitIdx)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("[MatchEngine] loadTxnsForVendor filtered query error: %v — falling back to date-only", err)
		// Fallback: load by date only
		rows2, err2 := db.Query(`
			SELECT id, location_name, transaction_date, amount,
			       description, merchant_name, normalized_merchant
			FROM plaid_transactions
			WHERE match_status = 'pending'
			  AND transaction_date BETWEEN $1 AND $2
			ORDER BY transaction_date ASC
			LIMIT $3`, from, to, txBatchLimit)
		if err2 != nil {
			return nil
		}
		defer rows2.Close()
		txns, _ := scanTxns(rows2)
		return txns
	}
	defer rows.Close()
	txns, _ := scanTxns(rows)
	return txns
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

// ── Chronological pre-filters ───────────────────────────────────

func passesDateRule(doc wfDoc, tx plaidTx, isCOD bool, dateWindow int) bool {
	if !doc.DocDate.Valid || !tx.TransactionDate.Valid {
		return true
	}
	docDate := doc.DocDate.Time
	txDate := tx.TransactionDate.Time

	docType := ""
	if doc.DocType.Valid {
		docType = strings.ToUpper(doc.DocType.String)
	}

	if isCOD {
		return sameDay(docDate, txDate)
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
	case "RMA":
		return true
	}
	return true
}

func sameDay(a, b time.Time) bool {
	return a.Year() == b.Year() && a.YearDay() == b.YearDay()
}

// ── Scoring functions ───────────────────────────────────────────

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

// ── Direct vendor matching ──────────────────────────────────────

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

// ── Statement weekly vendor matching ────────────────────────────

type weekBucket struct {
	periodStart time.Time
	periodEnd   time.Time
	docs        []wfDoc
}

func isMultiPaymentVendor(doc wfDoc) bool {
	return doc.MultiPayment.Valid && doc.MultiPayment.Bool
}

func isStatementWeekly(doc wfDoc) bool {
	if !doc.StmtFreq.Valid || doc.StmtFreq.String == "" {
		return false
	}
	freq := strings.ToLower(strings.TrimSpace(doc.StmtFreq.String))
	if freq != "weekly" {
		return false
	}
	if doc.IsStatementVendor.Valid && doc.IsStatementVendor.Bool {
		return true
	}
	return true // weekly frequency alone implies statement vendor
}

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

func groupByWeek(docs []wfDoc) []weekBucket {
	buckets := map[string]*weekBucket{}
	for _, d := range docs {
		if !d.DocDate.Valid {
			continue
		}
		monday := weekStart(d.DocDate.Time)
		key := monday.Format("2006-01-02")
		b, ok := buckets[key]
		if !ok {
			sunday := monday.AddDate(0, 0, 6)
			b = &weekBucket{periodStart: monday, periodEnd: sunday}
			buckets[key] = b
		}
		b.docs = append(b.docs, d)
	}
	result := make([]weekBucket, 0, len(buckets))
	for _, b := range buckets {
		result = append(result, *b)
	}
	return result
}

func weekStart(t time.Time) time.Time {
	wd := int(t.Weekday())
	if wd == 0 {
		wd = 7
	}
	return t.AddDate(0, 0, -(wd - 1))
}

func (h *MatchingEngineHandler) findStatementMatch(
	vendorID string, docs []wfDoc, weekSum float64,
	periodEnd time.Time, txns []plaidTx, consumed map[string]bool,
	aliases map[string][]vendorAlias,
) (*plaidTx, scoreBreakdown) {
	rep := docs[0]
	var bestTx *plaidTx
	var bestScore scoreBreakdown

	for i := range txns {
		tx := &txns[i]
		if consumed[tx.ID] || !tx.TransactionDate.Valid || !tx.Amount.Valid {
			continue
		}
		txDate := tx.TransactionDate.Time
		if txDate.Before(periodEnd) || txDate.After(periodEnd.AddDate(0, 0, 7)) {
			continue
		}
		txAmt := math.Abs(tx.Amount.Float64)
		if weekSum > 0 {
			if math.Abs(txAmt-weekSum)/weekSum > 0.01 {
				continue
			}
		}

		s := scoreBreakdown{
			Amount:   scoreAmount(weekSum, txAmt),
			Date:     scoreDate(periodEnd, txDate),
			Vendor:   scoreVendorWithAliases(rep, *tx, aliases),
			Location: scoreLocationMatch(rep, *tx),
		}
		s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location

		if s.Total > bestScore.Total {
			bestScore = s
			bestTx = tx
		}
	}
	return bestTx, bestScore
}

// ── Multi-payment / rolling statement matching (AmEx pattern) ───

func (h *MatchingEngineHandler) matchMultiPaymentPeriod(
	db *sql.DB, vendorID string, bucket weekBucket,
	txns []plaidTx, consumed map[string]bool, aliases map[string][]vendorAlias,
) (matched int, unmatched int, errors []string) {
	if len(bucket.docs) == 0 {
		return
	}
	rep := bucket.docs[0]

	windowStart := bucket.periodStart
	windowEnd := bucket.periodEnd.AddDate(0, 0, 16)

	var candidates []plaidTx
	for i := range txns {
		tx := &txns[i]
		if consumed[tx.ID] || !tx.TransactionDate.Valid || !tx.Amount.Valid {
			continue
		}
		txDate := tx.TransactionDate.Time
		if txDate.Before(windowStart) || txDate.After(windowEnd) {
			continue
		}
		if scoreVendorWithAliases(rep, *tx, aliases) < 25 {
			continue
		}
		candidates = append(candidates, *tx)
	}

	sortedDocs := make([]wfDoc, len(bucket.docs))
	copy(sortedDocs, bucket.docs)
	sort.Slice(sortedDocs, func(i, j int) bool {
		ai, aj := 0.0, 0.0
		if sortedDocs[i].Amount.Valid {
			ai = math.Abs(sortedDocs[i].Amount.Float64)
		}
		if sortedDocs[j].Amount.Valid {
			aj = math.Abs(sortedDocs[j].Amount.Float64)
		}
		return ai > aj
	})

	for _, doc := range sortedDocs {
		if !doc.Amount.Valid || doc.Amount.Float64 == 0 {
			unmatched++
			continue
		}
		docAmt := math.Abs(doc.Amount.Float64)

		bestIdx := -1
		var bestScore scoreBreakdown
		for ci, cand := range candidates {
			if consumed[cand.ID] {
				continue
			}
			txAmt := math.Abs(cand.Amount.Float64)
			amtPctDiff := math.Abs(docAmt-txAmt) / docAmt
			if amtPctDiff > 0.01 {
				continue
			}
			s := computeFullScore(doc, cand, aliases)
			if s.Total > bestScore.Total {
				bestScore = s
				bestIdx = ci
			}
		}

		if bestIdx >= 0 && bestScore.Total >= matchThreshold {
			cand := candidates[bestIdx]
			if err := h.recordMatch(db, doc, cand, bestScore, "auto_multi_pay"); err != nil {
				errors = append(errors, err.Error())
			} else {
				matched++
				consumed[cand.ID] = true
			}
		} else {
			matchedBySumming := h.tryPartialSumMatch(db, doc, docAmt, candidates, consumed, aliases)
			if matchedBySumming {
				matched++
			} else {
				unmatched++
			}
		}
	}
	return
}

func (h *MatchingEngineHandler) tryPartialSumMatch(
	db *sql.DB, doc wfDoc, docAmt float64,
	candidates []plaidTx, consumed map[string]bool,
	aliases map[string][]vendorAlias,
) bool {
	for _, cand := range candidates {
		if consumed[cand.ID] || !cand.Amount.Valid {
			continue
		}
		txAmt := math.Abs(cand.Amount.Float64)
		if docAmt > 0 && math.Abs(txAmt-docAmt)/docAmt <= 0.01 {
			s := computeFullScore(doc, cand, aliases)
			if s.Total >= matchThreshold {
				if err := h.recordMatch(db, doc, cand, s, "auto_multi_pay_partial"); err != nil {
					return false
				}
				consumed[cand.ID] = true
				return true
			}
		}
	}
	return false
}

// ── statement_periods tracking ──────────────────────────────────

func (h *MatchingEngineHandler) upsertStatementPeriod(db *sql.DB, vendorID string, periodStart, periodEnd time.Time, expectedTotal, matchedTotal float64) {
	if vendorID == "" {
		return
	}
	_, err := db.Exec(`
		INSERT INTO statement_periods (id, vendor_id, period_start, period_end, expected_total, matched_total, status, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5,
			CASE WHEN $5 >= $4 * 0.98 THEN 'fully_matched' ELSE 'partial' END, NOW())
		ON CONFLICT (vendor_id, period_start) DO UPDATE SET
			expected_total = EXCLUDED.expected_total,
			matched_total = EXCLUDED.matched_total,
			status = CASE WHEN EXCLUDED.matched_total >= EXCLUDED.expected_total * 0.98 THEN 'fully_matched' ELSE 'partial' END`,
		vendorID, periodStart, periodEnd, expectedTotal, matchedTotal)
	if err != nil {
		log.Printf("[MatchEngine] upsert statement_period error: %v", err)
		db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stmt_periods_vendor_start ON statement_periods (vendor_id, period_start)`)
		db.Exec(`
			INSERT INTO statement_periods (id, vendor_id, period_start, period_end, expected_total, matched_total, status, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, $4, $5,
				CASE WHEN $5 >= $4 * 0.98 THEN 'fully_matched' ELSE 'partial' END, NOW())
			ON CONFLICT (vendor_id, period_start) DO UPDATE SET
				expected_total = EXCLUDED.expected_total,
				matched_total = EXCLUDED.matched_total,
				status = CASE WHEN EXCLUDED.matched_total >= EXCLUDED.expected_total * 0.98 THEN 'fully_matched' ELSE 'partial' END`,
			vendorID, periodStart, periodEnd, expectedTotal, matchedTotal)
	}
}

// ── Record match ────────────────────────────────────────────────

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
