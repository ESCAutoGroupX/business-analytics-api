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

type vendorAlias struct {
	Alias        string
	LocationName string
}

// statementPeriod groups docs by vendor + location + week with a calculated total.
type statementPeriod struct {
	VendorID    string
	VendorNorm  string
	Location    string
	PeriodStart time.Time
	PeriodEnd   time.Time
	InvTotal    float64 // sum of invoice amounts
	CreditTotal float64 // sum of credit amounts
	NetTotal    float64 // inv - credit
	StmtAmount  float64 // from an actual statement doc if one exists, else 0
	Docs        []wfDoc // all docs in this period
	RepDoc      wfDoc   // representative doc for vendor/location scoring
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
	// PHASE 1: STATEMENT VENDORS — amount-first matching
	// ═══════════════════════════════════════════════════════════════
	stmtDocs, err := h.loadStatementVendorDocs(db)
	if err != nil {
		log.Printf("[MatchEngine] load statement docs error: %v", err)
	} else if len(stmtDocs) > 0 {
		// Step 1: Build period totals grouped by vendor + location + week
		periods := buildStatementPeriods(stmtDocs)
		log.Printf("[MatchEngine] Statement vendors: %d docs → %d periods", len(stmtDocs), len(periods))

		// Log first few periods as samples
		for i, sp := range periods {
			if i >= 3 {
				break
			}
			total := sp.NetTotal
			if sp.StmtAmount > 0 {
				total = sp.StmtAmount
			}
			log.Printf("[MatchEngine] Sample period: vendor=%s location=%s period=%s to %s invTotal=%.2f creditTotal=%.2f net=%.2f stmtAmt=%.2f expected=%.2f docs=%d",
				sp.VendorNorm, sp.Location,
				sp.PeriodStart.Format("2006-01-02"), sp.PeriodEnd.Format("2006-01-02"),
				sp.InvTotal, sp.CreditTotal, sp.NetTotal, sp.StmtAmount, total, len(sp.Docs))
		}

		for i := range periods {
			p := &periods[i]
			periodTotal := p.NetTotal
			if p.StmtAmount > 0 {
				periodTotal = p.StmtAmount // prefer actual statement amount
			}
			if periodTotal == 0 {
				totalUnmatched += len(p.Docs)
				continue
			}

			isMultiPay := p.RepDoc.MultiPayment.Valid && p.RepDoc.MultiPayment.Bool

			// Step 2: Find transactions by amount (amount-first lookup)
			if i < 3 {
				windowStart := p.PeriodStart.AddDate(0, 0, -3)
				windowEnd := p.PeriodEnd.AddDate(0, 0, 16)
				log.Printf("[MatchEngine] Looking for txns: vendor=%s amount=%.2f window=%s to %s (periodStart=%s periodEnd=%s)",
					p.VendorNorm, periodTotal,
					windowStart.Format("2006-01-02"), windowEnd.Format("2006-01-02"),
					p.PeriodStart.Format("2006-01-02"), p.PeriodEnd.Format("2006-01-02"))
			}
			txns := h.loadTxnsByAmount(db, periodTotal, p.PeriodStart, p.PeriodEnd, p.VendorNorm, aliases, p.RepDoc)
			if i < 3 {
				log.Printf("[MatchEngine] Found %d candidate txns for period (vendor=%s amount=%.2f)", len(txns), p.VendorNorm, periodTotal)
			}

			if !isMultiPay {
				// Step 3a: Single-payment match — find ONE transaction matching period total
				bestTx, bestScore := h.scorePeriodCandidates(p, periodTotal, txns, consumed, aliases)
				if bestTx != nil && bestScore.Total >= matchThreshold {
					for _, d := range p.Docs {
						docScore := scoreBreakdown{
							Amount: bestScore.Amount, Date: bestScore.Date,
							Vendor: bestScore.Vendor, Invoice: bestScore.Invoice,
							Location: bestScore.Location, Total: bestScore.Total,
						}
						if err := h.recordMatch(db, d, *bestTx, docScore, "auto_statement_group"); err != nil {
							allErrors = append(allErrors, err.Error())
						} else {
							totalMatched++
							totalStmtGroup++
						}
					}
					consumed[bestTx.ID] = true

					txAmt := math.Abs(bestTx.Amount.Float64)
					status := "reconciled"
					if periodTotal > 0 && math.Abs(txAmt-periodTotal)/periodTotal > 0.01 {
						status = "partial"
					}
					h.upsertStatementPeriod(db, p.VendorID, p.Location, p.PeriodStart, p.PeriodEnd, periodTotal, txAmt, status)
				} else {
					totalUnmatched += len(p.Docs)
				}
			} else {
				// Step 3b: Multi-payment (AmEx) — find transactions that sum to period total
				m, u, errs := h.matchMultiPayPeriod(db, p, periodTotal, txns, consumed, aliases)
				totalMatched += m
				totalMultiPay += m
				totalUnmatched += u
				allErrors = append(allErrors, errs...)
			}
		}
		stmtDocs = nil
	}

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
// STATEMENT PERIOD BUILDING
// ═══════════════════════════════════════════════════════════════

// buildStatementPeriods groups statement vendor docs by vendor_id + location + week,
// calculates net totals (invoices - credits), and checks for actual statement docs.
func buildStatementPeriods(docs []wfDoc) []statementPeriod {
	type key struct {
		VendorID string
		Location string
		WeekKey  string
	}
	buckets := map[key]*statementPeriod{}

	for _, d := range docs {
		if !d.DocDate.Valid || !d.VendorID.Valid {
			continue
		}
		monday := weekStart(d.DocDate.Time)
		loc := ""
		if d.LocationName.Valid {
			loc = d.LocationName.String
		}
		k := key{VendorID: d.VendorID.String, Location: loc, WeekKey: monday.Format("2006-01-02")}

		p, ok := buckets[k]
		if !ok {
			vn := ""
			if d.VendorNorm.Valid {
				vn = d.VendorNorm.String
			}
			p = &statementPeriod{
				VendorID:    d.VendorID.String,
				VendorNorm:  vn,
				Location:    loc,
				PeriodStart: monday,
				PeriodEnd:   monday.AddDate(0, 0, 6),
				RepDoc:      d,
			}
			buckets[k] = p
		}
		p.Docs = append(p.Docs, d)

		if !d.Amount.Valid {
			continue
		}
		amt := math.Abs(d.Amount.Float64)
		docType := ""
		if d.DocType.Valid {
			docType = strings.ToUpper(d.DocType.String)
		}

		switch docType {
		case "CREDIT", "CREDIT_MEMO", "RMA":
			p.CreditTotal += amt
		case "STATEMENT":
			p.StmtAmount = amt // use statement amount if available
		default:
			p.InvTotal += amt
		}
	}

	result := make([]statementPeriod, 0, len(buckets))
	for _, p := range buckets {
		p.NetTotal = p.InvTotal - p.CreditTotal
		if p.NetTotal < 0 {
			p.NetTotal = 0
		}
		result = append(result, *p)
	}
	return result
}

// ═══════════════════════════════════════════════════════════════
// AMOUNT-FIRST TRANSACTION LOADING
// ═══════════════════════════════════════════════════════════════

// loadTxnsByAmount finds transactions by amount match (within 1%) for a period total.
// Date window: period_start - 3 to period_end + 16.
func (h *MatchingEngineHandler) loadTxnsByAmount(
	db *sql.DB, periodTotal float64,
	periodStart, periodEnd time.Time,
	vendorNorm string, aliases map[string][]vendorAlias, repDoc wfDoc,
) []plaidTx {
	from := periodStart.AddDate(0, 0, -3).Format("2006-01-02")
	to := periodEnd.AddDate(0, 0, 16).Format("2006-01-02")
	amtLow := periodTotal * 0.99
	amtHigh := periodTotal * 1.01

	// Build vendor LIKE patterns
	var patterns []string
	if vendorNorm != "" {
		patterns = append(patterns, strings.ToLower(strings.TrimSpace(vendorNorm)))
	}
	if repDoc.VendorID.Valid {
		for _, a := range aliases[repDoc.VendorID.String] {
			patterns = append(patterns, a.Alias)
		}
	}

	// Query: amount within 1% AND date in window AND vendor match
	var args []interface{}
	args = append(args, from, to, amtLow, amtHigh) // $1-$4

	vendorCond := "true" // no vendor filter if no patterns
	if len(patterns) > 0 {
		var likeConds []string
		for _, p := range patterns {
			args = append(args, "%"+p+"%")
			likeConds = append(likeConds, fmt.Sprintf("normalized_merchant LIKE $%d", len(args)))
		}
		vendorCond = "(" + strings.Join(likeConds, " OR ") + ")"
	}

	args = append(args, txBatchLimit)
	limitIdx := len(args)

	query := fmt.Sprintf(`
		SELECT id, location_name, transaction_date, amount,
		       description, merchant_name, normalized_merchant
		FROM plaid_transactions
		WHERE match_status = 'pending'
		  AND transaction_date BETWEEN $1 AND $2
		  AND (ABS(amount) BETWEEN $3 AND $4)
		  AND %s
		ORDER BY ABS(ABS(amount) - %f) ASC
		LIMIT $%d`, vendorCond, periodTotal, limitIdx)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("[MatchEngine] loadTxnsByAmount error: %v", err)
		return nil
	}
	defer rows.Close()
	txns, _ := scanTxns(rows)
	return txns
}

// loadTxnsByAmountMulti loads ALL vendor-matching transactions in the date window
// (not filtered by amount) for multi-payment summing.
func (h *MatchingEngineHandler) loadTxnsByAmountMulti(
	db *sql.DB, periodStart, periodEnd time.Time,
	vendorNorm string, aliases map[string][]vendorAlias, repDoc wfDoc,
) []plaidTx {
	from := periodStart.AddDate(0, 0, -3).Format("2006-01-02")
	to := periodEnd.AddDate(0, 0, 16).Format("2006-01-02")

	var patterns []string
	if vendorNorm != "" {
		patterns = append(patterns, strings.ToLower(strings.TrimSpace(vendorNorm)))
	}
	if repDoc.VendorID.Valid {
		for _, a := range aliases[repDoc.VendorID.String] {
			patterns = append(patterns, a.Alias)
		}
	}

	var args []interface{}
	args = append(args, from, to) // $1, $2

	vendorCond := "true"
	if len(patterns) > 0 {
		var likeConds []string
		for _, p := range patterns {
			args = append(args, "%"+p+"%")
			likeConds = append(likeConds, fmt.Sprintf("normalized_merchant LIKE $%d", len(args)))
		}
		vendorCond = "(" + strings.Join(likeConds, " OR ") + ")"
	}

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
		log.Printf("[MatchEngine] loadTxnsByAmountMulti error: %v", err)
		return nil
	}
	defer rows.Close()
	txns, _ := scanTxns(rows)
	return txns
}

// ═══════════════════════════════════════════════════════════════
// STATEMENT SINGLE-PAYMENT MATCHING
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) scorePeriodCandidates(
	p *statementPeriod, periodTotal float64,
	txns []plaidTx, consumed map[string]bool, aliases map[string][]vendorAlias,
) (*plaidTx, scoreBreakdown) {
	var bestTx *plaidTx
	var bestScore scoreBreakdown

	for i := range txns {
		tx := &txns[i]
		if consumed[tx.ID] || !tx.Amount.Valid || !tx.TransactionDate.Valid {
			continue
		}

		txAmt := math.Abs(tx.Amount.Float64)
		s := scoreBreakdown{
			Amount:   scoreAmount(periodTotal, txAmt),
			Date:     scoreDate(p.PeriodEnd, tx.TransactionDate.Time),
			Vendor:   scoreVendorWithAliases(p.RepDoc, *tx, aliases),
			Location: scoreLocationMatch(p.RepDoc, *tx),
		}
		// Invoice count bonus: 10 pts if period has more than 1 doc
		if len(p.Docs) > 1 {
			s.Invoice = 10
		}
		s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location

		if s.Total > bestScore.Total {
			bestScore = s
			bestTx = tx
		}
	}
	return bestTx, bestScore
}

// ═══════════════════════════════════════════════════════════════
// MULTI-PAYMENT MATCHING (AmEx pattern)
// ═══════════════════════════════════════════════════════════════

func (h *MatchingEngineHandler) matchMultiPayPeriod(
	db *sql.DB, p *statementPeriod, periodTotal float64,
	txns []plaidTx, consumed map[string]bool, aliases map[string][]vendorAlias,
) (matched int, unmatched int, errors []string) {
	if len(txns) == 0 {
		// Load all vendor transactions in the window (not amount-filtered)
		txns = h.loadTxnsByAmountMulti(db, p.PeriodStart, p.PeriodEnd, p.VendorNorm, aliases, p.RepDoc)
	}

	// Filter to unconsumed vendor-matching candidates
	var candidates []plaidTx
	for i := range txns {
		tx := &txns[i]
		if consumed[tx.ID] || !tx.Amount.Valid {
			continue
		}
		if scoreVendorWithAliases(p.RepDoc, *tx, aliases) >= 25 {
			candidates = append(candidates, *tx)
		}
	}

	// Try to find subset of transactions that sums to period total within 1%
	sumMatched, matchedTxIDs := findSumMatch(candidates, periodTotal)

	if sumMatched {
		// Mark all matching transactions
		for _, txID := range matchedTxIDs {
			consumed[txID] = true
		}
		// Mark all docs in this period as matched to the first transaction
		firstTxID := matchedTxIDs[0]
		for _, d := range p.Docs {
			s := scoreBreakdown{Amount: 25, Date: 20, Vendor: 25, Location: 0, Total: 70}
			s.Total = s.Amount + s.Date + s.Vendor + s.Invoice + s.Location
			// Build a synthetic plaidTx for recording
			synTx := plaidTx{ID: firstTxID}
			if err := h.recordMatch(db, d, synTx, s, "auto_multi_pay"); err != nil {
				errors = append(errors, err.Error())
			} else {
				matched++
			}
		}
		// Mark all matched transactions
		for _, txID := range matchedTxIDs {
			db.Exec(`UPDATE plaid_transactions SET match_status = 'matched' WHERE id = $1`, txID)
		}

		txAmt := 0.0
		for _, c := range candidates {
			for _, mid := range matchedTxIDs {
				if c.ID == mid && c.Amount.Valid {
					txAmt += math.Abs(c.Amount.Float64)
				}
			}
		}
		status := "reconciled"
		if periodTotal > 0 && math.Abs(txAmt-periodTotal)/periodTotal > 0.01 {
			status = "partial"
		}
		h.upsertStatementPeriod(db, p.VendorID, p.Location, p.PeriodStart, p.PeriodEnd, periodTotal, txAmt, status)
	} else {
		unmatched = len(p.Docs)
	}
	return
}

// findSumMatch tries to find a subset of transactions whose absolute amounts
// sum to within 1% of target. Uses greedy accumulation sorted by amount desc.
func findSumMatch(candidates []plaidTx, target float64) (bool, []string) {
	if target <= 0 || len(candidates) == 0 {
		return false, nil
	}

	// Sort by absolute amount descending
	sorted := make([]plaidTx, len(candidates))
	copy(sorted, candidates)
	sort.Slice(sorted, func(i, j int) bool {
		return math.Abs(sorted[i].Amount.Float64) > math.Abs(sorted[j].Amount.Float64)
	})

	var used []string
	runningSum := 0.0
	for _, tx := range sorted {
		txAmt := math.Abs(tx.Amount.Float64)
		if runningSum+txAmt <= target*1.01 {
			runningSum += txAmt
			used = append(used, tx.ID)
		}
		if runningSum >= target*0.99 {
			break
		}
	}

	if runningSum >= target*0.99 && runningSum <= target*1.01 {
		return true, used
	}
	return false, nil
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

func isStatementWeekly(doc wfDoc) bool {
	if !doc.StmtFreq.Valid || doc.StmtFreq.String == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(doc.StmtFreq.String), "weekly")
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

func weekStart(t time.Time) time.Time {
	wd := int(t.Weekday())
	if wd == 0 {
		wd = 7
	}
	return t.AddDate(0, 0, -(wd - 1))
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
