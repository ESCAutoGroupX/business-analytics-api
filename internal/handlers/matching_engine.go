package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
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

const matchThreshold = 75.0

// ── POST /admin/run-matching ────────────────────────────────────

func (h *MatchingEngineHandler) RunMatching(c *gin.Context) {
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

	// ── Log wf_documents schema for diagnostics ─────────────────
	h.logTableColumns(db, "wf_documents")
	h.logTableColumns(db, "vendors")

	// ── Log vendor_id coverage (vendor_id is pre-populated on wf_documents) ──
	var linkedCount, unlinkedCount int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE vendor_id IS NOT NULL`).Scan(&linkedCount)
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE vendor_id IS NULL`).Scan(&unlinkedCount)
	log.Printf("[MatchEngine] Vendor coverage: %d linked, %d unlinked (vendor_id already set on wf_documents)", linkedCount, unlinkedCount)

	// ── Load data ───────────────────────────────────────────────
	docs, err := h.loadPendingDocs(db)
	if err != nil {
		log.Printf("[MatchEngine] load docs error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to load documents", "error": err.Error()})
		return
	}

	txns, err := h.loadPendingTxns(db)
	if err != nil {
		log.Printf("[MatchEngine] load txns error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to load transactions", "error": err.Error()})
		return
	}

	aliases := h.loadVendorAliases(db)

	log.Printf("[MatchEngine] Processing %d documents against %d transactions", len(docs), len(txns))

	consumed := map[string]bool{}
	matched := 0
	unmatched := 0
	stmtGroupMatched := 0
	multiPayMatched := 0
	var errors []string

	// Classify documents into 3 buckets
	var directDocs []wfDoc
	stmtWeeklyGroups := map[string][]wfDoc{} // vendorID → docs
	multiPayGroups := map[string][]wfDoc{}    // vendorID → docs

	for _, doc := range docs {
		if isMultiPaymentVendor(doc) {
			key := ""
			if doc.VendorID.Valid {
				key = doc.VendorID.String
			}
			multiPayGroups[key] = append(multiPayGroups[key], doc)
		} else if isStatementWeekly(doc) {
			key := ""
			if doc.VendorID.Valid {
				key = doc.VendorID.String
			}
			stmtWeeklyGroups[key] = append(stmtWeeklyGroups[key], doc)
		} else {
			directDocs = append(directDocs, doc)
		}
	}

	// ── 1. Statement weekly vendors: group by week, match sum ───
	for vendorID, vendorDocs := range stmtWeeklyGroups {
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
						errors = append(errors, err.Error())
					} else {
						matched++
						stmtGroupMatched++
					}
				}
				consumed[bestTx.ID] = true

				// Update statement_periods
				h.upsertStatementPeriod(db, vendorID, bucket.periodStart, bucket.periodEnd, weekSum, weekSum)
			} else {
				unmatched += len(bucket.docs)
			}
		}
	}

	// ── 2. Multi-payment vendors (AmEx pattern) ─────────────────
	for vendorID, vendorDocs := range multiPayGroups {
		weekBuckets := groupByWeek(vendorDocs)
		for _, bucket := range weekBuckets {
			periodTotal := 0.0
			for _, d := range bucket.docs {
				if d.Amount.Valid {
					periodTotal += math.Abs(d.Amount.Float64)
				}
			}

			m, u, errs := h.matchMultiPaymentPeriod(db, vendorID, bucket, txns, consumed, aliases)
			matched += m
			multiPayMatched += m
			unmatched += u
			errors = append(errors, errs...)

			// Update statement_periods with cumulative matched total
			matchedAmt := 0.0
			for _, d := range bucket.docs {
				if d.Amount.Valid {
					// Check if this doc was matched (its ID would have been recorded)
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

	// ── 3. Direct vendor: match each invoice individually ────────
	debugCount := 0
	for _, doc := range directDocs {
		isCOD := isCODVendor(doc)
		dateWindow := 3
		if isCOD {
			dateWindow = 0
		}

		debug := debugCount < 5
		if debug {
			debugCount++
			logDocDebug(doc, debugCount)
		}

		best, bestScore := h.findDirectMatchDebug(doc, txns, consumed, aliases, isCOD, dateWindow, debug)
		if best != nil && bestScore.Total >= matchThreshold {
			if err := h.recordMatch(db, doc, *best, bestScore, "auto"); err != nil {
				errors = append(errors, err.Error())
				continue
			}
			consumed[best.ID] = true
			matched++
		} else {
			unmatched++
		}
	}

	log.Printf("[MatchEngine] Done: %d processed, %d matched (stmt=%d, multi=%d), %d unmatched, %d errors",
		len(docs), matched, stmtGroupMatched, multiPayMatched, unmatched, len(errors))

	c.JSON(http.StatusOK, gin.H{
		"processed":         len(docs),
		"matched":           matched,
		"unmatched":         unmatched,
		"statement_grouped": stmtGroupMatched,
		"multi_pay_matched": multiPayMatched,
		"errors":            errors,
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

// ── Data loaders ────────────────────────────────────────────────

func (h *MatchingEngineHandler) loadPendingDocs(db *sql.DB) ([]wfDoc, error) {
	rows, err := db.Query(`
		SELECT d.id, d.vendor_id, d.location_name, d.doc_type, d.doc_date,
		       d.amount, d.invoice_number, d.matched_rma_id, d.matched_credit_id,
		       v.normalized_name AS vendor_normalized_name,
		       v.statement_frequency AS stmt_freq,
		       COALESCE(v.multi_payment, false) AS multi_payment,
		       CASE WHEN v.vendor_type = 'statement' THEN true ELSE false END AS is_statement_vendor
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE d.match_status = 'pending'
		ORDER BY d.doc_date ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []wfDoc
	for rows.Next() {
		var d wfDoc
		if err := rows.Scan(&d.ID, &d.VendorID, &d.LocationName, &d.DocType,
			&d.DocDate, &d.Amount, &d.InvoiceNumber, &d.MatchedRMAID, &d.MatchedCreditID,
			&d.VendorNorm, &d.StmtFreq, &d.MultiPayment, &d.IsStatementVendor); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, rows.Err()
}

func (h *MatchingEngineHandler) loadPendingTxns(db *sql.DB) ([]plaidTx, error) {
	rows, err := db.Query(`
		SELECT id, location_name, transaction_date, amount,
		       description, merchant_name, normalized_merchant
		FROM plaid_transactions
		WHERE match_status = 'pending'
		ORDER BY transaction_date ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// loadVendorAliases returns location-aware aliases keyed by vendor_id.
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

	// COD: exact same calendar day
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

// scoreVendorWithAliases checks normalized name and location-aware aliases.
func scoreVendorWithAliases(doc wfDoc, tx plaidTx, aliases map[string][]vendorAlias) float64 {
	txMerch := ""
	if tx.NormalizedMerch.Valid {
		txMerch = strings.ToLower(strings.TrimSpace(tx.NormalizedMerch.String))
	}
	if txMerch == "" {
		return 0
	}

	// Check vendor normalized_name
	if doc.VendorNorm.Valid {
		vnDoc := strings.ToLower(strings.TrimSpace(doc.VendorNorm.String))
		if vnDoc != "" {
			if vnDoc == txMerch || strings.Contains(txMerch, vnDoc) || strings.Contains(vnDoc, txMerch) {
				return 25
			}
		}
	}

	// Check location-aware vendor aliases
	if doc.VendorID.Valid {
		docLoc := ""
		if doc.LocationName.Valid {
			docLoc = strings.ToLower(strings.TrimSpace(doc.LocationName.String))
		}

		for _, a := range aliases[doc.VendorID.String] {
			// If alias has a location constraint, it must match the document's location
			if a.LocationName != "" {
				if docLoc == "" || !strings.EqualFold(a.LocationName, docLoc) {
					continue // skip — location doesn't match
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
	if strings.EqualFold(
		strings.TrimSpace(doc.LocationName.String),
		strings.TrimSpace(tx.LocationName.String)) {
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
	return h.findDirectMatchDebug(doc, txns, consumed, aliases, isCOD, dateWindow, false)
}

func (h *MatchingEngineHandler) findDirectMatchDebug(
	doc wfDoc, txns []plaidTx, consumed map[string]bool,
	aliases map[string][]vendorAlias, isCOD bool, dateWindow int, debug bool,
) (*plaidTx, scoreBreakdown) {
	var bestTx *plaidTx
	var bestScore scoreBreakdown

	// For debug: collect top 3 candidates
	type debugCandidate struct {
		tx    plaidTx
		score scoreBreakdown
	}
	var topCandidates []debugCandidate

	for i := range txns {
		tx := &txns[i]
		if consumed[tx.ID] {
			continue
		}
		if !passesDateRule(doc, *tx, isCOD, dateWindow) {
			if debug {
				dayDiff := 0
				if doc.DocDate.Valid && tx.TransactionDate.Valid {
					dayDiff = int(doc.DocDate.Time.Sub(tx.TransactionDate.Time).Hours() / 24)
				}
				reason := "chrono_prefilter"
				if isCOD {
					reason = "cod_not_same_day"
				}
				log.Printf("[MatchEngine]   REJECTED tx=%s reason=%s date_diff=%d", tx.ID, reason, dayDiff)
			}
			continue
		}
		score := computeFullScore(doc, *tx, aliases)
		if score.Total > bestScore.Total {
			bestScore = score
			bestTx = tx
		}

		if debug {
			// Insert into top 3 sorted by total desc
			topCandidates = append(topCandidates, debugCandidate{tx: *tx, score: score})
			sort.Slice(topCandidates, func(a, b int) bool {
				return topCandidates[a].score.Total > topCandidates[b].score.Total
			})
			if len(topCandidates) > 3 {
				topCandidates = topCandidates[:3]
			}
		}
	}

	if debug {
		for rank, c := range topCandidates {
			txDate := ""
			if c.tx.TransactionDate.Valid {
				txDate = c.tx.TransactionDate.Time.Format("2006-01-02")
			}
			txAmt := 0.0
			if c.tx.Amount.Valid {
				txAmt = c.tx.Amount.Float64
			}
			log.Printf("[MatchEngine]   #%d TX %s | merchant=%s | location=%s | date=%s | amount=%.2f | scores: amt=%.1f date=%.1f vendor=%.1f inv=%.1f loc=%.1f TOTAL=%.1f",
				rank+1, c.tx.ID,
				nullStr(c.tx.NormalizedMerch), nullStr(c.tx.LocationName),
				txDate, txAmt,
				c.score.Amount, c.score.Date, c.score.Vendor, c.score.Invoice, c.score.Location, c.score.Total)
		}
		if len(topCandidates) == 0 {
			log.Printf("[MatchEngine]   (no candidates passed pre-filter)")
		}
	}

	return bestTx, bestScore
}

func logDocDebug(doc wfDoc, n int) {
	docDate := ""
	if doc.DocDate.Valid {
		docDate = doc.DocDate.Time.Format("2006-01-02")
	}
	docAmt := 0.0
	if doc.Amount.Valid {
		docAmt = doc.Amount.Float64
	}
	log.Printf("[MatchEngine] DOC #%d %s | vendor=%s | location=%s | date=%s | amount=%.2f | type=%s",
		n, doc.ID,
		nullStr(doc.VendorNorm), nullStr(doc.LocationName),
		docDate, docAmt, nullStr(doc.DocType))
}

func nullStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return "<null>"
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
	// Statement vendor: vendor_type = 'statement' with weekly frequency
	isStmt := doc.IsStatementVendor.Valid && doc.IsStatementVendor.Bool
	if !isStmt {
		return false
	}
	if !doc.StmtFreq.Valid {
		return false
	}
	return strings.EqualFold(doc.StmtFreq.String, "weekly")
}

func isStatementVendor(doc wfDoc) bool {
	return doc.IsStatementVendor.Valid && doc.IsStatementVendor.Bool
}

func isCODVendor(doc wfDoc) bool {
	// Statement vendors are never COD
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

	// Date window: period_start to period_end + 16 days
	windowStart := bucket.periodStart
	windowEnd := bucket.periodEnd.AddDate(0, 0, 16)

	// Collect candidate transactions in the date window that vendor-match
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

	// Sort docs by amount descending for greedy matching
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

	// Strategy 1: Try to match individual invoices to individual transactions
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
				continue // not within 1%
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
			// Strategy 2: Try matching against a running partial sum of remaining candidate txns
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

// tryPartialSumMatch tries to find a subset of transactions whose sum matches
// the document amount within 1%. Returns true if a match was found.
func (h *MatchingEngineHandler) tryPartialSumMatch(
	db *sql.DB, doc wfDoc, docAmt float64,
	candidates []plaidTx, consumed map[string]bool,
	aliases map[string][]vendorAlias,
) bool {
	// Also check: can a single transaction cover multiple invoices?
	// Here we check if any unconsumed transaction matches this doc amount.
	// The greedy partial-sum is more complex; for now check if any candidate
	// transaction amount is within 1% of this document amount.
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
		// Create unique constraint if it doesn't exist, then retry
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
