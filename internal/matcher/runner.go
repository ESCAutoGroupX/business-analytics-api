package matcher

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// txnRow is the minimal projection we pull from transactions. Uses the
// VARCHAR id and the signed double-precision amount.
type txnRow struct {
	ID                  string     `gorm:"column:id"`
	Amount              *float64   `gorm:"column:amount"`
	Date                *time.Time `gorm:"column:date"`
	Name                *string    `gorm:"column:name"`
	MerchantName        *string    `gorm:"column:merchant_name"`
	Vendor              *string    `gorm:"column:vendor"`
	AccountType         *string    `gorm:"column:account_type"`
	AccountSubtype      *string    `gorm:"column:account_subtype"`
	AccountName         *string    `gorm:"column:account_name"`
	DocumentMatchStatus *string    `gorm:"column:document_match_status"`
}

// docRow is the candidate-document projection. document_date is TEXT in
// this schema so we parse it client-side.
type docRow struct {
	ID                int64    `gorm:"column:id"`
	DocumentType      *string  `gorm:"column:document_type"`
	VendorName        *string  `gorm:"column:vendor_name"`
	TotalAmount       *float64 `gorm:"column:total_amount"`
	DocumentDate      *string  `gorm:"column:document_date"`
	WfInvoiceNumber   *string  `gorm:"column:wf_invoice_number"`
	WfScanPageID      *string  `gorm:"column:wf_scan_page_id"`
}

// RunPreview scores candidate matches for every transaction matching the
// filter and writes a human-review log to opts.LogPath. Never writes to
// wf_transaction_matches or wf_matcher_runs — that's live mode's job.
func RunPreview(ctx context.Context, db *gorm.DB, opts PreviewOpts) (*RunStats, []MatchProposal, error) {
	start := time.Now()
	stats := &RunStats{}

	aliasMap, err := loadAliasMap(ctx, db)
	if err != nil {
		return stats, nil, fmt.Errorf("load alias map: %w", err)
	}

	txns, err := loadTransactions(ctx, db, opts)
	if err != nil {
		return stats, nil, fmt.Errorf("load transactions: %w", err)
	}
	stats.TransactionsScanned = len(txns)

	var logFile *os.File
	if opts.LogPath != "" {
		if f, err := os.Create(opts.LogPath); err == nil {
			logFile = f
			defer f.Close()
		} else {
			recordErr(stats, fmt.Errorf("open log: %w", err))
		}
	}

	proposals := make([]MatchProposal, 0, len(txns))
	for _, t := range txns {
		if err := ctx.Err(); err != nil {
			return stats, proposals, err
		}
		p, considered, perr := scoreTransaction(ctx, db, t, aliasMap)
		stats.CandidatesConsidered += considered
		if perr != nil {
			recordErr(stats, perr)
			continue
		}
		if p == nil {
			// No candidates — synthesize an unmatched entry for the log so
			// humans reviewing the file see "tx X had no candidates".
			stats.UnmatchedCount++
			p = &MatchProposal{
				TransactionID: t.ID,
				Status:        StatusUnmatched,
				TxnDate:       derefTime(t.Date),
				TxnAmount:     derefFloat(t.Amount),
				TxnVendor:     txnVendorString(t),
				TxnAccount:    txnAccountString(t),
			}
		} else {
			switch p.Status {
			case StatusMatched:
				stats.MatchedCount++
			case StatusSuspect:
				stats.SuspectCount++
			case StatusAmbiguous:
				stats.AmbiguousCount++
			default:
				stats.UnmatchedCount++
			}
		}
		proposals = append(proposals, *p)
		if logFile != nil {
			writeLogEntry(logFile, p)
		}
	}

	if logFile != nil {
		fmt.Fprintf(logFile, "\n=== SUMMARY ===\n")
		fmt.Fprintf(logFile, "transactions_scanned=%d candidates_considered=%d\n",
			stats.TransactionsScanned, stats.CandidatesConsidered)
		fmt.Fprintf(logFile, "matched=%d suspect=%d ambiguous=%d unmatched=%d errors=%d\n",
			stats.MatchedCount, stats.SuspectCount, stats.AmbiguousCount,
			stats.UnmatchedCount, stats.ErrorCount)
	}

	stats.ElapsedMs = time.Since(start).Milliseconds()
	return stats, proposals, nil
}

func loadAliasMap(ctx context.Context, db *gorm.DB) (map[AliasKey]int64, error) {
	type aliasRow struct {
		ID        int64  `gorm:"column:id"`
		Alias     string `gorm:"column:alias"`
		Canonical string `gorm:"column:canonical"`
	}
	var rows []aliasRow
	err := db.WithContext(ctx).Raw(
		`SELECT id, alias, canonical FROM wf_vendor_aliases`,
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	// Apply the same vendor normalization pipeline to both sides so map
	// lookups against normalized request strings actually hit. LOWER() alone
	// isn't enough — "amazon.com" stays different from "amazon com" until
	// punctuation is stripped.
	out := make(map[AliasKey]int64, len(rows))
	for _, r := range rows {
		out[AliasKey{Alias: normalizeVendor(r.Alias), Canonical: normalizeVendor(r.Canonical)}] = r.ID
	}
	return out, nil
}

func loadTransactions(ctx context.Context, db *gorm.DB, opts PreviewOpts) ([]txnRow, error) {
	var where []string
	var args []interface{}
	if opts.OnlyUnmatched {
		where = append(where, "(document_match_status IS NULL OR document_match_status = 'none')")
	}
	if opts.DateFrom != nil {
		where = append(where, "date >= ?")
		args = append(args, opts.DateFrom.Format("2006-01-02"))
	}
	if opts.DateTo != nil {
		where = append(where, "date <= ?")
		args = append(args, opts.DateTo.Format("2006-01-02"))
	}
	// Only signed-debit transactions are plausible AP-invoice payers (positive
	// amount = debit in Plaid's convention). Keep all for now — scoreAmount
	// uses abs() so credits (refunds) will also score against credit docs.
	sql := `SELECT id, amount, date, name, merchant_name, vendor,
	               account_type, account_subtype, account_name, document_match_status
	          FROM transactions`
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	sql += " ORDER BY date DESC, id"
	if opts.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	var rows []txnRow
	if err := db.WithContext(ctx).Raw(sql, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// candidateScore pairs a docRow with its computed score so we can sort.
type candidateScore struct {
	doc     docRow
	score   int
	amount  AmountScore
	vendor  VendorScore
	dateSc  int
	daysGap int
	docDate time.Time
}

// scoreTransaction fetches the 7-day-window candidates and picks a top +
// runner-up. Returns (nil, considered, nil) if the candidate set is empty.
func scoreTransaction(
	ctx context.Context, db *gorm.DB, t txnRow, aliasMap map[AliasKey]int64,
) (*MatchProposal, int, error) {
	if t.Date == nil || t.Amount == nil {
		return nil, 0, nil
	}
	txnDate := *t.Date
	from := txnDate.AddDate(0, 0, -7).Format("2006-01-02")
	to := txnDate.Format("2006-01-02")

	var docs []docRow
	err := db.WithContext(ctx).Raw(
		`SELECT id, document_type, vendor_name, total_amount, document_date,
		        wf_invoice_number, wf_scan_page_id
		   FROM documents
		  WHERE document_type IN ('invoice','invoice-other','credit','credit-other','statement')
		    AND document_date BETWEEN ? AND ?
		    AND total_amount > 0
		    AND wf_ocr_agent_version IN ('wf-import-v1','wf-mongo-v1')
		    AND is_deleted = FALSE`,
		from, to,
	).Scan(&docs).Error
	if err != nil {
		return nil, 0, fmt.Errorf("candidate query: %w", err)
	}
	if len(docs) == 0 {
		return nil, 0, nil
	}

	isCredit := isCreditAccount(CreditAccountFields{
		AccountType:    derefStr(t.AccountType),
		AccountSubtype: derefStr(t.AccountSubtype),
		AccountName:    derefStr(t.AccountName),
	})
	txnVendor := txnVendorString(t)

	scored := make([]candidateScore, 0, len(docs))
	for _, d := range docs {
		if d.TotalAmount == nil {
			continue
		}
		amt := scoreAmount(*t.Amount, *d.TotalAmount, isCredit)
		if amt.Skip {
			continue
		}
		ven := scoreVendor(txnVendor, derefStr(d.VendorName), aliasMap)
		dd, _ := parseDocDate(derefStr(d.DocumentDate))
		dateSc, daysGap := scoreDate(txnDate, dd)
		total := amt.Score + ven.Score + dateSc
		scored = append(scored, candidateScore{
			doc: d, score: total, amount: amt, vendor: ven,
			dateSc: dateSc, daysGap: daysGap, docDate: dd,
		})
	}
	considered := len(scored)
	if considered == 0 {
		return nil, len(docs), nil
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	top := scored[0]
	var runnerUpScore *int
	if len(scored) > 1 {
		s := scored[1].score
		runnerUpScore = &s
	}
	status := classifyProposal(ClassifyInput{
		TopScore:         top.score,
		TopVendorScore:   top.vendor.Score,
		TopSurchargeFlag: top.amount.SurchargeFlag,
		TopAmountScore:   top.amount.Score,
		RunnerUpScore:    runnerUpScore,
	})

	p := &MatchProposal{
		TransactionID:        t.ID,
		DocumentID:           top.doc.ID,
		Score:                top.score,
		Status:               status,
		AmountMode:           top.amount.Mode,
		SurchargeFlag:        top.amount.SurchargeFlag,
		ActualInvoiceAmount:  top.amount.ActualInvoiceAmount,
		SurchargePct:         top.amount.SurchargePct,
		AmountScoreComponent: top.amount.Score,
		VendorScoreComponent: top.vendor.Score,
		DateScoreComponent:   top.dateSc,
		VendorSimilarityPct:  top.vendor.SimilarityPct,
		DaysApart:            top.daysGap,
		RunnerUpScore:        runnerUpScore,
		AliasMatchedID:       top.vendor.AliasMatchedID,

		TxnDate:       txnDate,
		TxnAmount:     *t.Amount,
		TxnVendor:     txnVendor,
		TxnAccount:    txnAccountString(t),
		DocDate:       top.docDate,
		DocAmount:     *top.doc.TotalAmount,
		DocVendor:     derefStr(top.doc.VendorName),
		DocType:       derefStr(top.doc.DocumentType),
		DocInvoiceNum: derefStr(top.doc.WfInvoiceNumber),
		DocScanPageID: derefStr(top.doc.WfScanPageID),
	}
	return p, considered, nil
}

// writeLogEntry formats a proposal block per the spec.
func writeLogEntry(w *os.File, p *MatchProposal) {
	drcr := "DR"
	if p.TxnAmount < 0 {
		drcr = "CR"
	}
	fmt.Fprintf(w, "=== TXN %s | $%.2f %s | %s | %s ===\n",
		p.TxnDate.Format("2006-01-02"), math.Abs(p.TxnAmount), drcr, p.TxnAccount, p.TxnVendor)

	switch p.Status {
	case StatusMatched:
		fmt.Fprintf(w, "  [MATCHED, score=%d, amount=%s]\n", p.Score, p.AmountMode)
		writeDocBlock(w, p)
	case StatusSuspect:
		tag := "SUSPECT"
		if p.SurchargeFlag {
			tag = "SUSPECT (surcharge)"
		}
		fmt.Fprintf(w, "  [%s, score=%d, amount=%s]\n", tag, p.Score, p.AmountMode)
		writeDocBlock(w, p)
	case StatusAmbiguous:
		fmt.Fprintf(w, "  [AMBIGUOUS, score=%d (runner-up=%d), amount=%s]\n",
			p.Score, derefInt(p.RunnerUpScore), p.AmountMode)
		writeDocBlock(w, p)
	case StatusUnmatched:
		fmt.Fprintln(w, "  [UNMATCHED — no qualifying candidates within 7 days]")
	}

	if p.RunnerUpScore != nil && p.Status != StatusAmbiguous && p.Status != StatusUnmatched {
		fmt.Fprintf(w, "  [runner-up score=%d]\n", *p.RunnerUpScore)
	}
	fmt.Fprintln(w)
}

func writeDocBlock(w *os.File, p *MatchProposal) {
	fmt.Fprintf(w, "    → Doc #%d | %s | $%.2f | %s | %s\n",
		p.DocumentID, p.DocDate.Format("2006-01-02"),
		p.DocAmount, p.DocType, p.DocVendor)
	if p.DocScanPageID != "" {
		fmt.Fprintf(w, "      scan_page_id: %s\n", p.DocScanPageID)
	}
	if p.DocInvoiceNum != "" {
		fmt.Fprintf(w, "      invoice_num:  %s\n", p.DocInvoiceNum)
	}
	if p.SurchargeFlag && p.SurchargePct != nil && p.ActualInvoiceAmount != nil {
		fmt.Fprintf(w, "      surcharge_pct: %.2f%%  actual_invoice_amount: $%.2f\n",
			*p.SurchargePct, *p.ActualInvoiceAmount)
	}
	fmt.Fprintf(w, "      scores: amount=%d vendor=%d date=%d sim=%.0f%%\n",
		p.AmountScoreComponent, p.VendorScoreComponent, p.DateScoreComponent, p.VendorSimilarityPct)
	if p.AliasMatchedID != nil {
		fmt.Fprintf(w, "      alias_matched_id: %d\n", *p.AliasMatchedID)
	}
}

// ── tiny helpers ────────────────────────────────────────────────

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
func derefFloat(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

func txnVendorString(t txnRow) string {
	for _, s := range []*string{t.MerchantName, t.Vendor, t.Name} {
		if s != nil && strings.TrimSpace(*s) != "" {
			return *s
		}
	}
	return ""
}

func txnAccountString(t txnRow) string {
	if t.AccountName != nil {
		return *t.AccountName
	}
	return ""
}

// parseDocDate accepts "YYYY-MM-DD" (our stored format). Returns zero
// Time on parse failure — scoring treats that as max-distant, which
// produces a low score naturally.
func parseDocDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse("2006-01-02", s)
}

func recordErr(stats *RunStats, err error) {
	stats.ErrorCount++
	if stats.FirstError == "" {
		stats.FirstError = err.Error()
	}
}
