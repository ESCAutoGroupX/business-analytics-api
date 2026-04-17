package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

// wfPDFRoot is the default location of WickedFile PDFs on the host.
var wfPDFRoot = "/var/www/html/wf-pdfs"

// wfLocationMap maps location folder name → location_code written to
// the documents table. ESC_Alpharetta and Alpharetta both resolve to ALP.
var wfLocationMap = map[string]string{
	"Alpharetta":     "ALP",
	"ESC_Alpharetta": "ALP",
	"Cedar_Springs":  "CED",
	"Duluth":         "DUL",
	"Highlands":      "HIG",
	"Houston":        "HOU",
	"Piedmont":       "PIE",
	"Preston":        "PRE",
	"Roswell_Rd":     "ROS",
	"Sandy_Springs":  "SAN",
	"Tracy_St":       "TRA",
	"ESC_AutoGroup":  "ESC",
}

// wfDocTypeMap translates the lowercase filename type token to the
// document_type column value used elsewhere in the codebase.
var wfDocTypeMap = map[string]string{
	"invoice":   "INVOICE",
	"statement": "STATEMENT",
	"credit":    "CREDIT_MEMO",
	"rma":       "CREDIT_MEMO",
	"receipt":   "RECEIPT",
	"unknown":   "INVOICE",
}

// Progress counters (atomic).
var (
	wfImportRunning  int32
	wfImportTotal    int32
	wfImportImported int32
	wfImportSkipped  int32
	wfImportErrors   int32

	wfOCRRunning   int32
	wfOCRPending   int32
	wfOCRProcessed int32
	wfOCRMatched   int32
	wfOCRFailed    int32
)

// BulkImportHandler wires together the pipeline and matcher handlers
// so the bulk-import endpoints can trigger the existing OCR + match flows.
type BulkImportHandler struct {
	GormDB   *gorm.DB
	Cfg      *config.Config
	Pipeline *DocumentHandler
	Matcher  *DocumentMatchHandler
}

// AutoMigrate adds columns used by the bulk-import flow.
func (h *BulkImportHandler) AutoMigrate() {
	db, _ := h.GormDB.DB()
	if db == nil {
		return
	}
	stmts := []string{
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_id VARCHAR`,
		`CREATE INDEX IF NOT EXISTS idx_documents_wf_id ON documents(wf_id)`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS wf_scan_id TEXT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_wf_scan_id ON documents(wf_scan_id) WHERE wf_scan_id IS NOT NULL`,
		`ALTER TABLE documents ADD COLUMN IF NOT EXISTS notes TEXT`,
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS ocr_vendor_name VARCHAR`,
		`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS ocr_vendor_source VARCHAR`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			log.Printf("[BulkImport] migration warning: %v", err)
		}
	}
}

// parseWFFilename parses a filename of the form
//
//	YYYY-MM-DD_Vendor_Name_Words_type_hexid.pdf
//
// and returns the structured parts. `docType` is already mapped to the
// canonical column value (INVOICE / STATEMENT / …). A 1970-01-01 date
// is returned as the empty string so callers can treat it as unknown.
func parseWFFilename(name string) (date, vendor, docType, wfID string, ok bool) {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	if len(base) < 20 {
		return "", "", "", "", false
	}
	date = base[:10]
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return "", "", "", "", false
	}
	if base[10] != '_' {
		return "", "", "", "", false
	}
	if date == "1970-01-01" {
		date = ""
	}

	parts := strings.Split(base[11:], "_")
	if len(parts) < 3 {
		return "", "", "", "", false
	}
	wfID = parts[len(parts)-1]
	typeTok := strings.ToLower(parts[len(parts)-2])
	mapped, known := wfDocTypeMap[typeTok]
	if !known {
		mapped = "INVOICE"
	}
	vendor = titleCaseWords(strings.Join(parts[:len(parts)-2], " "))
	return date, vendor, mapped, wfID, true
}

// titleCaseWords returns s with each space-separated word capitalised.
func titleCaseWords(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		if w == "" {
			continue
		}
		words[i] = strings.ToUpper(w[:1]) + strings.ToLower(w[1:])
	}
	return strings.Join(words, " ")
}

// scanWFPDFs walks wfPDFRoot and returns the full paths of every .pdf file.
func scanWFPDFs() ([]string, error) {
	var paths []string
	err := filepath.WalkDir(wfPDFRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".pdf") {
			paths = append(paths, p)
		}
		return nil
	})
	return paths, err
}

// BulkImportWF handles POST /documents/bulk-import-wf.
func (h *BulkImportHandler) BulkImportWF(c *gin.Context) {
	if !atomic.CompareAndSwapInt32(&wfImportRunning, 0, 1) {
		c.JSON(http.StatusConflict, gin.H{
			"detail":   "bulk import already running",
			"imported": atomic.LoadInt32(&wfImportImported),
			"total":    atomic.LoadInt32(&wfImportTotal),
		})
		return
	}

	atomic.StoreInt32(&wfImportTotal, 0)
	atomic.StoreInt32(&wfImportImported, 0)
	atomic.StoreInt32(&wfImportSkipped, 0)
	atomic.StoreInt32(&wfImportErrors, 0)

	paths, err := scanWFPDFs()
	if err != nil {
		atomic.StoreInt32(&wfImportRunning, 0)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	atomic.StoreInt32(&wfImportTotal, int32(len(paths)))

	c.JSON(http.StatusOK, gin.H{
		"status":          "started",
		"estimated_total": len(paths),
	})

	go h.runBulkImport(paths)
}

// BulkImportStatus handles GET /documents/bulk-import-status.
func (h *BulkImportHandler) BulkImportStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"imported":    atomic.LoadInt32(&wfImportImported),
		"skipped":     atomic.LoadInt32(&wfImportSkipped),
		"errors":      atomic.LoadInt32(&wfImportErrors),
		"running":     atomic.LoadInt32(&wfImportRunning) == 1,
		"total_found": atomic.LoadInt32(&wfImportTotal),
	})
}

// runBulkImport performs the actual scan-and-insert work. It is called
// from a goroutine by BulkImportWF so the HTTP caller returns immediately.
func (h *BulkImportHandler) runBulkImport(paths []string) {
	defer atomic.StoreInt32(&wfImportRunning, 0)

	// Pre-load existing wf_id values so we can skip already-imported files.
	var existing []string
	h.GormDB.Model(&models.Document{}).
		Where("wf_id IS NOT NULL AND wf_id != ''").
		Pluck("wf_id", &existing)
	seen := make(map[string]struct{}, len(existing))
	for _, id := range existing {
		seen[id] = struct{}{}
	}

	log.Printf("[BulkImportWF] starting: %d paths, %d already imported", len(paths), len(seen))

	const batchSize = 200
	batch := make([]models.Document, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := h.GormDB.Create(&batch).Error; err != nil {
			log.Printf("[BulkImportWF] batch insert error (%d rows): %v", len(batch), err)
			atomic.AddInt32(&wfImportErrors, int32(len(batch)))
		} else {
			atomic.AddInt32(&wfImportImported, int32(len(batch)))
		}
		batch = batch[:0]
	}

	agentVersion := "filename-parse-v1"
	isFinancial := true

	for _, fullPath := range paths {
		filename := filepath.Base(fullPath)
		folder := filepath.Base(filepath.Dir(fullPath))

		date, vendor, docType, wfID, okParse := parseWFFilename(filename)
		if !okParse {
			atomic.AddInt32(&wfImportErrors, 1)
			continue
		}
		if _, already := seen[wfID]; already {
			atomic.AddInt32(&wfImportSkipped, 1)
			continue
		}
		seen[wfID] = struct{}{}

		locationCode, locKnown := wfLocationMap[folder]
		locationName := folder
		if !locKnown {
			locationCode = ""
		}

		doc := models.Document{
			Filename:        filename,
			FilePath:        fullPath,
			DocumentType:    wfStrPtr(docType),
			VendorName:      wfStrPtrOrNil(vendor),
			DocumentDate:    wfStrPtrOrNil(date),
			Location:        wfStrPtrOrNil(locationName),
			LocationCode:    wfStrPtrOrNil(locationCode),
			Status:          "imported",
			IsFinancial:     &isFinancial,
			OCRAgentVersion: &agentVersion,
			WfID:            wfStrPtrOrNil(wfID),
		}
		batch = append(batch, doc)
		if len(batch) >= batchSize {
			flush()
		}
	}
	flush()

	log.Printf("[BulkImportWF] complete: imported=%d skipped=%d errors=%d total=%d",
		atomic.LoadInt32(&wfImportImported),
		atomic.LoadInt32(&wfImportSkipped),
		atomic.LoadInt32(&wfImportErrors),
		atomic.LoadInt32(&wfImportTotal))

	// Kick off matching in the background so the UI can continue polling
	// status; callers can also explicitly POST /documents/match-all.
	if h.Matcher != nil {
		go h.Matcher.MatchDocumentsToTransactions()
	}
}

// wfStrPtr returns a pointer to s.
func wfStrPtr(s string) *string { return &s }

// wfStrPtrOrNil returns nil if s is empty, otherwise a pointer to s.
func wfStrPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// BulkOCR handles POST /documents/bulk-ocr — runs the 4-agent pipeline
// against every document that was created by the filename-parse importer.
func (h *BulkImportHandler) BulkOCR(c *gin.Context) {
	if !atomic.CompareAndSwapInt32(&wfOCRRunning, 0, 1) {
		c.JSON(http.StatusConflict, gin.H{
			"detail":    "bulk OCR already running",
			"processed": atomic.LoadInt32(&wfOCRProcessed),
			"pending":   atomic.LoadInt32(&wfOCRPending),
		})
		return
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		atomic.StoreInt32(&wfOCRRunning, 0)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "ANTHROPIC_API_KEY not configured"})
		return
	}

	var docs []models.Document
	h.GormDB.Where("status = ? AND ocr_agent_version = ? AND is_deleted = false",
		"imported", "filename-parse-v1").Find(&docs)

	atomic.StoreInt32(&wfOCRPending, int32(len(docs)))
	atomic.StoreInt32(&wfOCRProcessed, 0)
	atomic.StoreInt32(&wfOCRMatched, 0)
	atomic.StoreInt32(&wfOCRFailed, 0)

	c.JSON(http.StatusOK, gin.H{
		"status":  "started",
		"pending": len(docs),
	})

	go h.runBulkOCR(docs, apiKey)
}

// BulkOCRStatus handles GET /documents/bulk-ocr-status.
func (h *BulkImportHandler) BulkOCRStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"processed": atomic.LoadInt32(&wfOCRProcessed),
		"pending":   atomic.LoadInt32(&wfOCRPending),
		"running":   atomic.LoadInt32(&wfOCRRunning) == 1,
		"matched":   atomic.LoadInt32(&wfOCRMatched),
		"failed":    atomic.LoadInt32(&wfOCRFailed),
	})
}

// runBulkOCR is the background worker for BulkOCR. It processes documents
// in batches of batchSize with a short delay between batches to stay under
// the upstream API rate limits.
func (h *BulkImportHandler) runBulkOCR(docs []models.Document, apiKey string) {
	defer atomic.StoreInt32(&wfOCRRunning, 0)

	const (
		batchSize = 10
		batchWait = 2 * time.Second
	)

	log.Printf("[BulkOCR] starting: %d documents", len(docs))

	for start := 0; start < len(docs); start += batchSize {
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		for _, doc := range docs[start:end] {
			if _, err := os.Stat(doc.FilePath); err != nil {
				log.Printf("[BulkOCR] doc %d missing file %s: %v", doc.ID, doc.FilePath, err)
				atomic.AddInt32(&wfOCRFailed, 1)
				atomic.AddInt32(&wfOCRProcessed, 1)
				continue
			}
			result := h.ocrOne(apiKey, doc)
			if errMsg, ok := result["error"].(string); ok && errMsg != "" {
				atomic.AddInt32(&wfOCRFailed, 1)
			} else {
				atomic.AddInt32(&wfOCRMatched, 1)
			}
			atomic.AddInt32(&wfOCRProcessed, 1)
		}
		if end < len(docs) {
			time.Sleep(batchWait)
		}
	}

	log.Printf("[BulkOCR] complete: processed=%d matched=%d failed=%d",
		atomic.LoadInt32(&wfOCRProcessed),
		atomic.LoadInt32(&wfOCRMatched),
		atomic.LoadInt32(&wfOCRFailed))

	if h.Matcher != nil {
		h.Matcher.MatchDocumentsToTransactions()
	}
}

// ocrOne runs the 4-agent pipeline against a single document and updates
// the row in place. It is a lighter-weight version of
// DocumentHandler.reprocessSingleDocument that skips multi-invoice
// splitting (WickedFile exports are already one file per invoice).
func (h *BulkImportHandler) ocrOne(apiKey string, doc models.Document) gin.H {
	fileBytes, err := os.ReadFile(doc.FilePath)
	if err != nil {
		h.GormDB.Model(&doc).Update("status", "ocr_failed")
		return gin.H{"error": fmt.Sprintf("read: %v", err)}
	}
	ext := strings.ToLower(filepath.Ext(doc.Filename))
	b64 := base64.StdEncoding.EncodeToString(fileBytes)

	ocrResult, ocrRaw, confidence, agentVersion, err := h.Pipeline.runPipeline(apiKey, b64, ext)
	if err != nil {
		h.GormDB.Model(&doc).Updates(map[string]interface{}{
			"status":            "ocr_failed",
			"ocr_agent_version": agentVersion,
		})
		return gin.H{"error": err.Error()}
	}

	status := "pending"
	if confidence >= 0.85 {
		status = "auto_matched"
	} else if confidence < 0.60 {
		status = "needs_review"
	}

	updates := map[string]interface{}{
		"document_type":         ocrResult.DocumentType,
		"vendor_name":           ocrResult.VendorName,
		"vendor_address":        ocrResult.VendorAddress,
		"document_date":         ocrResult.DocumentDate,
		"document_number":       ocrResult.DocumentNumber,
		"vendor_po_number":      ocrResult.VendorPONumber,
		"vendor_invoice_number": ocrResult.VendorInvoiceNumber,
		"order_number":          ocrResult.OrderNumber,
		"ocr_raw":               ocrRaw,
		"ocr_confidence":        confidence,
		"ocr_agent_version":     agentVersion,
		"status":                status,
	}
	if ocrResult.TotalAmount != 0 {
		updates["total_amount"] = ocrResult.TotalAmount
	}
	if ocrResult.TaxAmount != 0 {
		updates["tax_amount"] = ocrResult.TaxAmount
	}
	if ocrResult.LineItems != nil {
		if liJSON, mErr := json.Marshal(ocrResult.LineItems); mErr == nil {
			updates["line_items"] = string(liJSON)
		}
	}
	for _, ref := range ocrResult.POReferences {
		if strings.HasPrefix(ref, "agent4_match:") {
			matchID := strings.TrimPrefix(ref, "agent4_match:")
			updates["matched_transaction_id"] = matchID
			if status != "auto_matched" {
				updates["status"] = "matched"
			}
			break
		}
	}

	h.GormDB.Model(&models.Document{}).Where("id = ?", doc.ID).Updates(updates)
	return gin.H{"document_id": doc.ID, "confidence": confidence}
}

// ──────────────────────────────────────────────────────────────────────────────
// Import from wf_documents (DB-driven, one-shot)
// ──────────────────────────────────────────────────────────────────────────────

// buildWFPathMap walks wfPDFRoot once and builds a map of
//
//	location_name + ":" + last-6-hex-of-filename  →  full disk path
//
// The wf_documents.wf_scan_id is a 24-char hex string; only its last 6
// chars appear in the filename on disk, so we key by that suffix.
func buildWFPathMap() (map[string]string, error) {
	m := make(map[string]string, 12000)
	err := filepath.WalkDir(wfPDFRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".pdf") {
			return nil
		}
		base := strings.TrimSuffix(name, filepath.Ext(name))
		parts := strings.Split(base, "_")
		if len(parts) == 0 {
			return nil
		}
		suffix := strings.ToLower(parts[len(parts)-1])
		if len(suffix) != 6 {
			return nil
		}
		loc := filepath.Base(filepath.Dir(p))
		m[loc+":"+suffix] = p
		return nil
	})
	return m, err
}

// locationCodeFor returns the canonical ALP/CED/… code for a WickedFile
// folder name. Unknown folders fall back to ESC.
func locationCodeFor(folder string) string {
	if code, ok := wfLocationMap[folder]; ok {
		return code
	}
	return "ESC"
}

// matchStatusToDocStatus maps wf_documents.match_status to the documents
// table's status enum.
func matchStatusToDocStatus(ms string) string {
	switch ms {
	case "matched":
		return "matched"
	case "ignored":
		return "unmatched_explicit"
	default:
		return "pending"
	}
}

// ImportFromWF handles POST /documents/import-from-wf — joins wf_documents
// against the vendors table, resolves each row's real disk path via the
// filesystem map, and batch-inserts into documents with
// ON CONFLICT (wf_scan_id) DO NOTHING.
func (h *BulkImportHandler) ImportFromWF(c *gin.Context) {
	if !atomic.CompareAndSwapInt32(&wfImportRunning, 0, 1) {
		c.JSON(http.StatusConflict, gin.H{"detail": "bulk import already running"})
		return
	}
	defer atomic.StoreInt32(&wfImportRunning, 0)

	start := time.Now()

	pathMap, err := buildWFPathMap()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "scan: " + err.Error()})
		return
	}

	db, _ := h.GormDB.DB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "no db"})
		return
	}

	rows, err := db.Query(`
		SELECT
			wf.wf_scan_id,
			wf.location_name,
			wf.doc_type,
			v.name,
			wf.doc_date,
			wf.amount,
			wf.invoice_number,
			wf.po_number,
			wf.line_items::text,
			wf.match_status,
			wf.matched_transaction_id::text,
			wf.customer_number,
			wf.wf_created_at
		FROM wf_documents wf
		LEFT JOIN vendors v ON v.id::text = wf.vendor_id::text
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "query wf_documents: " + err.Error()})
		return
	}
	defer rows.Close()

	isFinancial := true
	agent := "wf-import-v1"

	var (
		docs         []models.Document
		missingFile  int
		total        int
		shortScan    int
	)
	const batchSize = 500

	flush := func() (inserted int) {
		if len(docs) == 0 {
			return
		}
		res := h.GormDB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "wf_scan_id"}},
			DoNothing: true,
		}).Create(&docs)
		if res.Error != nil {
			log.Printf("[ImportFromWF] batch insert error (%d rows): %v", len(docs), res.Error)
		}
		inserted = int(res.RowsAffected)
		docs = docs[:0]
		return
	}

	var imported, skippedExisting int
	for rows.Next() {
		total++
		var (
			scanID, locName, docType                  string
			vendorName, invNum, poNum, liJSON         *string
			matchStatus, matchedTxnID, custNum        *string
			docDate                                   *time.Time
			amount                                    *float64
			wfCreated                                 *time.Time
		)
		if err := rows.Scan(
			&scanID, &locName, &docType, &vendorName, &docDate, &amount,
			&invNum, &poNum, &liJSON, &matchStatus, &matchedTxnID,
			&custNum, &wfCreated,
		); err != nil {
			log.Printf("[ImportFromWF] row scan error: %v", err)
			continue
		}

		if len(scanID) < 6 {
			shortScan++
			continue
		}
		suffix := strings.ToLower(scanID[len(scanID)-6:])
		path, ok := pathMap[locName+":"+suffix]
		if !ok {
			missingFile++
			continue
		}
		filename := filepath.Base(path)

		docTypeUpper := strings.ToUpper(docType)
		status := "pending"
		if matchStatus != nil {
			status = matchStatusToDocStatus(*matchStatus)
		}

		var docDateStr *string
		if docDate != nil {
			s := docDate.Format("2006-01-02")
			docDateStr = &s
		}

		noteCN := ""
		if custNum != nil {
			noteCN = *custNum
		}
		noteVal := "customer_number: " + noteCN

		locCode := locationCodeFor(locName)
		scanIDCopy := scanID
		locCopy := locName

		doc := models.Document{
			Filename:             filename,
			FilePath:             path,
			DocumentType:         &docTypeUpper,
			VendorName:           vendorName,
			DocumentDate:         docDateStr,
			TotalAmount:          amount,
			VendorInvoiceNumber:  invNum,
			PONumber:             poNum,
			LineItems:            liJSON,
			Location:             &locCopy,
			LocationCode:         &locCode,
			Status:               status,
			MatchedTransactionID: matchedTxnID,
			Notes:                &noteVal,
			IsDeleted:            false,
			IsFinancial:          &isFinancial,
			OCRAgentVersion:      &agent,
			WfScanID:             &scanIDCopy,
		}
		if wfCreated != nil {
			doc.CreatedAt = *wfCreated
		}

		docs = append(docs, doc)
		if len(docs) >= batchSize {
			inserted := flush()
			imported += inserted
			skippedExisting += batchSize - inserted
		}
	}
	if len(docs) > 0 {
		remaining := len(docs)
		inserted := flush()
		imported += inserted
		skippedExisting += remaining - inserted
	}

	elapsed := time.Since(start)
	log.Printf("[ImportFromWF] done: total=%d imported=%d skipped_existing=%d missing_file=%d short_scan=%d in %s",
		total, imported, skippedExisting, missingFile, shortScan, elapsed)

	resp := gin.H{
		"status":           "complete",
		"total":            total,
		"imported":         imported,
		"skipped_existing": skippedExisting,
		"missing_file":     missingFile,
		"short_scan_id":    shortScan,
		"elapsed_ms":       elapsed.Milliseconds(),
	}
	c.JSON(http.StatusOK, resp)

	// Kick off matching asynchronously so the response returns quickly.
	if h.Matcher != nil && imported > 0 {
		go h.Matcher.MatchDocumentsToTransactions()
	}
}
