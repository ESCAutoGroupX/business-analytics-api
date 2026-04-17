package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
)

// findPDF looks for a PDF file matching *{suffix}.pdf in a location subfolder.
func findPDF(baseDir, folder, suffix string) string {
	pattern := filepath.Join(baseDir, folder, "*"+suffix+".pdf")
	matches, _ := filepath.Glob(pattern)
	if len(matches) > 0 {
		return matches[0]
	}
	return ""
}

// CustomerNumberExtractor handles PDF text extraction and customer number parsing.
type CustomerNumberExtractor struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

// ── Regex patterns (ordered by specificity) ─────────────────

var customerNumberPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)Customer\s*Number[:\s]+(\d+)`),
	regexp.MustCompile(`(?i)Account\s*Number\s+(\d+)`),
	regexp.MustCompile(`(?i)Account\s*[#No.]*[:\s]+([A-Z0-9\-]+)`),
	regexp.MustCompile(`(?i)Customer\s*#[:\s]*(\w+)`),
	regexp.MustCompile(`(?i)Cust[.\s]*No[.:\s]+(\w+)`),
}

// IMC Parts Authority: header pattern "01000 XXXXXX" where XXXXXX is the account number
var imcAccountPattern = regexp.MustCompile(`\b01000\s+(\d{5,6})\b`)

// ── Find PDF on disk ────────────────────────────────────────

func findPDFOnDisk(pdfDir, locationName, wfScanID string) string {
	suffix := wfScanID
	if len(suffix) > 6 {
		suffix = suffix[len(suffix)-6:]
	}

	// Try the document's location folder first
	if locationName != "" {
		if p := findPDF(pdfDir, locationName, suffix); p != "" {
			return p
		}
	}

	// Fallback: search all subfolders
	entries, _ := os.ReadDir(pdfDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if p := findPDF(pdfDir, e.Name(), suffix); p != "" {
			return p
		}
	}
	return ""
}

// ── Extract text from PDF ───────────────────────────────────

func extractTextFromPDF(pdfPath string) (string, error) {
	cmd := exec.Command("pdftotext", "-layout", pdfPath, "-")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("pdftotext failed: %w", err)
	}
	return string(out), nil
}

// ── Extract customer number from text ───────────────────────

func extractCustomerNumberFromText(text string) string {
	// Try standard patterns first
	for _, pat := range customerNumberPatterns {
		m := pat.FindStringSubmatch(text)
		if len(m) >= 2 && strings.TrimSpace(m[1]) != "" {
			return strings.TrimSpace(m[1])
		}
	}

	// Try IMC-specific pattern
	m := imcAccountPattern.FindStringSubmatch(text)
	if len(m) >= 2 {
		return strings.TrimSpace(m[1])
	}

	return ""
}

// ExtractCustomerNumberFromLocalPDF finds the PDF, extracts text, parses customer number.
func ExtractCustomerNumberFromLocalPDF(pdfDir, wfScanID, locationName string) (string, error) {
	pdfPath := findPDFOnDisk(pdfDir, locationName, wfScanID)
	if pdfPath == "" {
		return "", fmt.Errorf("pdf not found for scan_id suffix %s", wfScanID)
	}

	text, err := extractTextFromPDF(pdfPath)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("pdf has no extractable text (image-only)")
	}

	return extractCustomerNumberFromText(text), nil
}

// ── Backfill job ────────────────────────────────────────────

type extractionJobStatus struct {
	mu        sync.Mutex
	Status    string   `json:"status"`
	Processed int      `json:"processed"`
	Extracted int      `json:"extracted"`
	Failed    int      `json:"failed"`
	FailedIDs []string `json:"failed_scan_ids"`
	Total     int      `json:"total"`
}

var currentJob = &extractionJobStatus{Status: "idle"}

func (h *CustomerNumberExtractor) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

func (h *CustomerNumberExtractor) BackfillCustomerNumbers() {
	db := h.sqlDB()

	currentJob.mu.Lock()
	currentJob.Status = "running"
	currentJob.Processed = 0
	currentJob.Extracted = 0
	currentJob.Failed = 0
	currentJob.FailedIDs = nil
	currentJob.mu.Unlock()

	rows, err := db.Query(`
		SELECT id, wf_scan_id, location_name FROM wf_documents
		WHERE doc_type = 'statement'
		  AND customer_number IS NULL
		  AND wf_scan_id IS NOT NULL AND wf_scan_id != ''
		ORDER BY doc_date DESC NULLS LAST`)
	if err != nil {
		log.Printf("[CustNumExtract] query error: %v", err)
		currentJob.mu.Lock()
		currentJob.Status = "error"
		currentJob.mu.Unlock()
		return
	}
	defer rows.Close()

	type docRow struct {
		id, scanID, location string
	}
	var docs []docRow
	for rows.Next() {
		var d docRow
		var loc sql.NullString
		if err := rows.Scan(&d.id, &d.scanID, &loc); err != nil {
			continue
		}
		if loc.Valid {
			d.location = loc.String
		}
		docs = append(docs, d)
	}

	currentJob.mu.Lock()
	currentJob.Total = len(docs)
	currentJob.mu.Unlock()

	log.Printf("[CustNumExtract] Starting: %d documents to process", len(docs))

	for i, d := range docs {
		custNum, err := ExtractCustomerNumberFromLocalPDF(h.Cfg.PDFDir, d.scanID, d.location)
		if err != nil {
			log.Printf("[CustNumExtract] %s: error: %v", d.scanID, err)
			currentJob.mu.Lock()
			currentJob.Failed++
			currentJob.FailedIDs = append(currentJob.FailedIDs, d.scanID)
			currentJob.mu.Unlock()
		} else if custNum == "" {
			currentJob.mu.Lock()
			currentJob.Failed++
			currentJob.FailedIDs = append(currentJob.FailedIDs, d.scanID)
			currentJob.mu.Unlock()
		} else {
			db.Exec(`UPDATE wf_documents SET customer_number = $1, customer_number_extracted_at = NOW() WHERE id = $2`,
				custNum, d.id)
			currentJob.mu.Lock()
			currentJob.Extracted++
			currentJob.mu.Unlock()
		}

		currentJob.mu.Lock()
		currentJob.Processed++
		currentJob.mu.Unlock()

		if (i+1)%50 == 0 {
			currentJob.mu.Lock()
			log.Printf("[CustNumExtract] Progress: %d/%d processed, %d extracted, %d failed",
				currentJob.Processed, currentJob.Total, currentJob.Extracted, currentJob.Failed)
			currentJob.mu.Unlock()
		}

		// Small delay to avoid hammering disk
		if (i+1)%50 == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	currentJob.mu.Lock()
	currentJob.Status = "complete"
	log.Printf("[CustNumExtract] Done: %d processed, %d extracted, %d failed",
		currentJob.Processed, currentJob.Extracted, currentJob.Failed)
	currentJob.mu.Unlock()
}

// ResolveLocationsFromCustomerNumbers updates location_name from vendor_location_accounts.
func (h *CustomerNumberExtractor) ResolveLocationsFromCustomerNumbers() int {
	db := h.sqlDB()
	result, err := db.Exec(`
		UPDATE wf_documents d
		SET location_name = vla.location_name
		FROM vendor_location_accounts vla
		WHERE d.vendor_id = vla.vendor_id
		  AND d.customer_number = vla.customer_number
		  AND d.customer_number IS NOT NULL
		  AND (d.location_name IS NULL OR d.location_name != vla.location_name)`)
	if err != nil {
		log.Printf("[CustNumExtract] resolve locations error: %v", err)
		return 0
	}
	n, _ := result.RowsAffected()
	log.Printf("[CustNumExtract] Resolved %d locations from customer numbers", n)
	return int(n)
}

// ── API Handlers ────────────────────────────────────────────

func (h *CustomerNumberExtractor) StartExtraction(c *gin.Context) {
	currentJob.mu.Lock()
	if currentJob.Status == "running" {
		currentJob.mu.Unlock()
		c.JSON(200, gin.H{"status": "already_running", "processed": currentJob.Processed, "total": currentJob.Total})
		return
	}
	currentJob.mu.Unlock()

	go h.BackfillCustomerNumbers()

	c.JSON(200, gin.H{"status": "started"})
}

func (h *CustomerNumberExtractor) GetExtractionStatus(c *gin.Context) {
	currentJob.mu.Lock()
	defer currentJob.mu.Unlock()
	c.JSON(200, gin.H{
		"status":          currentJob.Status,
		"processed":       currentJob.Processed,
		"total":           currentJob.Total,
		"extracted":       currentJob.Extracted,
		"failed":          currentJob.Failed,
		"failed_scan_ids": currentJob.FailedIDs,
	})
}

func (h *CustomerNumberExtractor) ResolveLocations(c *gin.Context) {
	n := h.ResolveLocationsFromCustomerNumbers()
	c.JSON(200, gin.H{"resolved": n})
}

func (h *CustomerNumberExtractor) AssignCustomerNumber(c *gin.Context) {
	docID := c.Param("doc_id")
	var req struct {
		CustomerNumber string `json:"customer_number"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.CustomerNumber == "" {
		c.JSON(400, gin.H{"detail": "customer_number is required"})
		return
	}

	db := h.sqlDB()
	res, err := db.Exec(`UPDATE wf_documents SET customer_number = $1, customer_number_extracted_at = NOW() WHERE id = $2`,
		req.CustomerNumber, docID)
	if err != nil {
		c.JSON(500, gin.H{"detail": err.Error()})
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		c.JSON(404, gin.H{"detail": "document not found"})
		return
	}

	// Try to resolve location
	db.Exec(`
		UPDATE wf_documents d
		SET location_name = vla.location_name
		FROM vendor_location_accounts vla
		WHERE d.id = $1
		  AND d.vendor_id = vla.vendor_id
		  AND d.customer_number = vla.customer_number`, docID)

	c.JSON(200, gin.H{"status": "updated", "customer_number": req.CustomerNumber})
}

func (h *CustomerNumberExtractor) CustomerNumberStats(c *gin.Context) {
	db := h.sqlDB()
	var total, withCN, withoutCN int
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE doc_type = 'statement'`).Scan(&total)
	db.QueryRow(`SELECT COUNT(*) FROM wf_documents WHERE doc_type = 'statement' AND customer_number IS NOT NULL`).Scan(&withCN)
	withoutCN = total - withCN

	// breakdown by vendor
	rows, err := db.Query(`
		SELECT COALESCE(v.normalized_name, 'unknown'),
		       COUNT(*),
		       COUNT(d.customer_number)
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE d.doc_type = 'statement'
		GROUP BY v.normalized_name
		ORDER BY COUNT(*) DESC`)
	if err != nil {
		c.JSON(200, gin.H{
			"total_statements":        total,
			"with_customer_number":    withCN,
			"without_customer_number": withoutCN,
		})
		return
	}
	defer rows.Close()

	type vendorStat struct {
		Total   int `json:"total"`
		WithCN  int `json:"with_customer_number"`
		Missing int `json:"without_customer_number"`
	}
	byVendor := map[string]vendorStat{}
	for rows.Next() {
		var name string
		var t, w int
		if rows.Scan(&name, &t, &w) == nil {
			byVendor[name] = vendorStat{Total: t, WithCN: w, Missing: t - w}
		}
	}

	c.JSON(200, gin.H{
		"total_statements":        total,
		"with_customer_number":    withCN,
		"without_customer_number": withoutCN,
		"by_vendor":               byVendor,
	})
}

// BackfillCustomerNumbersSync is the synchronous version for cron jobs.
func BackfillCustomerNumbersSync(gormDB *gorm.DB, cfg *config.Config) {
	h := &CustomerNumberExtractor{GormDB: gormDB, Cfg: cfg}
	h.BackfillCustomerNumbers()
}

func ResolveLocationsSync(gormDB *gorm.DB, cfg *config.Config) {
	h := &CustomerNumberExtractor{GormDB: gormDB, Cfg: cfg}
	h.ResolveLocationsFromCustomerNumbers()
}

// FindPDFPath exports the PDF lookup for use by ServePDF (avoid duplication).
func FindPDFPath(pdfDir, locationName, wfScanID string) string {
	return findPDFOnDisk(pdfDir, locationName, wfScanID)
}
