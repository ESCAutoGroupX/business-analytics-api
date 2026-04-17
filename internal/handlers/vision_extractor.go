package handlers

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
)

// VisionExtractor handles Claude Vision-based document data extraction.
type VisionExtractor struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

type VisionExtractedData struct {
	CustomerNumber string           `json:"customer_number"`
	VendorName     string           `json:"vendor_name"`
	DocumentDate   string           `json:"document_date"`
	InvoiceNumber  string           `json:"invoice_number"`
	TotalAmount    *float64         `json:"total_amount"`
	LineItems      []VisionLineItem `json:"line_items"`
	Confidence     string           `json:"confidence"`
}

type VisionLineItem struct {
	InvoiceID   string   `json:"invoice_id"`
	Date        string   `json:"date"`
	Amount      *float64 `json:"amount"`
	Description string   `json:"description"`
}

func (h *VisionExtractor) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// ── PDF to image conversion ─────────────────────────────────

func convertPDFPageToImage(pdfPath string) ([]byte, error) {
	// Use a unique temp prefix to avoid collisions
	prefix := fmt.Sprintf("/tmp/vision_page_%d", time.Now().UnixNano())

	cmd := exec.Command("pdftoppm", "-r", "150", "-f", "1", "-l", "1", "-png", pdfPath, prefix)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("pdftoppm failed: %w: %s", err, string(out))
	}

	// pdftoppm outputs {prefix}-1.png (or {prefix}-01.png depending on page count)
	var imgPath string
	for _, suffix := range []string{"-1.png", "-01.png", "-001.png"} {
		candidate := prefix + suffix
		if _, err := os.Stat(candidate); err == nil {
			imgPath = candidate
			break
		}
	}
	if imgPath == "" {
		// Try glob as last resort
		matches, _ := filepath.Glob(prefix + "*.png")
		if len(matches) > 0 {
			imgPath = matches[0]
		}
	}
	if imgPath == "" {
		return nil, fmt.Errorf("pdftoppm produced no output file for prefix %s", prefix)
	}
	defer os.Remove(imgPath)

	data, err := os.ReadFile(imgPath)
	if err != nil {
		return nil, fmt.Errorf("reading converted image: %w", err)
	}
	return data, nil
}

// ── Claude Vision API call ──────────────────────────────────

func (h *VisionExtractor) callClaudeVision(imageBytes []byte, docType string) (*VisionExtractedData, string, error) {
	apiKey := h.Cfg.AnthropicAPIKey
	if apiKey == "" {
		return nil, "", fmt.Errorf("ANTHROPIC_API_KEY not configured")
	}

	b64Image := base64.StdEncoding.EncodeToString(imageBytes)

	userPrompt := fmt.Sprintf(`Extract all available data from this %s document and return a JSON object with these exact fields:
{
  "customer_number": "the account or customer number for this location",
  "vendor_name": "the vendor or company name",
  "document_date": "date in YYYY-MM-DD format",
  "invoice_number": "the statement number, delivery number, or reference ID printed at the top of the document — NOT the total amount, NOT a dollar value — this must be an alphanumeric identifier like 260407 or INV-12345",
  "total_amount": numeric total amount as a number not a string,
  "line_items": [
    {
      "invoice_id": "reference or invoice number for this line",
      "date": "YYYY-MM-DD",
      "amount": numeric amount,
      "description": "line item description"
    }
  ],
  "confidence": "high if all fields found, medium if some missing, low if mostly unreadable"
}
Use null for any field you cannot find. Do not guess.
IMPORTANT: total_amount must be a number. invoice_number must be a text identifier, never a dollar amount. customer_number must be the account number for this specific shop location, not a phone number or zip code.`, docType)

	reqBody := map[string]interface{}{
		"model":      "claude-3-haiku-20240307",
		"max_tokens": 4096,
		"system":     "You are a document data extraction assistant for an automotive repair business accounting system. Extract structured data from vendor documents. Always respond with valid JSON only — no explanation, no markdown, just the JSON object.",
		"messages": []map[string]interface{}{
			{
				"role": "user",
				"content": []map[string]interface{}{
					{
						"type": "image",
						"source": map[string]interface{}{
							"type":         "base64",
							"media_type":   "image/png",
							"data":         b64Image,
						},
					},
					{
						"type": "text",
						"text": userPrompt,
					},
				},
			},
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("API call failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, string(respBody), fmt.Errorf("API returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse Anthropic response envelope
	var apiResp struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, string(respBody), fmt.Errorf("parse API response: %w", err)
	}

	if len(apiResp.Content) == 0 {
		return nil, string(respBody), fmt.Errorf("empty response from API")
	}

	rawJSON := apiResp.Content[0].Text

	// Strip markdown code fences if present
	rawJSON = strings.TrimSpace(rawJSON)
	if strings.HasPrefix(rawJSON, "```") {
		lines := strings.Split(rawJSON, "\n")
		if len(lines) > 2 {
			lines = lines[1 : len(lines)-1]
			rawJSON = strings.Join(lines, "\n")
		}
	}

	var data VisionExtractedData
	if err := json.Unmarshal([]byte(rawJSON), &data); err != nil {
		return nil, rawJSON, fmt.Errorf("parse extracted JSON: %w (raw: %s)", err, rawJSON)
	}

	return &data, rawJSON, nil
}

// ── Extract from a single document ──────────────────────────

func (h *VisionExtractor) ExtractDocumentDataWithVision(pdfPath, docType string) (*VisionExtractedData, string, error) {
	imageBytes, err := convertPDFPageToImage(pdfPath)
	if err != nil {
		return nil, "", err
	}
	return h.callClaudeVision(imageBytes, docType)
}

// ── Backfill job ────────────────────────────────────────────

type visionJobStatus struct {
	mu        sync.Mutex
	Status    string `json:"status"`
	Processed int    `json:"processed"`
	Extracted int    `json:"extracted"`
	Failed    int    `json:"failed"`
	Total     int    `json:"total"`
}

var currentVisionJob = &visionJobStatus{Status: "idle"}

func (h *VisionExtractor) BackfillVisionExtraction() {
	db := h.sqlDB()

	currentVisionJob.mu.Lock()
	currentVisionJob.Status = "running"
	currentVisionJob.Processed = 0
	currentVisionJob.Extracted = 0
	currentVisionJob.Failed = 0
	currentVisionJob.mu.Unlock()

	rows, err := db.Query(`
		SELECT d.id, d.wf_scan_id, d.location_name, d.doc_type,
		       COALESCE(v.normalized_name, '') as vendor_name
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE d.customer_number IS NULL
		  AND d.wf_scan_id IS NOT NULL AND d.wf_scan_id != ''
		  AND d.doc_type = 'statement'
		ORDER BY d.doc_date DESC NULLS LAST`)
	if err != nil {
		log.Printf("[VisionExtract] query error: %v", err)
		currentVisionJob.mu.Lock()
		currentVisionJob.Status = "error"
		currentVisionJob.mu.Unlock()
		return
	}
	defer rows.Close()

	type docRow struct {
		id, scanID, location, docType, vendor string
	}
	var docs []docRow
	for rows.Next() {
		var d docRow
		var loc, dt sql.NullString
		if err := rows.Scan(&d.id, &d.scanID, &loc, &dt, &d.vendor); err != nil {
			continue
		}
		if loc.Valid {
			d.location = loc.String
		}
		if dt.Valid {
			d.docType = dt.String
		} else {
			d.docType = "statement"
		}
		docs = append(docs, d)
	}

	currentVisionJob.mu.Lock()
	currentVisionJob.Total = len(docs)
	currentVisionJob.mu.Unlock()

	log.Printf("[VisionExtract] Starting: %d documents to process", len(docs))

	for i, d := range docs {
		pdfPath := findPDFOnDisk(h.Cfg.PDFDir, d.location, d.scanID)
		if pdfPath == "" {
			log.Printf("[VisionExtract] %s: PDF not found on disk", d.scanID)
			currentVisionJob.mu.Lock()
			currentVisionJob.Failed++
			currentVisionJob.Processed++
			currentVisionJob.mu.Unlock()
			continue
		}

		data, _, err := h.ExtractDocumentDataWithVision(pdfPath, d.docType)
		if err != nil {
			log.Printf("[VisionExtract] %s (%s): vision error: %v", d.scanID, d.vendor, err)
			currentVisionJob.mu.Lock()
			currentVisionJob.Failed++
			currentVisionJob.Processed++
			currentVisionJob.mu.Unlock()
			continue
		}

		updated := false
		if data.CustomerNumber != "" {
			db.Exec(`UPDATE wf_documents SET customer_number = $1, customer_number_extracted_at = NOW() WHERE id = $2`,
				data.CustomerNumber, d.id)
			updated = true
		}
		if data.TotalAmount != nil && *data.TotalAmount != 0 {
			db.Exec(`UPDATE wf_documents SET amount = $1 WHERE id = $2 AND (amount IS NULL OR amount = 0)`,
				*data.TotalAmount, d.id)
		}
		if data.InvoiceNumber != "" {
			db.Exec(`UPDATE wf_documents SET invoice_number = $1 WHERE id = $2 AND (invoice_number IS NULL OR invoice_number = '')`,
				data.InvoiceNumber, d.id)
		}

		if updated {
			log.Printf("[VisionExtract] %s (%s): customer=%s amount=%v confidence=%s",
				d.scanID, d.vendor, data.CustomerNumber, data.TotalAmount, data.Confidence)
			currentVisionJob.mu.Lock()
			currentVisionJob.Extracted++
			currentVisionJob.mu.Unlock()
		} else {
			log.Printf("[VisionExtract] %s (%s): no customer number found, confidence=%s",
				d.scanID, d.vendor, data.Confidence)
			currentVisionJob.mu.Lock()
			currentVisionJob.Failed++
			currentVisionJob.mu.Unlock()
		}

		currentVisionJob.mu.Lock()
		currentVisionJob.Processed++
		currentVisionJob.mu.Unlock()

		if (i+1)%10 == 0 {
			currentVisionJob.mu.Lock()
			log.Printf("[VisionExtract] Progress: %d/%d processed, %d extracted, %d failed",
				currentVisionJob.Processed, currentVisionJob.Total, currentVisionJob.Extracted, currentVisionJob.Failed)
			currentVisionJob.mu.Unlock()
		}

		// Rate limit: ~1 request per second
		time.Sleep(1 * time.Second)
	}

	currentVisionJob.mu.Lock()
	currentVisionJob.Status = "complete"
	log.Printf("[VisionExtract] Done: %d processed, %d extracted, %d failed",
		currentVisionJob.Processed, currentVisionJob.Extracted, currentVisionJob.Failed)
	currentVisionJob.mu.Unlock()
}

// ── API Handlers ────────────────────────────────────────────

func (h *VisionExtractor) StartVisionExtraction(c *gin.Context) {
	currentVisionJob.mu.Lock()
	if currentVisionJob.Status == "running" {
		currentVisionJob.mu.Unlock()
		c.JSON(200, gin.H{"status": "already_running", "processed": currentVisionJob.Processed, "total": currentVisionJob.Total})
		return
	}
	currentVisionJob.mu.Unlock()

	go h.BackfillVisionExtraction()

	c.JSON(200, gin.H{"status": "started"})
}

func (h *VisionExtractor) GetVisionExtractionStatus(c *gin.Context) {
	currentVisionJob.mu.Lock()
	defer currentVisionJob.mu.Unlock()
	c.JSON(200, gin.H{
		"status":    currentVisionJob.Status,
		"processed": currentVisionJob.Processed,
		"total":     currentVisionJob.Total,
		"extracted": currentVisionJob.Extracted,
		"failed":    currentVisionJob.Failed,
	})
}

// TestVisionSingleDoc extracts data from a single document for testing.
func (h *VisionExtractor) TestVisionSingleDoc(c *gin.Context) {
	docID := c.Param("doc_id")
	db := h.sqlDB()

	var scanID, location, docType sql.NullString
	err := db.QueryRow(`SELECT wf_scan_id, location_name, doc_type FROM wf_documents WHERE id = $1`, docID).
		Scan(&scanID, &location, &docType)
	if err != nil {
		c.JSON(404, gin.H{"detail": "document not found"})
		return
	}
	if !scanID.Valid || scanID.String == "" {
		c.JSON(400, gin.H{"detail": "document has no wf_scan_id"})
		return
	}

	loc := ""
	if location.Valid {
		loc = location.String
	}
	dt := "statement"
	if docType.Valid {
		dt = docType.String
	}

	pdfPath := findPDFOnDisk(h.Cfg.PDFDir, loc, scanID.String)
	if pdfPath == "" {
		c.JSON(404, gin.H{"detail": "PDF not found on disk"})
		return
	}

	data, rawJSON, err := h.ExtractDocumentDataWithVision(pdfPath, dt)
	if err != nil {
		c.JSON(500, gin.H{"detail": err.Error(), "raw_response": rawJSON})
		return
	}

	c.JSON(200, gin.H{
		"pdf_path":       pdfPath,
		"extracted_data": data,
		"raw_json":       rawJSON,
	})
}

// ExtractAmounts re-runs Vision only on documents that have NULL amount.
func (h *VisionExtractor) ExtractAmounts(c *gin.Context) {
	db := h.sqlDB()

	rows, err := db.Query(`
		SELECT d.id, d.wf_scan_id, d.location_name, d.doc_type,
		       COALESCE(v.normalized_name, '') as vendor_name
		FROM wf_documents d
		LEFT JOIN vendors v ON v.id = d.vendor_id
		WHERE d.amount IS NULL
		  AND d.wf_scan_id IS NOT NULL AND d.wf_scan_id != ''
		  AND d.doc_type = 'statement'
		  AND d.customer_number_extracted_at IS NOT NULL
		ORDER BY d.doc_date DESC NULLS LAST`)
	if err != nil {
		c.JSON(500, gin.H{"detail": err.Error()})
		return
	}
	defer rows.Close()

	type docRow struct {
		id, scanID, location, docType, vendor string
	}
	var docs []docRow
	for rows.Next() {
		var d docRow
		var loc, dt sql.NullString
		if err := rows.Scan(&d.id, &d.scanID, &loc, &dt, &d.vendor); err != nil {
			continue
		}
		if loc.Valid {
			d.location = loc.String
		}
		if dt.Valid {
			d.docType = dt.String
		} else {
			d.docType = "statement"
		}
		docs = append(docs, d)
	}

	processed := 0
	saved := 0
	for _, d := range docs {
		pdfPath := findPDFOnDisk(h.Cfg.PDFDir, d.location, d.scanID)
		if pdfPath == "" {
			processed++
			continue
		}
		data, _, err := h.ExtractDocumentDataWithVision(pdfPath, d.docType)
		if err != nil {
			log.Printf("[VisionAmounts] %s (%s): error: %v", d.scanID, d.vendor, err)
			processed++
			continue
		}
		if data.TotalAmount != nil {
			db.Exec(`UPDATE wf_documents SET amount = $1 WHERE id = $2 AND amount IS NULL`,
				*data.TotalAmount, d.id)
			saved++
			log.Printf("[VisionAmounts] %s (%s): saved amount=%.2f", d.scanID, d.vendor, *data.TotalAmount)
		}
		processed++
		time.Sleep(1 * time.Second)
	}

	c.JSON(200, gin.H{"processed": processed, "amounts_saved": saved})
}
