package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/config"
	"github.com/ESCAutoGroupX/business-analytics-api/internal/models"
)

type DocumentHandler struct {
	GormDB *gorm.DB
	Cfg    *config.Config
}

func (h *DocumentHandler) sqlDB() *sql.DB {
	db, _ := h.GormDB.DB()
	return db
}

// shopAddresses maps location codes to address fragments for matching.
var shopAddresses = map[string][]string{
	"ALP": {"865 Owens Lake", "Alpharetta", "30004"},
	"CED": {"Cedar Springs", "Dallas"},
	"DUL": {"Duluth"},
	"HIG": {"Highlands"},
	"HOU": {"Houston"},
	"PIE": {"Piedmont"},
	"PRE": {"Preston Road", "Dallas"},
	"ROS": {"Roswell"},
	"SAN": {"Sandy Springs"},
	"TRA": {"Tracy"},
}

// ── POST /documents/upload ────────────────────────────────────

func (h *DocumentHandler) Upload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "file is required"})
		return
	}

	// Validate file type by extension
	ext := strings.ToLower(filepath.Ext(file.Filename))
	allowedExts := map[string]bool{".pdf": true, ".jpg": true, ".jpeg": true, ".png": true}
	if !allowedExts[ext] {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "unsupported file type; allowed: PDF, JPG, PNG"})
		return
	}

	// Save to local filesystem
	timestamp := time.Now().UnixMilli()
	safeFilename := fmt.Sprintf("%d_%s", timestamp, file.Filename)
	dir := "static/documents"
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("documents: failed to create directory %s: %v", dir, err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to create upload directory"})
		return
	}
	savePath := filepath.Join(dir, safeFilename)
	if err := c.SaveUploadedFile(file, savePath); err != nil {
		log.Printf("documents: failed to save file: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to save file"})
		return
	}

	// Read file content for OCR
	fileBytes, err := os.ReadFile(savePath)
	if err != nil {
		log.Printf("documents: failed to read saved file: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to read file"})
		return
	}
	base64Data := base64.StdEncoding.EncodeToString(fileBytes)

	// Call Claude Vision API
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		log.Printf("documents: ANTHROPIC_API_KEY not configured")
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "ANTHROPIC_API_KEY not configured"})
		return
	}

	ocrResult, ocrRaw, err := h.callClaudeVision(apiKey, base64Data, ext)
	if err != nil {
		log.Printf("documents: Claude Vision API error: %v", err)
		// Save document even if OCR fails
		doc := models.Document{
			Filename: file.Filename,
			FilePath: savePath,
			FileURL:  "/documents/file/" + safeFilename,
			Status:   "ocr_failed",
		}
		h.GormDB.Create(&doc)
		c.JSON(http.StatusOK, gin.H{"document": doc, "ocr_error": err.Error()})
		return
	}

	// Match location from addresses
	locationCode := matchLocation(ocrResult.ShipToAddress, ocrResult.BillToAddress)

	// Build document record
	doc := models.Document{
		Filename:     file.Filename,
		FilePath:     savePath,
		FileURL:      "/documents/file/" + safeFilename,
		DocumentType: docStrPtr(ocrResult.DocumentType),
		VendorName:   docStrPtr(ocrResult.VendorName),
		VendorAddress: docStrPtr(ocrResult.VendorAddress),
		DocumentDate: docStrPtr(ocrResult.DocumentDate),
		DocumentNumber: docStrPtr(ocrResult.DocumentNumber),
		Status:       "pending",
		OCRRaw:       docStrPtr(ocrRaw),
	}

	if ocrResult.TotalAmount != 0 {
		doc.TotalAmount = &ocrResult.TotalAmount
	}
	if ocrResult.TaxAmount != 0 {
		doc.TaxAmount = &ocrResult.TaxAmount
	}

	// Serialize line items as JSON
	if ocrResult.LineItems != nil {
		liJSON, _ := json.Marshal(ocrResult.LineItems)
		liStr := string(liJSON)
		doc.LineItems = &liStr
	}

	if locationCode != "" {
		doc.LocationCode = &locationCode
		locName := locationCode // Use code as name fallback
		doc.Location = &locName
	}

	// Create document first so we have the ID for PO number
	if err := h.GormDB.Create(&doc).Error; err != nil {
		log.Printf("documents: failed to save document: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to save document"})
		return
	}

	// Generate PO number
	poNumber := generatePONumber(ocrResult, locationCode, doc.ID)
	if poNumber != "" {
		doc.PONumber = &poNumber
		h.GormDB.Model(&doc).Update("po_number", poNumber)
	}

	// Auto-match to transactions
	matchedTxnID := h.autoMatchTransaction(ocrResult.TotalAmount, ocrResult.DocumentDate, ocrResult.VendorName)
	if matchedTxnID != "" {
		doc.MatchedTransactionID = &matchedTxnID
		doc.Status = "matched"
		h.GormDB.Model(&doc).Updates(map[string]interface{}{
			"matched_transaction_id": matchedTxnID,
			"status":                 "matched",
		})
	}

	c.JSON(http.StatusOK, gin.H{"document": doc})
}

// ── Claude Vision API ─────────────────────────────────────────

type ocrExtractedData struct {
	DocumentType  string        `json:"document_type"`
	VendorName    string        `json:"vendor_name"`
	VendorAddress string        `json:"vendor_address"`
	DocumentDate  string        `json:"document_date"`
	DocumentNumber string       `json:"document_number"`
	TotalAmount   float64       `json:"total_amount"`
	TaxAmount     float64       `json:"tax_amount"`
	LineItems     []interface{} `json:"line_items"`
	ShipToAddress string        `json:"ship_to_address"`
	BillToAddress string        `json:"bill_to_address"`
	POReferences  []string      `json:"po_references"`
}

func (h *DocumentHandler) callClaudeVision(apiKey, base64Data, ext string) (*ocrExtractedData, string, error) {
	// Determine media type and content block type
	var mediaType string
	var contentBlockType string

	switch ext {
	case ".jpg", ".jpeg":
		mediaType = "image/jpeg"
		contentBlockType = "image"
	case ".png":
		mediaType = "image/png"
		contentBlockType = "image"
	case ".pdf":
		mediaType = "application/pdf"
		contentBlockType = "document"
	default:
		return nil, "", fmt.Errorf("unsupported file extension: %s", ext)
	}

	userPrompt := `Extract all information from this automotive industry invoice/statement and return JSON only.
Return a JSON object with these fields:
- document_type (e.g. "invoice", "statement", "credit_memo", "purchase_order")
- vendor_name
- vendor_address
- document_date (YYYY-MM-DD format)
- document_number
- total_amount (number)
- tax_amount (number)
- line_items (array of objects with: description, quantity, unit_price, amount, part_number if available)
- ship_to_address
- bill_to_address
- po_references (array of PO numbers referenced)

Return JSON only, no markdown fences.`

	systemPrompt := "You are an expert at reading automotive industry invoices and statements. Extract all information and return JSON only."

	// Build content block based on file type
	fileBlock := map[string]interface{}{
		"type": contentBlockType,
		"source": map[string]interface{}{
			"type":       "base64",
			"media_type": mediaType,
			"data":       base64Data,
		},
	}

	textBlock := map[string]interface{}{
		"type": "text",
		"text": userPrompt,
	}

	reqBody := map[string]interface{}{
		"model":      "claude-sonnet-4-20250514",
		"max_tokens": 4096,
		"system":     systemPrompt,
		"messages": []interface{}{
			map[string]interface{}{
				"role":    "user",
				"content": []interface{}{fileBlock, textBlock},
			},
		},
	}

	bodyBytes, _ := json.Marshal(reqBody)

	httpReq, _ := http.NewRequestWithContext(context.Background(), "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", apiKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, "", fmt.Errorf("anthropic API call failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return nil, string(respBody), fmt.Errorf("anthropic API returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response
	var apiResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, string(respBody), fmt.Errorf("failed to parse anthropic response: %w", err)
	}

	if len(apiResp.Content) == 0 {
		return nil, string(respBody), fmt.Errorf("empty response from anthropic")
	}

	text := apiResp.Content[0].Text
	rawText := text

	// Strip markdown fences if present
	text = stripJSONFences(text)

	var result ocrExtractedData
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		log.Printf("documents: failed to parse OCR output: %v\nRaw: %s", err, text)
		return nil, rawText, fmt.Errorf("failed to parse OCR output: %w", err)
	}

	return &result, rawText, nil
}

// ── Location matching ─────────────────────────────────────────

func matchLocation(shipTo, billTo string) string {
	combined := strings.ToLower(shipTo + " " + billTo)
	if combined == " " {
		return ""
	}
	for code, fragments := range shopAddresses {
		for _, frag := range fragments {
			if strings.Contains(combined, strings.ToLower(frag)) {
				return code
			}
		}
	}
	return ""
}

// ── PO number generation ──────────────────────────────────────

func generatePONumber(data *ocrExtractedData, locationCode string, docID int) string {
	if locationCode == "" {
		locationCode = "GEN"
	}

	// Detect type from line items and vendor
	poType := detectPOType(data)

	seq := fmt.Sprintf("%04d", docID)
	return fmt.Sprintf("%s-%s-%s", poType, locationCode, seq)
}

func detectPOType(data *ocrExtractedData) string {
	vendorLower := strings.ToLower(data.VendorName)

	// Check vendor name for clues
	toolVendors := []string{"snap-on", "snapon", "matco", "mac tools", "cornwell"}
	for _, tv := range toolVendors {
		if strings.Contains(vendorLower, tv) {
			return "TOOL"
		}
	}

	officeVendors := []string{"staples", "office depot", "amazon"}
	for _, ov := range officeVendors {
		if strings.Contains(vendorLower, ov) {
			return "OFS"
		}
	}

	supplyVendors := []string{"cintas", "unifirst", "grainger", "fastenal"}
	for _, sv := range supplyVendors {
		if strings.Contains(vendorLower, sv) {
			return "SHP"
		}
	}

	// Check line items for parts indicators
	if data.LineItems != nil {
		liJSON, _ := json.Marshal(data.LineItems)
		liStr := strings.ToLower(string(liJSON))
		if strings.Contains(liStr, "part") || strings.Contains(liStr, "filter") ||
			strings.Contains(liStr, "brake") || strings.Contains(liStr, "rotor") ||
			strings.Contains(liStr, "oil") || strings.Contains(liStr, "fluid") {
			return "INV"
		}
	}

	// Default to INV for parts invoices (most common in auto shops)
	return "INV"
}

// ── Transaction auto-matching ─────────────────────────────────

func (h *DocumentHandler) autoMatchTransaction(amount float64, dateStr, vendorName string) string {
	if amount == 0 || dateStr == "" {
		return ""
	}

	docDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return ""
	}

	dateFrom := docDate.AddDate(0, 0, -7).Format("2006-01-02")
	dateTo := docDate.AddDate(0, 0, 7).Format("2006-01-02")

	db := h.sqlDB()
	if db == nil {
		return ""
	}

	rows, err := db.Query(
		`SELECT id, date, amount, name, vendor FROM transactions
		 WHERE ABS(amount - $1) < 0.02
		 AND date BETWEEN $2 AND $3`,
		math.Abs(amount), dateFrom, dateTo,
	)
	if err != nil {
		log.Printf("documents: auto-match query error: %v", err)
		return ""
	}
	defer rows.Close()

	vendorLower := strings.ToLower(vendorName)

	type txnMatch struct {
		id     string
		score  int
	}

	var bestMatch txnMatch
	for rows.Next() {
		var id, date string
		var txnAmount float64
		var name, vendor sql.NullString
		if err := rows.Scan(&id, &date, &txnAmount, &name, &vendor); err != nil {
			continue
		}

		score := 1 // Amount match

		// Check vendor name similarity
		if vendor.Valid && strings.Contains(strings.ToLower(vendor.String), vendorLower) {
			score += 2
		}
		if name.Valid && strings.Contains(strings.ToLower(name.String), vendorLower) {
			score += 2
		}

		if score > bestMatch.score {
			bestMatch = txnMatch{id: id, score: score}
		}
	}

	return bestMatch.id
}

// ── GET /documents ────────────────────────────────────────────

func (h *DocumentHandler) List(c *gin.Context) {
	query := h.GormDB.Model(&models.Document{}).Where("is_deleted = false")

	// Filters
	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if location := c.Query("location"); location != "" {
		query = query.Where("location_code = ?", location)
	}
	if dateFrom := c.Query("date_from"); dateFrom != "" {
		query = query.Where("document_date >= ?", dateFrom)
	}
	if dateTo := c.Query("date_to"); dateTo != "" {
		query = query.Where("document_date <= ?", dateTo)
	}
	if search := c.Query("search"); search != "" {
		like := "%" + search + "%"
		query = query.Where("vendor_name ILIKE ? OR po_number ILIKE ?", like, like)
	}

	// Count total
	var total int64
	query.Count(&total)

	// Pagination
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	perPage, _ := strconv.Atoi(c.DefaultQuery("per_page", "50"))
	if page < 1 {
		page = 1
	}
	if perPage < 1 || perPage > 200 {
		perPage = 50
	}
	offset := (page - 1) * perPage

	var docs []models.Document
	query.Order("created_at DESC").Offset(offset).Limit(perPage).Find(&docs)

	c.JSON(http.StatusOK, gin.H{
		"data":  docs,
		"total": total,
	})
}

// ── GET /documents/:id ────────────────────────────────────────

func (h *DocumentHandler) Get(c *gin.Context) {
	id := c.Param("id")

	var doc models.Document
	result := h.GormDB.Where("id = ? AND is_deleted = false", id).First(&doc)
	if result.Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	c.JSON(http.StatusOK, doc)
}

// ── PATCH /documents/:id ──────────────────────────────────────

func (h *DocumentHandler) Update(c *gin.Context) {
	id := c.Param("id")

	var req struct {
		Status               *string `json:"status"`
		VendorName           *string `json:"vendor_name"`
		DocumentType         *string `json:"document_type"`
		LocationCode         *string `json:"location_code"`
		MatchedTransactionID *string `json:"matched_transaction_id"`
		MatchedXeroInvoiceID *int    `json:"matched_xero_invoice_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid request body"})
		return
	}

	updates := map[string]interface{}{}
	if req.Status != nil {
		updates["status"] = *req.Status
	}
	if req.VendorName != nil {
		updates["vendor_name"] = *req.VendorName
	}
	if req.DocumentType != nil {
		updates["document_type"] = *req.DocumentType
	}
	if req.LocationCode != nil {
		updates["location_code"] = *req.LocationCode
	}
	if req.MatchedTransactionID != nil {
		updates["matched_transaction_id"] = *req.MatchedTransactionID
	}
	if req.MatchedXeroInvoiceID != nil {
		updates["matched_xero_invoice_id"] = *req.MatchedXeroInvoiceID
	}

	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "no fields to update"})
		return
	}

	result := h.GormDB.Model(&models.Document{}).Where("id = ? AND is_deleted = false", id).Updates(updates)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to update document"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	var doc models.Document
	h.GormDB.First(&doc, id)
	c.JSON(http.StatusOK, doc)
}

// ── DELETE /documents/:id ─────────────────────────────────────

func (h *DocumentHandler) Delete(c *gin.Context) {
	id := c.Param("id")

	result := h.GormDB.Model(&models.Document{}).Where("id = ? AND is_deleted = false", id).Update("is_deleted", true)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to delete document"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"detail": "document deleted"})
}

// ── POST /documents/:id/match ─────────────────────────────────

func (h *DocumentHandler) Match(c *gin.Context) {
	id := c.Param("id")

	var req struct {
		TransactionID  *string `json:"transaction_id"`
		XeroInvoiceID  *int    `json:"xero_invoice_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid request body"})
		return
	}

	if req.TransactionID == nil && req.XeroInvoiceID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "provide transaction_id or xero_invoice_id"})
		return
	}

	updates := map[string]interface{}{
		"status": "matched",
	}
	if req.TransactionID != nil {
		updates["matched_transaction_id"] = *req.TransactionID
	}
	if req.XeroInvoiceID != nil {
		updates["matched_xero_invoice_id"] = *req.XeroInvoiceID
	}

	result := h.GormDB.Model(&models.Document{}).Where("id = ? AND is_deleted = false", id).Updates(updates)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to match document"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	var doc models.Document
	h.GormDB.First(&doc, id)
	c.JSON(http.StatusOK, doc)
}

// ── GET /documents/:id/file ───────────────────────────────────

func (h *DocumentHandler) ServeFile(c *gin.Context) {
	id := c.Param("id")

	var doc models.Document
	result := h.GormDB.Where("id = ? AND is_deleted = false", id).First(&doc)
	if result.Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	if _, err := os.Stat(doc.FilePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"detail": "file not found on disk"})
		return
	}

	c.File(doc.FilePath)
}

// ── GET /documents/summary ────────────────────────────────────

func (h *DocumentHandler) Summary(c *gin.Context) {
	db := h.sqlDB()
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "database error"})
		return
	}

	var total, pending, matched, unmatched, thisMonth int

	_ = db.QueryRow(`SELECT COUNT(*) FROM documents WHERE is_deleted = false`).Scan(&total)
	_ = db.QueryRow(`SELECT COUNT(*) FROM documents WHERE is_deleted = false AND status = 'pending'`).Scan(&pending)
	_ = db.QueryRow(`SELECT COUNT(*) FROM documents WHERE is_deleted = false AND status = 'matched'`).Scan(&matched)
	_ = db.QueryRow(`SELECT COUNT(*) FROM documents WHERE is_deleted = false AND status != 'matched'`).Scan(&unmatched)

	now := time.Now()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	_ = db.QueryRow(`SELECT COUNT(*) FROM documents WHERE is_deleted = false AND created_at >= $1`, monthStart).Scan(&thisMonth)

	c.JSON(http.StatusOK, gin.H{
		"total":      total,
		"pending":    pending,
		"matched":    matched,
		"unmatched":  unmatched,
		"this_month": thisMonth,
	})
}

// ── Helpers ───────────────────────────────────────────────────

func docStrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
