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
	"mime/multipart"
	"net/http"
	"net/smtp"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	syncAtomic "sync/atomic"
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

	// ── Step 0: Multi-invoice PDF detection ──────────────────
	groups, tmpDir, splitErr := h.detectMultiInvoicePDF(apiKey, savePath, ext)
	if tmpDir != "" {
		defer os.RemoveAll(tmpDir)
	}

	if splitErr != nil {
		log.Printf("documents: split detection error (proceeding as single): %v", splitErr)
	}

	if len(groups) > 1 {
		// Multi-invoice PDF — create parent + child documents
		docs := h.processMultiInvoicePDF(apiKey, file.Filename, savePath, safeFilename, ext, groups, fileBytes)
		c.JSON(http.StatusOK, gin.H{"documents": docs, "split": true, "invoice_count": len(groups)})
		return
	}

	// ── Single document processing (original flow) ───────────
	ocrResult, ocrRaw, confidence, agentVersion, err := h.runPipeline(apiKey, base64Data, ext)
	if err != nil {
		log.Printf("documents: pipeline error: %v", err)
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

	doc := h.buildDocumentFromPipeline(file.Filename, savePath, safeFilename, ocrResult, ocrRaw, confidence, agentVersion, nil, nil)

	if err := h.GormDB.Create(&doc).Error; err != nil {
		log.Printf("documents: failed to save document: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "failed to save document"})
		return
	}

	// Legacy auto-match fallback
	h.tryAutoMatch(&doc, ocrResult)

	go h.forwardToWickedFile(&doc, fileBytes)

	c.JSON(http.StatusOK, gin.H{"document": doc})
}

// ── Claude Vision API ─────────────────────────────────────────

type ocrExtractedData struct {
	DocumentType        string        `json:"document_type"`
	VendorName          string        `json:"vendor_name"`
	VendorAddress       string        `json:"vendor_address"`
	DocumentDate        string        `json:"document_date"`
	DocumentNumber      string        `json:"document_number"`
	TotalAmount         float64       `json:"total_amount"`
	TaxAmount           float64       `json:"tax_amount"`
	LineItems           []interface{} `json:"line_items"`
	ShipToAddress       string        `json:"ship_to_address"`
	BillToAddress       string        `json:"bill_to_address"`
	POReferences        []string      `json:"po_references"`
	VendorPONumber      string        `json:"vendor_po_number"`
	VendorInvoiceNumber string        `json:"vendor_invoice_number"`
	OrderNumber         string        `json:"order_number"`
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
- document_type (e.g. "INVOICE", "STATEMENT", "RECEIPT", "CHECK", "CREDIT_MEMO", "PURCHASE_ORDER")
- vendor_name
- vendor_address
- document_date (YYYY-MM-DD format)
- document_number (the document's own reference number)
- total_amount (number)
- tax_amount (number)
- line_items (array of objects with: description, quantity, unit_price, amount, part_number if available)
- ship_to_address
- bill_to_address
- po_references (array of PO numbers referenced)
- vendor_po_number: The P.O. No. or PO Number field on the invoice — this is the customer's purchase order number (e.g. 41381). This is NOT the invoice number.
- vendor_invoice_number: The Invoice No. or Invoice Number field (e.g. 62119698)
- order_number: The Order No. field if present

IMPORTANT for automotive parts invoices (WorldPac, NAPA, OReilly, AutoZone, Dorman, etc.):
- P.O. No. = the repair shop's purchase order number → put in vendor_po_number
- Invoice No. = the vendor's invoice reference number → put in vendor_invoice_number
These are DIFFERENT fields — do not confuse them. They appear in separate labeled fields on the document.

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
		query = query.Where("vendor_name ILIKE ? OR vendor_po_number ILIKE ? OR vendor_invoice_number ILIKE ? OR po_number ILIKE ?", like, like, like, like)
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
		VendorPONumber       *string `json:"vendor_po_number"`
		VendorInvoiceNumber  *string `json:"vendor_invoice_number"`
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
	if req.VendorPONumber != nil {
		updates["vendor_po_number"] = *req.VendorPONumber
	}
	if req.VendorInvoiceNumber != nil {
		updates["vendor_invoice_number"] = *req.VendorInvoiceNumber
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

// ── POST /documents/:id/record-payment ───────────────────────

func (h *DocumentHandler) RecordPayment(c *gin.Context) {
	id := c.Param("id")

	var doc models.Document
	if err := h.GormDB.Where("id = ? AND is_deleted = false", id).First(&doc).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	// If internal PO already assigned, return it
	if doc.PONumber != nil && *doc.PONumber != "" {
		c.JSON(http.StatusOK, gin.H{"document": doc, "message": "internal PO already assigned"})
		return
	}

	// Read OCR data to detect PO type
	var ocrData ocrExtractedData
	if doc.OCRRaw != nil {
		text := stripJSONFences(*doc.OCRRaw)
		json.Unmarshal([]byte(text), &ocrData)
	}
	// Fallback: populate from doc fields
	if ocrData.VendorName == "" && doc.VendorName != nil {
		ocrData.VendorName = *doc.VendorName
	}

	locationCode := "GEN"
	if doc.LocationCode != nil && *doc.LocationCode != "" {
		locationCode = *doc.LocationCode
	}

	poNumber := generatePONumber(&ocrData, locationCode, doc.ID)
	doc.PONumber = &poNumber
	h.GormDB.Model(&doc).Update("po_number", poNumber)

	c.JSON(http.StatusOK, gin.H{"document": doc, "po_number": poNumber})
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

// ── Document builder helpers ─────────────────────────────────

func (h *DocumentHandler) buildDocumentFromPipeline(
	filename, filePath, safeFilename string,
	ocr *ocrExtractedData, ocrRaw string,
	confidence float64, agentVersion string,
	parentID *int, pageInfo *string,
) models.Document {
	locationCode := matchLocation(ocr.ShipToAddress, ocr.BillToAddress)

	status := "pending"
	if confidence > 0.85 {
		status = "auto_matched"
	} else if confidence < 0.60 {
		status = "needs_review"
	}

	doc := models.Document{
		Filename:            filename,
		FilePath:            filePath,
		FileURL:             "/documents/file/" + safeFilename,
		DocumentType:        docStrPtr(ocr.DocumentType),
		VendorName:          docStrPtr(ocr.VendorName),
		VendorAddress:       docStrPtr(ocr.VendorAddress),
		DocumentDate:        docStrPtr(ocr.DocumentDate),
		DocumentNumber:      docStrPtr(ocr.DocumentNumber),
		VendorPONumber:      docStrPtr(ocr.VendorPONumber),
		VendorInvoiceNumber: docStrPtr(ocr.VendorInvoiceNumber),
		OrderNumber:         docStrPtr(ocr.OrderNumber),
		Status:              status,
		OCRRaw:              docStrPtr(ocrRaw),
		OCRConfidence:       &confidence,
		OCRAgentVersion:     docStrPtr(agentVersion),
		ParentDocumentID:    parentID,
		PageNumbers:         pageInfo,
	}

	if ocr.TotalAmount != 0 {
		doc.TotalAmount = &ocr.TotalAmount
	}
	if ocr.TaxAmount != 0 {
		doc.TaxAmount = &ocr.TaxAmount
	}
	if ocr.LineItems != nil {
		liJSON, _ := json.Marshal(ocr.LineItems)
		liStr := string(liJSON)
		doc.LineItems = &liStr
	}
	if locationCode != "" {
		doc.LocationCode = &locationCode
		doc.Location = &locationCode
	}

	// Check agent4 match
	for _, ref := range ocr.POReferences {
		if strings.HasPrefix(ref, "agent4_match:") {
			matchID := strings.TrimPrefix(ref, "agent4_match:")
			doc.MatchedTransactionID = &matchID
			if status != "auto_matched" {
				doc.Status = "matched"
			}
			break
		}
	}

	return doc
}

func (h *DocumentHandler) tryAutoMatch(doc *models.Document, ocr *ocrExtractedData) {
	// Skip if already matched by agent4
	if doc.MatchedTransactionID != nil {
		return
	}
	matchedTxnID := h.autoMatchTransaction(ocr.TotalAmount, ocr.DocumentDate, ocr.VendorName)
	if matchedTxnID != "" {
		doc.MatchedTransactionID = &matchedTxnID
		doc.Status = "matched"
		h.GormDB.Model(doc).Updates(map[string]interface{}{
			"matched_transaction_id": matchedTxnID,
			"status":                 "matched",
		})
	}
}

func (h *DocumentHandler) processMultiInvoicePDF(
	apiKey, origFilename, origPath, origSafeFilename, ext string,
	groups []invoiceGroup, origFileBytes []byte,
) []models.Document {
	// Create parent document record
	pageCount := 0
	for _, g := range groups {
		pageCount += len(g.Pages)
	}
	pc := pageCount
	parentDoc := models.Document{
		Filename:  origFilename,
		FilePath:  origPath,
		FileURL:   "/documents/file/" + origSafeFilename,
		Status:    "split",
		PageCount: &pc,
	}
	h.GormDB.Create(&parentDoc)
	log.Printf("splitter: created parent doc %d with %d pages, %d invoices", parentDoc.ID, pageCount, len(groups))

	var childDocs []models.Document

	for _, group := range groups {
		// Build page numbers string
		pageStrs := make([]string, len(group.Pages))
		for i, p := range group.Pages {
			pageStrs[i] = strconv.Itoa(p)
		}
		pageNumStr := strings.Join(pageStrs, ",")
		groupPageCount := len(group.Pages)

		// Extract pages into a child PDF
		childFilename := fmt.Sprintf("%s_inv_%s.pdf",
			strings.TrimSuffix(origFilename, filepath.Ext(origFilename)),
			group.InvoiceNumber)
		childSafe := fmt.Sprintf("%d_%s", time.Now().UnixMilli(), childFilename)
		childPath := filepath.Join("static/documents", childSafe)

		if err := extractPDFPageRange(origPath, childPath, group.Pages); err != nil {
			log.Printf("splitter: failed to extract pages %v for invoice %s: %v", group.Pages, group.InvoiceNumber, err)
			continue
		}

		// Read child PDF for pipeline
		childBytes, err := os.ReadFile(childPath)
		if err != nil {
			log.Printf("splitter: failed to read child PDF: %v", err)
			continue
		}
		childB64 := base64.StdEncoding.EncodeToString(childBytes)

		// Run full pipeline on the child PDF
		ocrResult, ocrRaw, confidence, agentVersion, err := h.runPipeline(apiKey, childB64, ext)
		if err != nil {
			log.Printf("splitter: pipeline failed for invoice %s: %v", group.InvoiceNumber, err)
			// Still create a record with minimal info
			pID := parentDoc.ID
			childDoc := models.Document{
				Filename:         childFilename,
				FilePath:         childPath,
				FileURL:          "/documents/file/" + childSafe,
				Status:           "ocr_failed",
				ParentDocumentID: &pID,
				PageCount:        &groupPageCount,
				PageNumbers:      &pageNumStr,
			}
			h.GormDB.Create(&childDoc)
			childDocs = append(childDocs, childDoc)
			continue
		}

		pID := parentDoc.ID
		childDoc := h.buildDocumentFromPipeline(childFilename, childPath, childSafe,
			ocrResult, ocrRaw, confidence, agentVersion, &pID, &pageNumStr)
		childDoc.PageCount = &groupPageCount

		if err := h.GormDB.Create(&childDoc).Error; err != nil {
			log.Printf("splitter: failed to save child doc: %v", err)
			continue
		}

		h.tryAutoMatch(&childDoc, ocrResult)

		go h.forwardToWickedFile(&childDoc, childBytes)

		childDocs = append(childDocs, childDoc)
		log.Printf("splitter: created child doc %d for invoice %s (pages %s, confidence=%.2f)",
			childDoc.ID, group.InvoiceNumber, pageNumStr, confidence)
	}

	return childDocs
}

// ── Rescan All Documents ──────────────────────────────────────

var (
	rescanRunning   int32 // atomic: 1 = running, 0 = idle
	rescanTotal     int32
	rescanCompleted int32
)

// POST /documents/rescan-all
func (h *DocumentHandler) RescanAll(c *gin.Context) {
	// Prevent concurrent rescans
	if !atomicCAS(&rescanRunning, 0, 1) {
		c.JSON(http.StatusConflict, gin.H{"detail": "rescan already in progress", "completed": atomicLoad(&rescanCompleted), "total": atomicLoad(&rescanTotal)})
		return
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		atomicStore(&rescanRunning, 0)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "ANTHROPIC_API_KEY not configured"})
		return
	}

	var docs []models.Document
	h.GormDB.Where("is_deleted = false").Find(&docs)

	total := int32(len(docs))
	atomicStore(&rescanTotal, total)
	atomicStore(&rescanCompleted, 0)

	c.JSON(http.StatusOK, gin.H{"status": "started", "total": total})

	go func() {
		defer atomicStore(&rescanRunning, 0)

		for _, doc := range docs {
			// Skip child documents (they'll be recreated from parent)
			if doc.ParentDocumentID != nil {
				atomicAdd(&rescanCompleted, 1)
				continue
			}

			log.Printf("Rescanning document %d: %s", doc.ID, doc.Filename)
			h.reprocessSingleDocument(apiKey, doc)
			atomicAdd(&rescanCompleted, 1)
		}
		log.Printf("Rescan complete: %d/%d documents processed", atomicLoad(&rescanCompleted), total)
	}()
}

// GET /documents/rescan-status
func (h *DocumentHandler) RescanStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"completed": atomicLoad(&rescanCompleted),
		"total":     atomicLoad(&rescanTotal),
		"running":   atomicLoad(&rescanRunning) == 1,
	})
}

// POST /documents/:id/reprocess — reprocess a single document through the full pipeline
func (h *DocumentHandler) Reprocess(c *gin.Context) {
	id := c.Param("id")

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "ANTHROPIC_API_KEY not configured"})
		return
	}

	var doc models.Document
	if err := h.GormDB.Where("id = ? AND is_deleted = false", id).First(&doc).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "document not found"})
		return
	}

	result := h.reprocessSingleDocument(apiKey, doc)
	c.JSON(http.StatusOK, result)
}

// reprocessSingleDocument runs the full pipeline (including page splitting) on one document.
func (h *DocumentHandler) reprocessSingleDocument(apiKey string, doc models.Document) gin.H {
	fileBytes, err := os.ReadFile(doc.FilePath)
	if err != nil {
		log.Printf("reprocess doc %d: cannot read file %s: %v", doc.ID, doc.FilePath, err)
		return gin.H{"document_id": doc.ID, "error": "cannot read file"}
	}

	ext := strings.ToLower(filepath.Ext(doc.Filename))

	// Delete any existing children so reprocessing is idempotent
	if res := h.GormDB.Exec("DELETE FROM documents WHERE parent_document_id = ?", doc.ID); res.Error != nil {
		log.Printf("reprocess doc %d: failed to delete old children: %v", doc.ID, res.Error)
	} else if res.RowsAffected > 0 {
		log.Printf("reprocess doc %d: deleted %d old children", doc.ID, res.RowsAffected)
	}

	// Try multi-invoice splitting for PDFs
	groups, tmpDir, splitErr := h.detectMultiInvoicePDF(apiKey, doc.FilePath, ext)
	if tmpDir != "" {
		defer os.RemoveAll(tmpDir)
	}
	if splitErr != nil {
		log.Printf("reprocess doc %d: split detection error: %v", doc.ID, splitErr)
	}

	if len(groups) > 1 {
		// Multi-invoice — create new children (old children already deleted above)
		safeFilename := filepath.Base(doc.FilePath)
		childDocs := h.processMultiInvoicePDFWithParent(apiKey, doc.Filename, doc.FilePath, safeFilename, ext, groups, fileBytes, doc.ID)

		// Update parent status
		pc := 0
		for _, g := range groups {
			pc += len(g.Pages)
		}
		h.GormDB.Model(&doc).Updates(map[string]interface{}{
			"status":     "split",
			"page_count": pc,
		})

		log.Printf("reprocess doc %d: split into %d invoices", doc.ID, len(childDocs))
		return gin.H{"document_id": doc.ID, "split": true, "invoice_count": len(childDocs)}
	}

	// Single document — run pipeline directly
	b64 := base64.StdEncoding.EncodeToString(fileBytes)
	ocrResult, ocrRaw, confidence, agentVersion, err := h.runPipeline(apiKey, b64, ext)
	if err != nil {
		log.Printf("reprocess doc %d: pipeline error: %v", doc.ID, err)
		return gin.H{"document_id": doc.ID, "error": err.Error()}
	}

	locationCode := matchLocation(ocrResult.ShipToAddress, ocrResult.BillToAddress)

	status := "pending"
	if confidence > 0.85 {
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
		liJSON, _ := json.Marshal(ocrResult.LineItems)
		updates["line_items"] = string(liJSON)
	}
	if locationCode != "" {
		updates["location_code"] = locationCode
		updates["location"] = locationCode
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
	log.Printf("reprocess doc %d: confidence=%.2f status=%s", doc.ID, confidence, updates["status"])
	return gin.H{"document_id": doc.ID, "confidence": confidence, "status": updates["status"]}
}

// processMultiInvoicePDFWithParent is like processMultiInvoicePDF but uses an existing parent ID.
func (h *DocumentHandler) processMultiInvoicePDFWithParent(
	apiKey, origFilename, origPath, origSafeFilename, ext string,
	groups []invoiceGroup, origFileBytes []byte, parentID int,
) []models.Document {
	var childDocs []models.Document

	for _, group := range groups {
		pageStrs := make([]string, len(group.Pages))
		for i, p := range group.Pages {
			pageStrs[i] = strconv.Itoa(p)
		}
		pageNumStr := strings.Join(pageStrs, ",")
		groupPageCount := len(group.Pages)

		childFilename := fmt.Sprintf("%s_inv_%s.pdf",
			strings.TrimSuffix(origFilename, filepath.Ext(origFilename)),
			group.InvoiceNumber)
		childSafe := fmt.Sprintf("%d_%s", time.Now().UnixMilli(), childFilename)
		childPath := filepath.Join("static/documents", childSafe)

		if err := extractPDFPageRange(origPath, childPath, group.Pages); err != nil {
			log.Printf("splitter: failed to extract pages %v for invoice %s: %v", group.Pages, group.InvoiceNumber, err)
			continue
		}

		childBytes, err := os.ReadFile(childPath)
		if err != nil {
			log.Printf("splitter: failed to read child PDF: %v", err)
			continue
		}
		childB64 := base64.StdEncoding.EncodeToString(childBytes)

		ocrResult, ocrRaw, confidence, agentVersion, err := h.runPipeline(apiKey, childB64, ext)
		if err != nil {
			log.Printf("splitter: pipeline failed for invoice %s: %v", group.InvoiceNumber, err)
			pID := parentID
			childDoc := models.Document{
				Filename:         childFilename,
				FilePath:         childPath,
				FileURL:          "/documents/file/" + childSafe,
				Status:           "ocr_failed",
				ParentDocumentID: &pID,
				PageCount:        &groupPageCount,
				PageNumbers:      &pageNumStr,
			}
			h.GormDB.Create(&childDoc)
			childDocs = append(childDocs, childDoc)
			continue
		}

		pID := parentID
		childDoc := h.buildDocumentFromPipeline(childFilename, childPath, childSafe,
			ocrResult, ocrRaw, confidence, agentVersion, &pID, &pageNumStr)
		childDoc.PageCount = &groupPageCount

		if err := h.GormDB.Create(&childDoc).Error; err != nil {
			log.Printf("splitter: failed to save child doc: %v", err)
			continue
		}

		h.tryAutoMatch(&childDoc, ocrResult)
		go h.forwardToWickedFile(&childDoc, childBytes)

		childDocs = append(childDocs, childDoc)
		log.Printf("splitter: created child doc %d for invoice %s (pages %s, confidence=%.2f)",
			childDoc.ID, group.InvoiceNumber, pageNumStr, confidence)
	}

	return childDocs
}

// atomic helpers
func atomicCAS(addr *int32, old, new int32) bool {
	return syncAtomic.CompareAndSwapInt32(addr, old, new)
}
func atomicStore(addr *int32, val int32) { syncAtomic.StoreInt32(addr, val) }
func atomicLoad(addr *int32) int32       { return syncAtomic.LoadInt32(addr) }
func atomicAdd(addr *int32, delta int32) { syncAtomic.AddInt32(addr, delta) }

// ── Helpers ───────────────────────────────────────────────────

func docStrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ── WickedFile Integration ───────────────────────────────────

// getWFSetting reads a WickedFile setting: first from integration_settings table,
// then falls back to env var / config.
func (h *DocumentHandler) getWFSetting(key string) string {
	var val string
	h.GormDB.Raw("SELECT value FROM integration_settings WHERE key = ?", key).Scan(&val)
	if val != "" {
		return val
	}
	switch key {
	case "wickedfile_api_url":
		return h.Cfg.WickedFileAPIURL
	case "wickedfile_api_key":
		return h.Cfg.WickedFileAPIKey
	case "wickedfile_email_intake":
		return h.Cfg.WickedFileEmailIntake
	}
	return ""
}

// forwardToWickedFile attempts both API and email intake methods.
func (h *DocumentHandler) forwardToWickedFile(doc *models.Document, fileBytes []byte) {
	apiURL := h.getWFSetting("wickedfile_api_url")
	apiKey := h.getWFSetting("wickedfile_api_key")
	emailIntake := h.getWFSetting("wickedfile_email_intake")

	if apiURL == "" && apiKey == "" && emailIntake == "" {
		return // WickedFile not configured
	}

	sent := false

	// Method 1: API intake
	if apiURL != "" && apiKey != "" {
		if err := h.wickedFileAPISend(doc, fileBytes, apiURL, apiKey); err != nil {
			log.Printf("WickedFile API send failed for doc %d: %v", doc.ID, err)
		} else {
			sent = true
			log.Printf("WickedFile API: sent doc %d", doc.ID)
		}
	}

	// Method 2: Email intake (as fallback or additional)
	if emailIntake != "" && !sent {
		if err := h.wickedFileEmailSend(doc, fileBytes, emailIntake); err != nil {
			log.Printf("WickedFile email send failed for doc %d: %v", doc.ID, err)
		} else {
			sent = true
			log.Printf("WickedFile email: sent doc %d to %s", doc.ID, emailIntake)
		}
	}

	if sent {
		now := time.Now()
		h.GormDB.Model(doc).Updates(map[string]interface{}{
			"wickedfile_sent":    true,
			"wickedfile_sent_at": now,
		})
	}
}

// wickedFileAPISend posts the document to WickedFile's intake API.
func (h *DocumentHandler) wickedFileAPISend(doc *models.Document, fileBytes []byte, apiURL, apiKey string) error {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add file
	part, err := writer.CreateFormFile("file", doc.Filename)
	if err != nil {
		return err
	}
	part.Write(fileBytes)

	// Build metadata JSON
	meta := map[string]interface{}{
		"source": "business-analytics",
	}
	if doc.VendorName != nil {
		meta["vendor_name"] = *doc.VendorName
	}
	if doc.DocumentDate != nil {
		meta["document_date"] = *doc.DocumentDate
	}
	if doc.DocumentType != nil {
		meta["document_type"] = *doc.DocumentType
	}
	if doc.Location != nil {
		meta["location"] = *doc.Location
	}
	if doc.PONumber != nil {
		meta["po_number"] = *doc.PONumber
	}
	if doc.TotalAmount != nil {
		meta["total_amount"] = *doc.TotalAmount
	}

	metaJSON, _ := json.Marshal(meta)
	writer.WriteField("metadata", string(metaJSON))
	writer.Close()

	url := strings.TrimRight(apiURL, "/") + "/api/documents/intake"
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("WickedFile API returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// wickedFileEmailSend sends the document as an email attachment.
func (h *DocumentHandler) wickedFileEmailSend(doc *models.Document, fileBytes []byte, toAddr string) error {
	if h.Cfg.SMTPHost == "" || h.Cfg.SMTPPass == "" {
		return fmt.Errorf("SMTP not configured")
	}

	from := h.Cfg.SMTPFrom
	if from == "" {
		from = h.Cfg.SMTPUser
	}
	port := h.Cfg.SMTPPort
	if port == "" {
		port = "587"
	}

	// Build subject
	docType := "Document"
	if doc.DocumentType != nil {
		docType = *doc.DocumentType
	}
	vendor := "Unknown"
	if doc.VendorName != nil {
		vendor = *doc.VendorName
	}
	docDate := "No-Date"
	if doc.DocumentDate != nil {
		docDate = *doc.DocumentDate
	}
	poNum := ""
	if doc.PONumber != nil {
		poNum = *doc.PONumber
	}
	subject := fmt.Sprintf("%s - %s - %s", docType, vendor, docDate)
	if poNum != "" {
		subject += " - " + poNum
	}

	// Build plain-text body with metadata
	bodyText := fmt.Sprintf("Document forwarded from Business Analytics\n\n"+
		"Vendor: %s\nDate: %s\nType: %s\nPO #: %s\n",
		vendor, docDate, docType, poNum)
	if doc.TotalAmount != nil {
		bodyText += fmt.Sprintf("Amount: $%.2f\n", *doc.TotalAmount)
	}
	if doc.Location != nil {
		bodyText += fmt.Sprintf("Location: %s\n", *doc.Location)
	}

	// Build MIME multipart email with attachment
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)

	// Headers
	headers := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n"+
		"MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=%s\r\n\r\n",
		from, toAddr, subject, mw.Boundary())

	// Text part
	textHeader := textproto.MIMEHeader{}
	textHeader.Set("Content-Type", "text/plain; charset=utf-8")
	textPart, _ := mw.CreatePart(textHeader)
	textPart.Write([]byte(bodyText))

	// Attachment part
	ext := strings.ToLower(filepath.Ext(doc.Filename))
	mimeType := "application/octet-stream"
	switch ext {
	case ".pdf":
		mimeType = "application/pdf"
	case ".jpg", ".jpeg":
		mimeType = "image/jpeg"
	case ".png":
		mimeType = "image/png"
	}

	attHeader := textproto.MIMEHeader{}
	attHeader.Set("Content-Type", mimeType)
	attHeader.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", doc.Filename))
	attHeader.Set("Content-Transfer-Encoding", "base64")
	attPart, _ := mw.CreatePart(attHeader)
	encoded := base64.StdEncoding.EncodeToString(fileBytes)
	// Wrap at 76 chars for MIME compliance
	for i := 0; i < len(encoded); i += 76 {
		end := i + 76
		if end > len(encoded) {
			end = len(encoded)
		}
		attPart.Write([]byte(encoded[i:end] + "\r\n"))
	}

	mw.Close()

	msg := []byte(headers + buf.String())

	auth := smtp.PlainAuth("", h.Cfg.SMTPUser, h.Cfg.SMTPPass, h.Cfg.SMTPHost)
	addr := h.Cfg.SMTPHost + ":" + port

	return smtp.SendMail(addr, auth, from, []string{toAddr}, msg)
}

// ── Settings Endpoints ───────────────────────────────────────

// GET /settings/integrations — return WickedFile settings
func (h *DocumentHandler) GetIntegrationSettings(c *gin.Context) {
	keys := []string{"wickedfile_api_url", "wickedfile_api_key", "wickedfile_email_intake"}
	result := map[string]string{}
	for _, k := range keys {
		result[k] = h.getWFSetting(k)
	}
	// Mask the API key for display
	if key := result["wickedfile_api_key"]; len(key) > 8 {
		result["wickedfile_api_key"] = key[:4] + strings.Repeat("*", len(key)-8) + key[len(key)-4:]
	}
	c.JSON(http.StatusOK, result)
}

// PUT /settings/integrations — save WickedFile settings
func (h *DocumentHandler) SaveIntegrationSettings(c *gin.Context) {
	var body map[string]string
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	allowed := map[string]bool{
		"wickedfile_api_url":      true,
		"wickedfile_api_key":      true,
		"wickedfile_email_intake": true,
	}

	for k, v := range body {
		if !allowed[k] {
			continue
		}
		// Skip masked API keys (unchanged)
		if k == "wickedfile_api_key" && strings.Contains(v, "****") {
			continue
		}
		h.GormDB.Exec(
			`INSERT INTO integration_settings (key, value, updated_at)
			 VALUES (?, ?, NOW())
			 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
			k, v,
		)
	}

	c.JSON(http.StatusOK, gin.H{"message": "settings saved"})
}

// POST /settings/integrations/test-wickedfile — test connection
func (h *DocumentHandler) TestWickedFileConnection(c *gin.Context) {
	var body map[string]string
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}

	apiURL := body["wickedfile_api_url"]
	apiKey := body["wickedfile_api_key"]
	emailIntake := body["wickedfile_email_intake"]

	// If API key is masked, use stored value
	if strings.Contains(apiKey, "****") {
		apiKey = h.getWFSetting("wickedfile_api_key")
	}

	results := gin.H{}

	// Test API connection
	if apiURL != "" && apiKey != "" {
		url := strings.TrimRight(apiURL, "/") + "/api/health"
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", "Bearer "+apiKey)
		httpClient := &http.Client{Timeout: 10 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			results["api_status"] = "error"
			results["api_message"] = err.Error()
		} else {
			resp.Body.Close()
			if resp.StatusCode < 400 {
				results["api_status"] = "ok"
				results["api_message"] = fmt.Sprintf("Connected (%d)", resp.StatusCode)
			} else {
				results["api_status"] = "error"
				results["api_message"] = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
	}

	// Validate email
	if emailIntake != "" {
		if strings.Contains(emailIntake, "@") {
			results["email_status"] = "ok"
			results["email_message"] = fmt.Sprintf("Will send to %s", emailIntake)
		} else {
			results["email_status"] = "error"
			results["email_message"] = "Invalid email address"
		}
	}

	c.JSON(http.StatusOK, results)
}
