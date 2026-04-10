package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ── Multi-Agent OCR Pipeline ─────────────────────────────────
// 4-agent pipeline: Classifier → Extractor → Validator → Matcher

const pipelineVersion = "multi-agent-v1"

// ── Agent response types ─────────────────────────────────────

type classifierResult struct {
	VendorName     string  `json:"vendor_name"`
	VendorType     string  `json:"vendor_type"`
	DocumentType   string  `json:"document_type"`
	DocumentFormat string  `json:"document_format"`
	Confidence     float64 `json:"confidence"`
}

type extractorResult struct {
	VendorName          string        `json:"vendor_name"`
	VendorAddress       string        `json:"vendor_address"`
	DocumentDate        string        `json:"document_date"`
	DocumentNumber      string        `json:"document_number"`
	VendorPONumber      string        `json:"vendor_po_number"`
	VendorInvoiceNumber string        `json:"vendor_invoice_number"`
	OrderNumber         string        `json:"order_number"`
	TotalAmount         float64       `json:"total_amount"`
	TaxAmount           float64       `json:"tax_amount"`
	Subtotal            float64       `json:"subtotal"`
	Freight             float64       `json:"freight"`
	LineItems           []interface{} `json:"line_items"`
	ShipToAddress       string        `json:"ship_to_address"`
	BillToAddress       string        `json:"bill_to_address"`
	AccountNumber       string        `json:"account_number"`
	ShipFrom            string        `json:"ship_from"`
	SalesPerson         string        `json:"sales_person"`
}

type validatorResult struct {
	IsValid              bool     `json:"is_valid"`
	MathChecksOut        bool     `json:"math_checks_out"`
	LineItemsSum         float64  `json:"line_items_sum"`
	Discrepancies        []string `json:"discrepancies"`
	CorrectedTotal       float64  `json:"corrected_total"`
	ConfidenceAdjustment float64  `json:"confidence_adjustment"`
}

type matcherResult struct {
	BestMatchID string  `json:"best_match_id"`
	Confidence  float64 `json:"confidence"`
	Reasoning   string  `json:"reasoning"`
}

type pipelineOutput struct {
	Agent1Classifier *classifierResult `json:"agent1_classifier"`
	Agent2Extractor  *extractorResult  `json:"agent2_extractor"`
	Agent3Validator  *validatorResult  `json:"agent3_validator"`
	Agent4Matcher    *matcherResult    `json:"agent4_matcher"`
	PipelineVersion  string            `json:"pipeline_version"`
}

// ── Pipeline orchestrator ────────────────────────────────────

func (h *DocumentHandler) runPipeline(apiKey, base64Data, ext string) (*ocrExtractedData, string, float64, string, error) {
	mediaType, contentBlockType := mediaTypeForExt(ext)
	if mediaType == "" {
		return nil, "", 0, "", fmt.Errorf("unsupported extension: %s", ext)
	}

	output := pipelineOutput{PipelineVersion: pipelineVersion}

	// ── Agent 1: Classifier ──────────────────────────────────
	log.Printf("pipeline: agent1 classifier starting")
	agent1, err := h.agent1Classify(apiKey, base64Data, mediaType, contentBlockType)
	if err != nil {
		log.Printf("pipeline: agent1 failed: %v — falling back to single-agent", err)
		return h.fallbackSingleAgent(apiKey, base64Data, ext)
	}
	output.Agent1Classifier = agent1
	log.Printf("pipeline: agent1 done — vendor=%s format=%s confidence=%.2f", agent1.VendorName, agent1.DocumentFormat, agent1.Confidence)

	// ── Agent 2: Vendor-specific extractor ───────────────────
	log.Printf("pipeline: agent2 extractor starting (format=%s)", agent1.DocumentFormat)
	agent2, err := h.agent2Extract(apiKey, base64Data, mediaType, contentBlockType, agent1.DocumentFormat)
	if err != nil {
		log.Printf("pipeline: agent2 failed: %v — falling back to single-agent", err)
		return h.fallbackSingleAgent(apiKey, base64Data, ext)
	}
	output.Agent2Extractor = agent2
	log.Printf("pipeline: agent2 done — po=%s inv=%s total=%.2f", agent2.VendorPONumber, agent2.VendorInvoiceNumber, agent2.TotalAmount)

	// ── Agent 3: Validator ───────────────────────────────────
	log.Printf("pipeline: agent3 validator starting")
	agent3, err := h.agent3Validate(apiKey, agent2)
	if err != nil {
		log.Printf("pipeline: agent3 failed: %v — continuing without validation", err)
		agent3 = &validatorResult{IsValid: true, MathChecksOut: true, ConfidenceAdjustment: 0}
	}
	output.Agent3Validator = agent3
	log.Printf("pipeline: agent3 done — valid=%v math=%v adj=%.2f", agent3.IsValid, agent3.MathChecksOut, agent3.ConfidenceAdjustment)

	// ── Agent 4: Transaction matcher ─────────────────────────
	vendorName := agent2.VendorName
	if vendorName == "" {
		vendorName = agent1.VendorName
	}
	log.Printf("pipeline: agent4 matcher starting")
	agent4, err := h.agent4Match(apiKey, vendorName, agent2.DocumentDate, agent2.TotalAmount)
	if err != nil {
		log.Printf("pipeline: agent4 failed: %v — continuing without match", err)
		agent4 = &matcherResult{}
	}
	output.Agent4Matcher = agent4
	log.Printf("pipeline: agent4 done — match=%s confidence=%.2f", agent4.BestMatchID, agent4.Confidence)

	// ── Compose final result ─────────────────────────────────
	finalConfidence := agent1.Confidence + agent3.ConfidenceAdjustment
	if finalConfidence > 1.0 {
		finalConfidence = 1.0
	}
	if finalConfidence < 0 {
		finalConfidence = 0
	}

	result := &ocrExtractedData{
		DocumentType:        agent1.DocumentType,
		VendorName:          vendorName,
		VendorAddress:       agent2.VendorAddress,
		DocumentDate:        agent2.DocumentDate,
		DocumentNumber:      agent2.DocumentNumber,
		TotalAmount:         agent2.TotalAmount,
		TaxAmount:           agent2.TaxAmount,
		LineItems:           agent2.LineItems,
		ShipToAddress:       agent2.ShipToAddress,
		BillToAddress:       agent2.BillToAddress,
		VendorPONumber:      agent2.VendorPONumber,
		VendorInvoiceNumber: agent2.VendorInvoiceNumber,
		OrderNumber:         agent2.OrderNumber,
	}

	// Use corrected total if validator found math issues
	if !agent3.MathChecksOut && agent3.CorrectedTotal > 0 {
		result.TotalAmount = agent3.CorrectedTotal
	}

	// Serialize full pipeline output as ocr_raw
	rawJSON, _ := json.MarshalIndent(output, "", "  ")
	rawStr := string(rawJSON)

	// Determine matched transaction from agent4
	matchedTxID := agent4.BestMatchID

	// Store match info in result for upstream to use
	if matchedTxID != "" {
		result.POReferences = []string{"agent4_match:" + matchedTxID}
	}

	return result, rawStr, finalConfidence, pipelineVersion, nil
}

// ── Agent 1: Document Classifier ─────────────────────────────

func (h *DocumentHandler) agent1Classify(apiKey, b64, mediaType, blockType string) (*classifierResult, error) {
	prompt := `Classify this document. Return JSON only:
{
  "vendor_name": "exact company name",
  "vendor_type": "PARTS|SUPPLIES|OFFICE|TOOLS|UTILITY|OTHER",
  "document_type": "INVOICE|STATEMENT|RECEIPT|CREDIT_MEMO|OTHER",
  "document_format": "WORLDPAC|NAPA|OREILLY|AUTOZONE|DORMAN|MOTORCRAFT|GATES|GENERIC",
  "confidence": 0.0-1.0
}`

	text, err := h.callClaudeWithImage(apiKey, b64, mediaType, blockType,
		"You are a document classification expert for automotive repair shops. Respond with JSON only.",
		prompt)
	if err != nil {
		return nil, err
	}

	var result classifierResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("agent1 parse: %w", err)
	}
	return &result, nil
}

// ── Agent 2: Vendor-specific Extractor ───────────────────────

func (h *DocumentHandler) agent2Extract(apiKey, b64, mediaType, blockType, format string) (*extractorResult, error) {
	prompt := extractorPromptForFormat(format)

	text, err := h.callClaudeWithImage(apiKey, b64, mediaType, blockType,
		"You are an expert at reading automotive industry invoices. Extract all fields precisely. Return JSON only.",
		prompt)
	if err != nil {
		return nil, err
	}

	var result extractorResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("agent2 parse: %w", err)
	}
	return &result, nil
}

func extractorPromptForFormat(format string) string {
	switch strings.ToUpper(format) {
	case "WORLDPAC":
		return `This is a WorldPac automotive parts invoice. Extract these fields as JSON:
{
  "vendor_name": "WorldPac",
  "vendor_address": "",
  "document_date": "YYYY-MM-DD (from Invoice Date)",
  "document_number": "from Invoice No. field",
  "vendor_po_number": "from P.O. No. field (customer PO, usually 5 digits)",
  "vendor_invoice_number": "from Invoice No. field",
  "order_number": "from Order No. field",
  "account_number": "from Account No. field",
  "ship_from": "warehouse location (e.g. GA Norcross)",
  "total_amount": 0,
  "tax_amount": 0,
  "subtotal": 0,
  "freight": 0,
  "line_items": [{"part_number":"","brand":"","description":"","qty_ordered":0,"qty_shipped":0,"list_price":0,"net_price":0,"core":0,"extended_net_price":0}],
  "bill_to_address": "",
  "ship_to_address": "",
  "sales_person": ""
}
P.O. No. and Invoice No. are DIFFERENT fields. Do not confuse them.`

	case "NAPA":
		return `This is a NAPA Auto Parts invoice. Extract as JSON:
{
  "vendor_name": "NAPA Auto Parts",
  "vendor_address": "",
  "document_date": "YYYY-MM-DD",
  "document_number": "",
  "vendor_po_number": "PO number if present",
  "vendor_invoice_number": "Invoice number",
  "order_number": "",
  "total_amount": 0,
  "tax_amount": 0,
  "subtotal": 0,
  "freight": 0,
  "line_items": [{"part_number":"","description":"","qty_ordered":0,"qty_shipped":0,"net_price":0,"extended_net_price":0}],
  "ship_to_address": "",
  "bill_to_address": ""
}`

	case "OREILLY":
		return `This is an O'Reilly Auto Parts invoice. Extract as JSON:
{
  "vendor_name": "O'Reilly Auto Parts",
  "vendor_address": "",
  "document_date": "YYYY-MM-DD",
  "document_number": "",
  "vendor_po_number": "",
  "vendor_invoice_number": "",
  "order_number": "",
  "total_amount": 0,
  "tax_amount": 0,
  "subtotal": 0,
  "freight": 0,
  "line_items": [{"part_number":"","description":"","qty_ordered":0,"qty_shipped":0,"net_price":0,"extended_net_price":0}],
  "ship_to_address": "",
  "bill_to_address": ""
}`

	default:
		return `Extract all fields from this automotive invoice as JSON:
{
  "vendor_name": "",
  "vendor_address": "",
  "document_date": "YYYY-MM-DD",
  "document_number": "",
  "vendor_po_number": "PO or purchase order number (NOT the invoice number)",
  "vendor_invoice_number": "Invoice number",
  "order_number": "Order number if present",
  "total_amount": 0,
  "tax_amount": 0,
  "subtotal": 0,
  "freight": 0,
  "line_items": [{"part_number":"","description":"","qty_ordered":0,"qty_shipped":0,"unit_price":0,"net_price":0,"extended_net_price":0}],
  "ship_to_address": "",
  "bill_to_address": ""
}
IMPORTANT: vendor_po_number (P.O. No.) and vendor_invoice_number (Invoice No.) are DIFFERENT fields.`
	}
}

// ── Agent 3: Validator ───────────────────────────────────────

func (h *DocumentHandler) agent3Validate(apiKey string, data *extractorResult) (*validatorResult, error) {
	dataJSON, _ := json.Marshal(data)

	prompt := fmt.Sprintf(`Validate this extracted invoice data:
%s

Check:
1. Do line items sum to the subtotal? (within $0.01)
2. Does subtotal + tax + freight = total?
3. Is the PO number format valid? (numeric, 3-8 digits)
4. Is the date reasonable? (within last 90 days)
5. Are quantities positive numbers?

Return JSON only:
{
  "is_valid": true/false,
  "math_checks_out": true/false,
  "line_items_sum": 0.00,
  "discrepancies": ["list of issues"],
  "corrected_total": 0.00,
  "confidence_adjustment": -0.3 to 0.0
}`, string(dataJSON))

	text, err := h.callClaudeText(apiKey,
		"You are a financial document validator. Respond with JSON only.",
		prompt)
	if err != nil {
		return nil, err
	}

	var result validatorResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("agent3 parse: %w", err)
	}
	return &result, nil
}

// ── Agent 4: Transaction Matcher ─────────────────────────────

func (h *DocumentHandler) agent4Match(apiKey, vendorName, dateStr string, amount float64) (*matcherResult, error) {
	if amount == 0 || dateStr == "" {
		return &matcherResult{}, nil
	}

	// Query candidate transactions
	db := h.sqlDB()
	if db == nil {
		return &matcherResult{}, nil
	}

	rows, err := db.Query(`
		SELECT id, date, amount, name, vendor
		FROM transactions
		WHERE ABS(ABS(amount) - $1) < ($1 * 0.05 + 0.01)
		  AND date BETWEEN ($2::date - INTERVAL '14 days')::text AND ($2::date + INTERVAL '14 days')::text
		  AND ($3 = '' OR name ILIKE '%' || $3 || '%' OR merchant_name ILIKE '%' || $3 || '%' OR vendor ILIKE '%' || $3 || '%')
		ORDER BY ABS(ABS(amount) - $1), ABS(date::date - $2::date)
		LIMIT 10
	`, amount, dateStr, vendorName)
	if err != nil {
		return nil, fmt.Errorf("agent4 query: %w", err)
	}
	defer rows.Close()

	type txnCandidate struct {
		ID     string  `json:"id"`
		Date   string  `json:"date"`
		Amount float64 `json:"amount"`
		Name   string  `json:"name"`
	}

	var candidates []txnCandidate
	for rows.Next() {
		var c txnCandidate
		var name, vendor interface{}
		if err := rows.Scan(&c.ID, &c.Date, &c.Amount, &name, &vendor); err != nil {
			continue
		}
		if name != nil {
			c.Name = fmt.Sprintf("%v", name)
		}
		candidates = append(candidates, c)
	}

	if len(candidates) == 0 {
		return &matcherResult{Reasoning: "no candidate transactions found"}, nil
	}

	candidatesJSON, _ := json.Marshal(candidates)

	prompt := fmt.Sprintf(`Match this invoice to bank transactions. Return JSON only.

Invoice: vendor=%q, date=%s, total=$%.2f

Candidate transactions:
%s

Return:
{
  "best_match_id": "string or null if no good match",
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation"
}`, vendorName, dateStr, amount, string(candidatesJSON))

	text, err := h.callClaudeText(apiKey,
		"You are a financial transaction matching expert. Match invoices to bank transactions. Return JSON only.",
		prompt)
	if err != nil {
		return nil, err
	}

	var result matcherResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("agent4 parse: %w", err)
	}
	return &result, nil
}

// ── Shared Claude API helpers ────────────────────────────────

func mediaTypeForExt(ext string) (mediaType, blockType string) {
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg", "image"
	case ".png":
		return "image/png", "image"
	case ".pdf":
		return "application/pdf", "document"
	}
	return "", ""
}

// callClaudeWithImage sends a vision request with an image/document.
func (h *DocumentHandler) callClaudeWithImage(apiKey, b64, mediaType, blockType, system, prompt string) (string, error) {
	fileBlock := map[string]interface{}{
		"type": blockType,
		"source": map[string]interface{}{
			"type":       "base64",
			"media_type": mediaType,
			"data":       b64,
		},
	}
	textBlock := map[string]interface{}{
		"type": "text",
		"text": prompt,
	}

	reqBody := map[string]interface{}{
		"model":      "claude-sonnet-4-20250514",
		"max_tokens": 4096,
		"system":     system,
		"messages": []interface{}{
			map[string]interface{}{
				"role":    "user",
				"content": []interface{}{fileBlock, textBlock},
			},
		},
	}

	return h.doClaudeRequest(apiKey, reqBody)
}

// callClaudeText sends a text-only request (no image).
func (h *DocumentHandler) callClaudeText(apiKey, system, prompt string) (string, error) {
	reqBody := map[string]interface{}{
		"model":      "claude-sonnet-4-20250514",
		"max_tokens": 4096,
		"system":     system,
		"messages": []interface{}{
			map[string]interface{}{
				"role":    "user",
				"content": prompt,
			},
		},
	}

	return h.doClaudeRequest(apiKey, reqBody)
}

// doClaudeRequest is the low-level Anthropic API caller.
func (h *DocumentHandler) doClaudeRequest(apiKey string, reqBody map[string]interface{}) (string, error) {
	bodyBytes, _ := json.Marshal(reqBody)

	httpReq, _ := http.NewRequestWithContext(context.Background(), "POST",
		"https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", apiKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("anthropic API call failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("anthropic API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var apiResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	if len(apiResp.Content) == 0 {
		return "", fmt.Errorf("empty response")
	}

	text := stripJSONFences(apiResp.Content[0].Text)
	return text, nil
}

// fallbackSingleAgent wraps callClaudeVision to match the pipeline return signature.
func (h *DocumentHandler) fallbackSingleAgent(apiKey, base64Data, ext string) (*ocrExtractedData, string, float64, string, error) {
	result, raw, err := h.callClaudeVision(apiKey, base64Data, ext)
	return result, raw, 0.70, "single-agent-fallback", err
}
