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
	IsFinancial    bool    `json:"is_financial"`
	RONumber       string  `json:"ro_number,omitempty"`
	CustomerName   string  `json:"customer_name,omitempty"`
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
	// Statement-specific fields
	StatementDate    string  `json:"statement_date"`
	PeriodStart      string  `json:"period_start"`
	PeriodEnd        string  `json:"period_end"`
	PreviousBalance  float64 `json:"previous_balance"`
	NewCharges       float64 `json:"new_charges"`
	PaymentsReceived float64 `json:"payments_received"`
	BalanceDue       float64 `json:"balance_due"`
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
	log.Printf("pipeline: agent1 done — vendor=%s type=%s format=%s confidence=%.2f financial=%v",
		agent1.VendorName, agent1.DocumentType, agent1.DocumentFormat, agent1.Confidence, agent1.IsFinancial)

	// ── Non-financial short circuit ──────────────────────────
	if !agent1.IsFinancial || !isFinancialDocType(agent1.DocumentType) {
		log.Printf("pipeline: non-financial document detected (%s) — running lightweight extraction", agent1.DocumentType)
		result, rawStr := h.runNonFinancialPipeline(apiKey, base64Data, mediaType, contentBlockType, agent1)
		return result, rawStr, agent1.Confidence, "non-financial-v1", nil
	}

	// ── Agent 2: Vendor-specific extractor ───────────────────
	isStatement := strings.EqualFold(agent1.DocumentType, "STATEMENT")
	var agent2 *extractorResult
	if isStatement {
		log.Printf("pipeline: agent2 statement extractor starting (vendor=%s)", agent1.VendorName)
		agent2, err = h.agent2ExtractStatement(apiKey, base64Data, mediaType, contentBlockType)
		if err != nil {
			log.Printf("pipeline: agent2 statement failed: %v — falling back to single-agent", err)
			return h.fallbackSingleAgent(apiKey, base64Data, ext)
		}
		// Use balance_due as total for statements
		if agent2.BalanceDue != 0 {
			agent2.TotalAmount = agent2.BalanceDue
		}
		if agent2.DocumentDate == "" && agent2.StatementDate != "" {
			agent2.DocumentDate = agent2.StatementDate
		}
	} else {
		log.Printf("pipeline: agent2 extractor starting (format=%s)", agent1.DocumentFormat)
		agent2, err = h.agent2Extract(apiKey, base64Data, mediaType, contentBlockType, agent1.DocumentFormat)
		if err != nil {
			log.Printf("pipeline: agent2 failed: %v — falling back to single-agent", err)
			return h.fallbackSingleAgent(apiKey, base64Data, ext)
		}
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

	// Apply learned vendor corrections
	db := h.sqlDB()
	if db != nil {
		var correctName string
		err := db.QueryRow(`
			SELECT correct_name FROM vendor_corrections
			WHERE LOWER(detected_name) = LOWER($1) AND correction_count >= 1
			ORDER BY correction_count DESC
			LIMIT 1
		`, result.VendorName).Scan(&correctName)
		if err == nil && correctName != "" {
			log.Printf("Vendor correction applied: '%s' → '%s'", result.VendorName, correctName)
			result.VendorName = correctName
		}
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

	// ── Statement post-processing ────────────────────────────
	if isStatement {
		result.DocumentType = "STATEMENT"
		h.processStatementLineItems(agent2, vendorName)
	}

	return result, rawStr, finalConfidence, pipelineVersion, nil
}

// ── Agent 1: Document Classifier ─────────────────────────────

func (h *DocumentHandler) agent1Classify(apiKey, b64, mediaType, blockType string) (*classifierResult, error) {
	prompt := `Classify this document. Return JSON only:
{
  "vendor_name": "exact company name from header/logo",
  "vendor_type": "PARTS|SUPPLIES|OFFICE|TOOLS|UTILITY|OTHER",
  "document_type": "INVOICE|STATEMENT|RECEIPT|CREDIT_MEMO|SIGNED_RO|CONTRACT|WARRANTY|INSPECTION|INSURANCE|ESTIMATE|UNKNOWN",
  "document_format": "WORLDPAC|NAPA|OREILLY|AUTOZONE|DORMAN|MOTORCRAFT|GATES|CARQUEST|ADVANCE|GENERIC",
  "confidence": 0.0-1.0,
  "is_financial": true,
  "ro_number": ""
}

DOCUMENT TYPE CLASSIFICATION:

FINANCIAL (full processing pipeline):
- INVOICE: vendor invoice for parts/supplies
- STATEMENT: vendor monthly/weekly statement
- RECEIPT: payment receipt
- CREDIT_MEMO: vendor credit memo

NON-FINANCIAL (pass-through, minimal extraction):
- SIGNED_RO: customer-signed repair order (shows customer signature, vehicle info, RO number)
- CONTRACT: any contract or agreement
- WARRANTY: warranty document or claim form
- INSPECTION: vehicle inspection report
- INSURANCE: insurance card or claim document
- ESTIMATE: repair estimate for customer
- UNKNOWN: cannot classify

Set is_financial=true for INVOICE, STATEMENT, RECEIPT, CREDIT_MEMO.
Set is_financial=false for all others.
If document_type is SIGNED_RO, extract the RO number into ro_number field.

CRITICAL: The vendor is identified ONLY by the large logo/brand name printed at the very TOP of the document header.
The rule: Who is SENDING/SELLING? That is the vendor. Who is RECEIVING/BUYING? Ignore them.

Common automotive parts vendors:
- CarQuest logo → vendor_name: "CARQUEST", document_format: "CARQUEST"
- NAPA logo → vendor_name: "NAPA AUTO PARTS", document_format: "NAPA"
- WorldPac → vendor_name: "WorldPac", document_format: "WORLDPAC"
- O'Reilly → vendor_name: "O'Reilly Auto Parts", document_format: "OREILLY"
- AutoZone → vendor_name: "AutoZone", document_format: "AUTOZONE"
- Advance Auto Parts → vendor_name: "Advance Auto Parts", document_format: "ADVANCE"
- Dorman → vendor_name: "Dorman", document_format: "DORMAN"
- Gates → vendor_name: "Gates", document_format: "GATES"
- Motorcraft → vendor_name: "Motorcraft", document_format: "MOTORCRAFT"
- Genuine Parts Company → vendor_name: "Genuine Parts Company", document_format: "NAPA"`

	text, err := h.callClaudeWithImage(apiKey, b64, mediaType, blockType,
		"You are a document classification expert for automotive repair shops. Identify the VENDOR from the logo/header at the top of the document. Respond with JSON only.",
		prompt)
	if err != nil {
		return nil, err
	}

	var result classifierResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("agent1 parse: %w", err)
	}

	// Check vendor_corrections for learned corrections
	db := h.sqlDB()
	if db != nil && result.VendorName != "" {
		var correctName string
		err := db.QueryRow(
			"SELECT correct_name FROM vendor_corrections WHERE LOWER(detected_name) = LOWER($1) AND correction_count >= 1 ORDER BY correction_count DESC LIMIT 1",
			result.VendorName,
		).Scan(&correctName)
		if err == nil && correctName != "" {
			log.Printf("pipeline: agent1 vendor correction applied: '%s' → '%s'", result.VendorName, correctName)
			result.VendorName = correctName
		}
	}

	return &result, nil
}

// ── Agent 2: Vendor-specific Extractor ───────────────────────

func (h *DocumentHandler) agent2Extract(apiKey, b64, mediaType, blockType, format string) (*extractorResult, error) {
	prompt := extractorPromptForFormat(format)

	text, err := h.callClaudeWithImage(apiKey, b64, mediaType, blockType,
		"You are an expert at reading automotive industry invoices. Extract all fields precisely. For each line item, extract the part_number (product/SKU number) — this is critical for matching to repair orders. Return JSON only.",
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
  "vendor_address": "address of the VENDOR (seller), NOT the ship-to or bill-to customer address",
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
P.O. No. and Invoice No. are DIFFERENT fields. Do not confuse them.
IMPORTANT: For each line item, extract the part_number (product/SKU number) — this is critical for matching to repair orders.`

	case "NAPA":
		return `This is a NAPA Auto Parts invoice. Extract as JSON:
{
  "vendor_name": "NAPA Auto Parts",
  "vendor_address": "address of the VENDOR (seller), NOT the ship-to or bill-to customer address",
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
}
IMPORTANT: For each line item, extract the part_number (product/SKU number) — this is critical for matching to repair orders.`

	case "CARQUEST":
		return `This is a CarQuest automotive parts invoice.
- The PO No. field contains the repair shop's RO/PO number
- The Invoice No. field contains CarQuest's invoice number
- Ship To address is the receiving shop location
- Extract all line items with part numbers and prices

Extract as JSON:
{
  "vendor_name": "CARQUEST",
  "vendor_address": "address of the VENDOR (seller), NOT the ship-to or bill-to customer address",
  "document_date": "YYYY-MM-DD",
  "document_number": "from Invoice No. field",
  "vendor_po_number": "from PO No. field (customer PO/RO number)",
  "vendor_invoice_number": "from Invoice No. field",
  "order_number": "",
  "account_number": "",
  "total_amount": 0,
  "tax_amount": 0,
  "subtotal": 0,
  "freight": 0,
  "line_items": [{"part_number":"","description":"","qty_ordered":0,"qty_shipped":0,"unit_price":0,"net_price":0,"extended_net_price":0}],
  "ship_to_address": "",
  "bill_to_address": "",
  "sales_person": ""
}
IMPORTANT: vendor_po_number (PO No.) and vendor_invoice_number (Invoice No.) are DIFFERENT fields. Do not confuse them.
IMPORTANT: vendor_name MUST be "CARQUEST" — do NOT use the dealer/customer name (e.g. RSR AUTO PARTS).
IMPORTANT: For each line item, extract the part_number (product/SKU number) — this is critical for matching to repair orders.`

	case "OREILLY":
		return `This is an O'Reilly Auto Parts invoice. Extract as JSON:
{
  "vendor_name": "O'Reilly Auto Parts",
  "vendor_address": "address of the VENDOR (seller), NOT the ship-to or bill-to customer address",
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
}
IMPORTANT: For each line item, extract the part_number (product/SKU number) — this is critical for matching to repair orders.`

	default:
		return `Extract all fields from this automotive invoice as JSON:
{
  "vendor_name": "",
  "vendor_address": "address of the VENDOR (seller), NOT the ship-to or bill-to customer address",
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
IMPORTANT: vendor_po_number (P.O. No.) and vendor_invoice_number (Invoice No.) are DIFFERENT fields.
IMPORTANT: For each line item, extract the part_number (product/SKU number) — this is critical for matching to repair orders.`
	}
}

// ── Agent 2b: Statement Extractor ────────────────────────────

func (h *DocumentHandler) agent2ExtractStatement(apiKey, b64, mediaType, blockType string) (*extractorResult, error) {
	prompt := `This is a vendor account statement (not an individual invoice).
Extract ALL line items/transactions listed on this statement.

Return JSON:
{
  "vendor_name": "",
  "vendor_address": "address of the VENDOR (seller), NOT the ship-to or bill-to customer address",
  "account_number": "",
  "statement_date": "YYYY-MM-DD",
  "document_date": "YYYY-MM-DD (same as statement_date)",
  "period_start": "YYYY-MM-DD",
  "period_end": "YYYY-MM-DD",
  "previous_balance": 0,
  "new_charges": 0,
  "payments_received": 0,
  "balance_due": 0,
  "total_amount": 0,
  "line_items": [
    {
      "invoice_number": "",
      "invoice_date": "YYYY-MM-DD",
      "order_number": "",
      "po_number": "",
      "description": "",
      "amount": 0,
      "payment_amount": 0,
      "balance": 0
    }
  ],
  "ship_to_address": "",
  "bill_to_address": ""
}

IMPORTANT: Extract EVERY line item on the statement.
Each line represents either an invoice charge or a payment.
vendor_address is the address of the company SENDING the statement, NOT the customer.`

	text, err := h.callClaudeWithImage(apiKey, b64, mediaType, blockType,
		"You are an expert at reading vendor account statements for automotive repair shops. Extract all statement details and line items. Return JSON only.",
		prompt)
	if err != nil {
		return nil, err
	}

	var result extractorResult
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("agent2 statement parse: %w", err)
	}
	return &result, nil
}

// processStatementLineItems saves statement line items and creates an AP entry.
func (h *DocumentHandler) processStatementLineItems(agent2 *extractorResult, vendorName string) {
	db := h.sqlDB()
	if db == nil {
		log.Printf("pipeline: statement processing skipped — no DB")
		return
	}

	// We need the document ID, but it hasn't been created yet at this point in the pipeline.
	// The caller (buildDocumentFromPipeline / Upload) will handle saving the document first.
	// We store the statement data in a goroutine that fires after a short delay to let the doc be created.
	// Instead, expose the data for the caller to use.
	// NOTE: Actual statement_line_items insertion happens in processStatementAfterSave, called by the upload handler.
	log.Printf("pipeline: statement detected — vendor=%s lines=%d balance_due=%.2f", vendorName, len(agent2.LineItems), agent2.BalanceDue)
}

// ProcessStatementAfterSave creates statement_line_items and AP entry after the document is saved.
func (h *DocumentHandler) ProcessStatementAfterSave(docID int, vendorName string, agent2Raw string) {
	db := h.sqlDB()
	if db == nil {
		return
	}

	// Parse the agent2 data from ocr_raw
	var pipelineOut pipelineOutput
	if err := json.Unmarshal([]byte(agent2Raw), &pipelineOut); err != nil {
		log.Printf("pipeline: statement post-save parse error: %v", err)
		return
	}
	agent2 := pipelineOut.Agent2Extractor
	if agent2 == nil {
		log.Printf("pipeline: statement post-save — no agent2 data")
		return
	}

	matchedCount := 0
	totalCount := 0

	for _, li := range agent2.LineItems {
		liMap, ok := li.(map[string]interface{})
		if !ok {
			continue
		}
		totalCount++

		invNum, _ := liMap["invoice_number"].(string)
		invDateStr, _ := liMap["invoice_date"].(string)
		desc, _ := liMap["description"].(string)
		amount, _ := liMap["amount"].(float64)
		poNum, _ := liMap["po_number"].(string)

		// Insert statement line item
		var lineID int
		err := db.QueryRow(`INSERT INTO statement_line_items
			(statement_document_id, invoice_number, invoice_date, amount, description)
			VALUES ($1, $2, NULLIF($3, '')::date, $4, $5)
			RETURNING id`,
			docID, invNum, invDateStr, amount, desc).Scan(&lineID)
		if err != nil {
			log.Printf("pipeline: statement line insert error: %v", err)
			continue
		}

		// Try to match to existing invoice document (including across parent brand related vendors)
		var linkedDocID int
		var matchedTxID *string
		err = db.QueryRow(`SELECT id, matched_transaction_id FROM documents
			WHERE is_deleted = false AND (
				(vendor_invoice_number IS NOT NULL AND vendor_invoice_number != '' AND vendor_invoice_number ILIKE '%' || $1 || '%')
				OR ($2 != '' AND vendor_po_number IS NOT NULL AND vendor_po_number = $2)
			)
			AND (
				vendor_name ILIKE '%' || $3 || '%'
				OR vendor_name IN (
					SELECT name FROM vendors
					WHERE parent_brand IS NOT NULL AND parent_brand != '' AND parent_brand = (
						SELECT parent_brand FROM vendors
						WHERE LOWER(name) = LOWER($3) OR LOWER(normalized_name) = LOWER($3)
						LIMIT 1
					)
				)
			)
			LIMIT 1`, invNum, poNum, vendorName).Scan(&linkedDocID, &matchedTxID)

		if err == nil && linkedDocID > 0 {
			status := "invoice_found"
			if matchedTxID != nil && *matchedTxID != "" {
				status = "bank_matched"
			}
			db.Exec(`UPDATE statement_line_items SET linked_document_id = $1, status = $2 WHERE id = $3`,
				linkedDocID, status, lineID)
			if status == "bank_matched" {
				db.Exec(`UPDATE statement_line_items SET linked_transaction_id = $1 WHERE id = $2`,
					*matchedTxID, lineID)
			}
			matchedCount++
			log.Printf("pipeline: statement line %q matched doc %d (status=%s)", invNum, linkedDocID, status)
		}
	}

	// Look up vendor ID (also check parent brand / franchise network)
	var vendorID *string
	db.QueryRow(`SELECT id FROM vendors WHERE LOWER(name) = LOWER($1) OR LOWER(normalized_name) = LOWER($1) LIMIT 1`,
		vendorName).Scan(&vendorID)
	if vendorID == nil {
		// Try matching by parent brand or franchise network
		db.QueryRow(`SELECT id FROM vendors WHERE parent_brand ILIKE $1 OR franchise_network ILIKE $1 LIMIT 1`,
			vendorName).Scan(&vendorID)
	}

	// Create AP entry
	apStatus := "open"
	if totalCount > 0 && matchedCount == totalCount {
		apStatus = "ready_to_pay"
	}

	matchedAmount := 0.0
	unmatchedAmount := 0.0
	for _, li := range agent2.LineItems {
		if liMap, ok := li.(map[string]interface{}); ok {
			amt, _ := liMap["amount"].(float64)
			unmatchedAmount += amt
		}
	}
	if totalCount > 0 && matchedCount > 0 {
		// Rough split based on ratio
		totalAmt := agent2.BalanceDue
		if totalAmt == 0 {
			totalAmt = agent2.TotalAmount
		}
		matchedAmount = totalAmt * float64(matchedCount) / float64(totalCount)
		unmatchedAmount = totalAmt - matchedAmount
	}

	totalAmt := agent2.BalanceDue
	if totalAmt == 0 {
		totalAmt = agent2.TotalAmount
	}

	_, err := db.Exec(`INSERT INTO ap_entries
		(vendor_id, vendor_name, statement_document_id, period_start, period_end,
		 total_amount, matched_amount, unmatched_amount,
		 invoice_count, matched_invoice_count, status)
		VALUES ($1, $2, $3, NULLIF($4, '')::date, NULLIF($5, '')::date,
		 $6, $7, $8, $9, $10, $11)`,
		vendorID, vendorName, docID,
		agent2.PeriodStart, agent2.PeriodEnd,
		totalAmt, matchedAmount, unmatchedAmount,
		totalCount, matchedCount, apStatus)
	if err != nil {
		log.Printf("pipeline: AP entry creation error: %v", err)
	} else {
		log.Printf("pipeline: AP entry created — vendor=%s total=%.2f matched=%d/%d status=%s",
			vendorName, totalAmt, matchedCount, totalCount, apStatus)
	}
}

// ── Agent 3: Validator ───────────────────────────────────────

func (h *DocumentHandler) agent3Validate(apiKey string, data *extractorResult) (*validatorResult, error) {
	dataJSON, _ := json.Marshal(data)

	// Pre-validation: reject single-word PO numbers that are just prefixes with no digits
	if data.VendorPONumber != "" {
		po := strings.TrimSpace(data.VendorPONumber)
		hasDigit := false
		for _, c := range po {
			if c >= '0' && c <= '9' {
				hasDigit = true
				break
			}
		}
		if !hasDigit && !strings.Contains(po, " ") {
			log.Printf("pipeline: agent3 clearing invalid PO number %q (single word, no digits)", po)
			data.VendorPONumber = ""
		}
	}

	prompt := fmt.Sprintf(`Validate this extracted invoice data:
%s

Check:
1. Do line items sum to the subtotal? (within $0.01)
2. Does subtotal + tax + freight = total?
3. Is the PO number format valid? (numeric, 3-8 digits)
4. Is the date reasonable? (within last 90 days)
5. Are quantities positive numbers?
6. If vendor_po_number is a single word with no numbers (e.g. "INV", "PO", "NA", "N/A"), it is NOT a real PO number — flag it as invalid.

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

// ── Non-Financial Document Helpers ──────────────────────────────

// isFinancialDocType returns true if the document type is a financial type.
func isFinancialDocType(docType string) bool {
	switch strings.ToUpper(docType) {
	case "INVOICE", "STATEMENT", "RECEIPT", "CREDIT_MEMO":
		return true
	default:
		return false
	}
}

// documentTypeFolder returns the WickedFile folder name for a document type.
func documentTypeFolder(docType string) string {
	switch strings.ToUpper(docType) {
	case "SIGNED_RO":
		return "Signed Repair Orders"
	case "CONTRACT":
		return "Contracts"
	case "WARRANTY":
		return "Warranty Documents"
	case "INSPECTION":
		return "Inspections"
	case "INSURANCE":
		return "Insurance"
	case "ESTIMATE":
		return "Estimates"
	default:
		return "Other Documents"
	}
}

// extractWFKeywords extracts WickedFile classification keywords from OCR text.
func extractWFKeywords(text string) []string {
	keywords := []string{}
	textLower := strings.ToLower(text)
	if strings.Contains(textLower, "core") {
		keywords = append(keywords, "core")
	}
	if strings.Contains(textLower, "rma") || strings.Contains(textLower, "return authorization") {
		keywords = append(keywords, "rma")
	}
	if strings.Contains(textLower, "stock") || strings.Contains(textLower, "stk") {
		keywords = append(keywords, "stock")
	}
	if strings.Contains(textLower, "shop") {
		keywords = append(keywords, "shop")
	}
	if strings.Contains(textLower, "warranty") || strings.Contains(textLower, "warr") {
		keywords = append(keywords, "warranty")
	}
	if strings.Contains(textLower, "office") {
		keywords = append(keywords, "office")
	}
	return keywords
}

// assignCategoryFromPO assigns a WickedFile category based on PO number patterns.
func assignCategoryFromPO(poNumber string) string {
	po := strings.ToLower(poNumber)
	if strings.HasPrefix(po, "st") || strings.Contains(po, "inventory") || strings.Contains(po, "cogs") {
		return "inventory"
	}
	if strings.HasPrefix(po, "sup") || strings.HasPrefix(po, "sho") || strings.Contains(po, "shop") {
		return "shop_supplies"
	}
	if strings.HasPrefix(po, "off") || strings.Contains(po, "office") {
		return "office_supplies"
	}
	if strings.HasPrefix(po, "inv") {
		return "parts_cogs"
	}
	return "uncategorized"
}

// runNonFinancialPipeline handles lightweight extraction for non-financial documents.
func (h *DocumentHandler) runNonFinancialPipeline(apiKey, base64Data, mediaType, contentBlockType string, agent1 *classifierResult) (*ocrExtractedData, string) {
	// Lightweight extraction — just get key fields
	prompt := `Extract basic metadata from this non-financial document. Return JSON only:
{
  "customer_name": "customer or person name if visible",
  "vendor_name": "business name if visible",
  "document_date": "YYYY-MM-DD if visible",
  "ro_number": "repair order number if visible",
  "location": "shop/location name or address if visible",
  "description": "brief one-line description of document contents"
}`

	text, err := h.callClaudeWithImage(apiKey, base64Data, mediaType, contentBlockType,
		"Extract basic metadata from this document. Respond with JSON only.", prompt)
	if err != nil {
		log.Printf("pipeline: non-financial extraction failed: %v", err)
		return &ocrExtractedData{
			DocumentType: agent1.DocumentType,
			VendorName:   agent1.VendorName,
		}, "{}"
	}

	var extracted struct {
		CustomerName string `json:"customer_name"`
		VendorName   string `json:"vendor_name"`
		DocumentDate string `json:"document_date"`
		RONumber     string `json:"ro_number"`
		Location     string `json:"location"`
		Description  string `json:"description"`
	}
	json.Unmarshal([]byte(text), &extracted)

	vendorName := extracted.VendorName
	if vendorName == "" {
		vendorName = agent1.VendorName
	}

	result := &ocrExtractedData{
		DocumentType: agent1.DocumentType,
		VendorName:   vendorName,
		DocumentDate: extracted.DocumentDate,
	}

	// Build raw output
	output := map[string]interface{}{
		"pipeline_version": "non-financial-v1",
		"agent1":           agent1,
		"extraction":       extracted,
	}
	rawJSON, _ := json.Marshal(output)

	return result, string(rawJSON)
}
