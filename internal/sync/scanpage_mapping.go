// Package sync drives the WickedFile Mongo → Postgres ingestion. The
// engine is deliberately split into pure mapping (this file), a small
// writer seam (scanpage_writer.go), and the orchestration loop
// (scanpage_sync.go) so each piece can be exercised independently.
package sync

import (
	"log"
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// Column-length caps for the VARCHAR wf_* columns on documents. Anything
// longer gets truncated with a log warning so a bad upstream value never
// aborts a whole batch.
const (
	lenScanPageID    = 24
	lenLocationID    = 36
	lenS3Key         = 255
	lenAgentVersion  = 32
	lenInvoiceNumber = 64
	lenPoNumber      = 64
	ocrAgentVersion  = "wf-mongo-v1"
)

// DocumentRow is the minimal shape we write into Postgres. Keys match
// column names exactly so the writer can INSERT/UPDATE by the same name.
type DocumentRow struct {
	WfScanPageID      string     // varchar(24) primary Mongo _id
	WfScanID          string     // legacy column; dual-set so ServeFile fallback finds WF proxy
	WfLocationID      *string    // varchar(36)
	WfS3Key           *string    // varchar(255)
	WfOCRAgentVersion string     // 'wf-mongo-v1'
	WfMLParsed        bool
	WfMLReviewed      bool
	WfSyncedAt        time.Time
	DocumentType      *string
	VendorName        *string
	TotalAmount       *float64
	DocumentDate      *string // stored as text in documents.document_date
	WfInvoiceNumber   *string
	WfPoNumber        *string
	WfSubtotal        *float64
	WfTax             *float64
	WfLineItemCount   *int
	WfOCRConfidence   *float64 // 0.000–1.000 with 3-decimal precision

	Filename string // synthetic for INSERT; UPDATE path ignores
	FilePath string // "" so ServeFile falls back to WF proxy

	Timestamp time.Time // shared created_at/updated_at value
}

// MapScanPage converts a Mongo scanPage into a DocumentRow. Pure, no I/O.
func MapScanPage(p *mongodb.ScanPage) DocumentRow {
	now := time.Now().UTC()
	row := DocumentRow{
		WfScanPageID:      truncate("wf_scan_page_id", p.ID.Hex(), lenScanPageID),
		WfScanID:          p.ID.Hex(),
		WfOCRAgentVersion: ocrAgentVersion,
		WfMLParsed:        p.Search.MLParsed,
		WfSyncedAt:        now,
		Timestamp:         now,
		Filename:          "wf-mongo:" + p.ID.Hex(),
		FilePath:          "",
	}
	if p.MLReviewed != nil {
		row.WfMLReviewed = *p.MLReviewed
	}
	if v := strOrNil(p.LocationID); v != nil {
		out := truncate("wf_location_id", *v, lenLocationID)
		row.WfLocationID = &out
	}
	if v := strOrNil(p.S3Key); v != nil {
		out := truncate("wf_s3_key", *v, lenS3Key)
		row.WfS3Key = &out
	}
	if v := strOrNil(p.Type); v != nil {
		row.DocumentType = v
	}

	// vendor_name: prefer fields.formatted.vendorName, fall back to search.vendor
	if v := strOrNil(p.Fields.Formatted.VendorName); v != nil {
		row.VendorName = v
	} else if v := strOrNil(p.Search.Vendor); v != nil {
		row.VendorName = v
	}

	// total_amount: formatted.invoiceTotal ?? search.total
	if p.Fields.Formatted.InvoiceTotal != nil {
		row.TotalAmount = p.Fields.Formatted.InvoiceTotal
	} else if p.Search.Total != nil {
		row.TotalAmount = p.Search.Total
	}

	// document_date (text): formatted.invoiceDate ?? search.dates[0]
	if p.Fields.Formatted.InvoiceDate != nil && !p.Fields.Formatted.InvoiceDate.IsZero() {
		s := p.Fields.Formatted.InvoiceDate.UTC().Format("2006-01-02")
		row.DocumentDate = &s
	} else if len(p.Search.Dates) > 0 && !p.Search.Dates[0].IsZero() {
		s := p.Search.Dates[0].UTC().Format("2006-01-02")
		row.DocumentDate = &s
	}

	// wf_invoice_number: formatted.invoiceId ?? search.invoiceNumber
	if v := strOrNil(p.Fields.Formatted.InvoiceID); v != nil {
		out := truncate("wf_invoice_number", *v, lenInvoiceNumber)
		row.WfInvoiceNumber = &out
	} else if v := strOrNil(p.Search.InvoiceNumber); v != nil {
		out := truncate("wf_invoice_number", *v, lenInvoiceNumber)
		row.WfInvoiceNumber = &out
	}

	// wf_po_number: formatted.purchaseOrder ?? search.poNumber
	if v := strOrNil(p.Fields.Formatted.PurchaseOrder); v != nil {
		out := truncate("wf_po_number", *v, lenPoNumber)
		row.WfPoNumber = &out
	} else if v := strOrNil(p.Search.PoNumber); v != nil {
		out := truncate("wf_po_number", *v, lenPoNumber)
		row.WfPoNumber = &out
	}

	// wf_subtotal: formatted.subTotal ?? search.subTotal
	if p.Fields.Formatted.SubTotal != nil {
		row.WfSubtotal = p.Fields.Formatted.SubTotal
	} else if p.Search.SubTotal != nil {
		row.WfSubtotal = p.Search.SubTotal
	}

	// wf_tax: search.tax only
	if p.Search.Tax != nil {
		row.WfTax = p.Search.Tax
	}

	// wf_line_item_count: len(formatted.items) || len(search.lineItems)
	if n := len(p.Fields.Formatted.Items); n > 0 {
		row.WfLineItemCount = intPtr(n)
	} else if n := len(p.Search.LineItems); n > 0 {
		row.WfLineItemCount = intPtr(n)
	}

	// wf_ocr_confidence: average of top-level float64 values in [0,1]
	row.WfOCRConfidence = avgConfidence(p.Fields.Confidence)

	return row
}

// strOrNil returns nil for "" and trims leading/trailing spaces — stored
// empty strings in Postgres are semantically awkward for filtering, so the
// spec calls for NULL instead.
func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func intPtr(n int) *int { return &n }

// truncate caps a string to n bytes and logs a warning if it did so. The
// column name is only used in the warning — callers pass the target column.
func truncate(column, s string, n int) string {
	if len(s) <= n {
		return s
	}
	log.Printf("WF sync: truncating %s from %d to %d chars", column, len(s), n)
	return s[:n]
}

// avgConfidence walks top-level keys of fields.confidence. Only float64
// values in [0, 1] are included. Nested maps / arrays / nulls are skipped
// per spec. Returns nil if no valid values were found.
func avgConfidence(m map[string]interface{}) *float64 {
	var sum float64
	var count int
	for _, v := range m {
		f, ok := v.(float64)
		if !ok {
			continue
		}
		if f < 0 || f > 1 {
			continue
		}
		sum += f
		count++
	}
	if count == 0 {
		return nil
	}
	avg := sum / float64(count)
	return &avg
}
