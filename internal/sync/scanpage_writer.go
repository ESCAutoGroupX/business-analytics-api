package sync

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// DocumentWriter persists a batch of DocumentRow into Postgres. Exposed as
// an interface so unit tests can assert on calls without a live DB.
type DocumentWriter interface {
	UpsertBatch(ctx context.Context, rows []DocumentRow) (inserted, updated int, err error)
}

// PostgresDocumentWriter implements DocumentWriter against the real
// documents table. Uses INSERT … ON CONFLICT (wf_scan_page_id) DO UPDATE
// and counts inserts via `(xmax = 0)` in a RETURNING clause.
type PostgresDocumentWriter struct {
	DB *gorm.DB
}

func (w *PostgresDocumentWriter) UpsertBatch(ctx context.Context, rows []DocumentRow) (int, int, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}

	const colCount = 22
	args := make([]interface{}, 0, len(rows)*colCount)
	placeholders := make([]string, 0, len(rows))

	for i, r := range rows {
		base := i * colCount
		ph := make([]string, colCount)
		for j := 0; j < colCount; j++ {
			ph[j] = fmt.Sprintf("$%d", base+j+1)
		}
		placeholders = append(placeholders, "("+strings.Join(ph, ",")+")")

		args = append(args,
			r.Filename,            // filename
			r.FilePath,            // file_path
			r.DocumentType,        // document_type
			r.VendorName,          // vendor_name
			r.TotalAmount,         // total_amount
			r.DocumentDate,        // document_date
			r.WfScanID,            // wf_scan_id  (dual-set for ServeFile fallback)
			r.WfScanPageID,        // wf_scan_page_id
			r.WfLocationID,        // wf_location_id
			r.WfS3Key,             // wf_s3_key
			r.WfOCRAgentVersion,   // wf_ocr_agent_version
			r.WfMLParsed,          // wf_ml_parsed
			r.WfMLReviewed,        // wf_ml_reviewed
			r.WfSyncedAt,          // wf_synced_at
			r.WfInvoiceNumber,     // wf_invoice_number
			r.WfPoNumber,          // wf_po_number
			r.WfSubtotal,          // wf_subtotal
			r.WfTax,               // wf_tax
			r.WfLineItemCount,     // wf_line_item_count
			r.WfOCRConfidence,     // wf_ocr_confidence
			r.Timestamp,           // created_at
			r.Timestamp,           // updated_at
		)
	}

	sql := `
INSERT INTO documents (
    filename, file_path,
    document_type, vendor_name, total_amount, document_date,
    wf_scan_id, wf_scan_page_id, wf_location_id, wf_s3_key,
    wf_ocr_agent_version, wf_ml_parsed, wf_ml_reviewed, wf_synced_at,
    wf_invoice_number, wf_po_number, wf_subtotal, wf_tax, wf_line_item_count, wf_ocr_confidence,
    created_at, updated_at
) VALUES ` + strings.Join(placeholders, ",") + `
ON CONFLICT (wf_scan_page_id) DO UPDATE SET
    document_type        = EXCLUDED.document_type,
    vendor_name          = EXCLUDED.vendor_name,
    total_amount         = EXCLUDED.total_amount,
    document_date        = EXCLUDED.document_date,
    wf_scan_id           = EXCLUDED.wf_scan_id,
    wf_location_id       = EXCLUDED.wf_location_id,
    wf_s3_key            = EXCLUDED.wf_s3_key,
    wf_ocr_agent_version = EXCLUDED.wf_ocr_agent_version,
    wf_ml_parsed         = EXCLUDED.wf_ml_parsed,
    wf_ml_reviewed       = EXCLUDED.wf_ml_reviewed,
    wf_synced_at         = EXCLUDED.wf_synced_at,
    wf_invoice_number    = EXCLUDED.wf_invoice_number,
    wf_po_number         = EXCLUDED.wf_po_number,
    wf_subtotal          = EXCLUDED.wf_subtotal,
    wf_tax               = EXCLUDED.wf_tax,
    wf_line_item_count   = EXCLUDED.wf_line_item_count,
    wf_ocr_confidence    = EXCLUDED.wf_ocr_confidence,
    updated_at           = EXCLUDED.updated_at
RETURNING (xmax = 0) AS inserted
`

	type result struct {
		Inserted bool `gorm:"column:inserted"`
	}
	var results []result
	if err := w.DB.WithContext(ctx).Raw(sql, args...).Scan(&results).Error; err != nil {
		return 0, 0, err
	}
	var inserted, updated int
	for _, r := range results {
		if r.Inserted {
			inserted++
		} else {
			updated++
		}
	}
	return inserted, updated, nil
}
