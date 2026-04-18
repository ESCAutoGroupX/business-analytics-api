package sync

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// PartAuditWriter upserts a batch of PartAuditRow.
type PartAuditWriter interface {
	UpsertBatch(ctx context.Context, rows []PartAuditRow) (inserted, updated int, err error)
}

// PartAuditFKResolver populates DocumentID from the row's wf_object_id
// (the scanPage hex) with a single per-batch SELECT against documents.
type PartAuditFKResolver interface {
	Resolve(ctx context.Context, rows []PartAuditRow) ([]PartAuditRow, error)
}

// PostgresPartAuditFKResolver is the live-DB resolver.
type PostgresPartAuditFKResolver struct{ DB *gorm.DB }

func (r *PostgresPartAuditFKResolver) Resolve(ctx context.Context, rows []PartAuditRow) ([]PartAuditRow, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	scanSet := map[string]struct{}{}
	for _, row := range rows {
		if row.WfObjectID != nil && *row.WfObjectID != "" {
			scanSet[*row.WfObjectID] = struct{}{}
		}
	}
	if len(scanSet) == 0 {
		return rows, nil
	}
	ids := setKeys(scanSet)
	type docRow struct {
		ID           int64  `gorm:"column:id"`
		WfScanPageID string `gorm:"column:wf_scan_page_id"`
	}
	var docs []docRow
	if err := r.DB.WithContext(ctx).Raw(
		`SELECT id, wf_scan_page_id FROM documents WHERE wf_scan_page_id IN (?)`, ids,
	).Scan(&docs).Error; err != nil {
		return nil, fmt.Errorf("documents lookup: %w", err)
	}
	byScan := make(map[string]int64, len(docs))
	for _, d := range docs {
		byScan[d.WfScanPageID] = d.ID
	}
	for i := range rows {
		if rows[i].WfObjectID == nil {
			continue
		}
		if id, ok := byScan[*rows[i].WfObjectID]; ok {
			rows[i].DocumentID = &id
		}
	}
	return rows, nil
}

// PostgresPartAuditWriter upserts into wf_part_audits.
type PostgresPartAuditWriter struct{ DB *gorm.DB }

func (w *PostgresPartAuditWriter) UpsertBatch(ctx context.Context, rows []PartAuditRow) (int, int, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}

	const colCount = 20
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
			r.WfAuditID,         // wf_audit_id
			r.WfObjectID,        // wf_object_id
			r.DocumentID,        // document_id
			r.LocationID,        // location_id
			r.AuditType,         // audit_type
			r.AuditCategory,     // audit_category
			r.ProductCode,       // product_code
			r.ProductCodeSearch, // product_code_search
			r.Description,       // description
			r.Quantity,          // quantity
			r.UnitPrice,         // unit_price
			r.Amount,            // amount
			r.InvoiceID,         // invoice_id
			r.PurchaseOrder,     // purchase_order
			r.VendorName,        // vendor_name
			r.ROObjectID,        // ro_object_id
			r.RONumber,          // ro_number
			r.StartDate,         // start_date
			r.EndDate,           // end_date
			r.WfUpdatedAt,       // wf_updated_at
		)
	}

	sql := `
INSERT INTO wf_part_audits (
    wf_audit_id, wf_object_id, document_id, location_id,
    audit_type, audit_category,
    product_code, product_code_search, description,
    quantity, unit_price, amount,
    invoice_id, purchase_order, vendor_name,
    ro_object_id, ro_number, start_date, end_date,
    wf_updated_at
) VALUES ` + strings.Join(placeholders, ",") + `
ON CONFLICT (wf_audit_id) DO UPDATE SET
    wf_object_id        = EXCLUDED.wf_object_id,
    document_id         = EXCLUDED.document_id,
    location_id         = EXCLUDED.location_id,
    audit_type          = EXCLUDED.audit_type,
    audit_category      = EXCLUDED.audit_category,
    product_code        = EXCLUDED.product_code,
    product_code_search = EXCLUDED.product_code_search,
    description         = EXCLUDED.description,
    quantity            = EXCLUDED.quantity,
    unit_price          = EXCLUDED.unit_price,
    amount              = EXCLUDED.amount,
    invoice_id          = EXCLUDED.invoice_id,
    purchase_order      = EXCLUDED.purchase_order,
    vendor_name         = EXCLUDED.vendor_name,
    ro_object_id        = EXCLUDED.ro_object_id,
    ro_number           = EXCLUDED.ro_number,
    start_date          = EXCLUDED.start_date,
    end_date            = EXCLUDED.end_date,
    wf_updated_at       = EXCLUDED.wf_updated_at,
    synced_at           = NOW()
RETURNING (xmax = 0) AS inserted
`
	type result struct {
		Inserted bool `gorm:"column:inserted"`
	}
	var results []result
	if err := w.DB.WithContext(ctx).Raw(sql, args...).Scan(&results).Error; err != nil {
		return 0, 0, err
	}
	var ins, upd int
	for _, r := range results {
		if r.Inserted {
			ins++
		} else {
			upd++
		}
	}
	return ins, upd, nil
}
