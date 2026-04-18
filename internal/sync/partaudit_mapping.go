package sync

import (
	"time"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

// Column caps on wf_part_audits.
const (
	lenAuditType       = 32
	lenAuditCategory   = 32
	lenProductCode     = 128
	lenProductCodeSrch = 128
	lenInvoiceIDPart   = 64
	lenPurchaseOrder   = 64
	lenVendorName      = 255
	lenRONumber        = 64
	lenROObjectID      = 24
)

// PartAuditRow is one wf_part_audits row. DocumentID is populated by the
// batch resolver before upsert; the mapping itself just records the source
// scanPage hex so the resolver can look it up.
type PartAuditRow struct {
	WfAuditID         string
	WfObjectID        *string
	DocumentID        *int64 // resolved from wf_object_id → documents.wf_scan_page_id
	LocationID        *string
	AuditType         *string
	AuditCategory     *string
	ProductCode       *string
	ProductCodeSearch *string
	Description       *string
	Quantity          *float64
	UnitPrice         *float64
	Amount            *float64
	InvoiceID         *string
	PurchaseOrder     *string
	VendorName        *string
	ROObjectID        *string
	RONumber          *string
	StartDate         *time.Time
	EndDate           *time.Time
	WfUpdatedAt       time.Time
}

// MapPartAudit converts a Mongo partAudit into a PartAuditRow. Pure, no I/O.
func MapPartAudit(p *mongodb.PartAudit) PartAuditRow {
	row := PartAuditRow{
		WfAuditID:   p.ID.Hex(),
		WfUpdatedAt: p.UpdatedAt,
	}
	if p.ObjectID != nil {
		hex := p.ObjectID.Hex()
		row.WfObjectID = &hex
	}
	if v := strOrNil(p.LocationID); v != nil {
		row.LocationID = v
	}
	if v := strOrNil(p.Type); v != nil {
		t := truncate("audit_type", *v, lenAuditType)
		row.AuditType = &t
	}
	if v := strOrNil(p.Category); v != nil {
		t := truncate("audit_category", *v, lenAuditCategory)
		row.AuditCategory = &t
	}
	if v := strOrNil(p.ProductCode); v != nil {
		t := truncate("product_code", *v, lenProductCode)
		row.ProductCode = &t
	}
	if v := strOrNil(p.ProductCodeSearch); v != nil {
		t := truncate("product_code_search", *v, lenProductCodeSrch)
		row.ProductCodeSearch = &t
	}
	if v := strOrNil(p.Description); v != nil {
		row.Description = v // TEXT, no cap
	}
	if p.Quantity != nil {
		row.Quantity = p.Quantity
	}
	if p.UnitPrice != nil {
		row.UnitPrice = p.UnitPrice
	}
	if p.Amount != nil {
		row.Amount = p.Amount
	}
	if v := strOrNil(p.InvoiceID); v != nil {
		t := truncate("invoice_id", *v, lenInvoiceIDPart)
		row.InvoiceID = &t
	}
	if v := strOrNil(p.PurchaseOrder); v != nil {
		t := truncate("purchase_order", *v, lenPurchaseOrder)
		row.PurchaseOrder = &t
	}
	if v := strOrNil(p.VendorName); v != nil {
		t := truncate("vendor_name", *v, lenVendorName)
		row.VendorName = &t
	}
	if p.ROObjectID != nil {
		hex := p.ROObjectID.Hex()
		row.ROObjectID = &hex
	}
	if v := strOrNil(p.RONumber); v != nil {
		t := truncate("ro_number", *v, lenRONumber)
		row.RONumber = &t
	}
	if p.StartDate != nil && !p.StartDate.IsZero() {
		row.StartDate = p.StartDate
	}
	if p.EndDate != nil && !p.EndDate.IsZero() {
		row.EndDate = p.EndDate
	}
	return row
}
