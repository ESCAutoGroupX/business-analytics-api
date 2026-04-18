package sync

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ESCAutoGroupX/business-analytics-api/internal/mongodb"
)

func oid(hex string) primitive.ObjectID {
	id, _ := primitive.ObjectIDFromHex(hex)
	return id
}

func floatPtr(f float64) *float64 { return &f }
func boolPtr(b bool) *bool         { return &b }
func timePtr(t time.Time) *time.Time { return &t }

func TestMapScanPageToColumns_HappyPath(t *testing.T) {
	invoiceDate := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	sp := &mongodb.ScanPage{
		ID:         oid("507f1f77bcf86cd799439011"),
		LocationID: "loc-abc",
		S3Key:      "scans/2026/04/01/abc.pdf",
		Type:       "invoice",
		MLReviewed: boolPtr(true),
		UpdatedAt:  invoiceDate,
		Search: mongodb.ScanPageSearch{
			MLParsed:      true,
			Vendor:        "Search Vendor",
			InvoiceNumber: "INV-001",
			PoNumber:      "PO-001",
			SubTotal:      floatPtr(100),
			Tax:           floatPtr(8.5),
			Total:         floatPtr(108.5),
			Dates:         []time.Time{invoiceDate},
			LineItems:     []bson.M{{"a": 1}, {"a": 2}},
		},
		Fields: mongodb.ScanPageFields{
			Formatted: mongodb.ScanPageFormatted{
				VendorName:    "Formatted Vendor",
				InvoiceTotal:  floatPtr(108.50),
				InvoiceDate:   timePtr(invoiceDate),
				InvoiceID:     "F-INV-001",
				PurchaseOrder: "F-PO-001",
				SubTotal:      floatPtr(100),
				Items:         []bson.M{{"x": 1}},
			},
			Confidence: bson.M{"vendor": 0.9, "total": 0.8, "date": 0.7},
		},
	}
	r := MapScanPage(sp)

	if r.WfScanPageID != "507f1f77bcf86cd799439011" {
		t.Errorf("WfScanPageID = %q", r.WfScanPageID)
	}
	if r.WfScanID != "507f1f77bcf86cd799439011" {
		t.Errorf("WfScanID = %q, want dual-set to the ObjectId hex", r.WfScanID)
	}
	if r.WfOCRAgentVersion != "wf-mongo-v1" {
		t.Errorf("WfOCRAgentVersion = %q", r.WfOCRAgentVersion)
	}
	if r.DocumentType == nil || *r.DocumentType != "invoice" {
		t.Errorf("DocumentType = %v", r.DocumentType)
	}
	if r.VendorName == nil || *r.VendorName != "Formatted Vendor" {
		t.Errorf("VendorName = %v, want formatted preferred", r.VendorName)
	}
	if r.TotalAmount == nil || *r.TotalAmount != 108.50 {
		t.Errorf("TotalAmount = %v", r.TotalAmount)
	}
	if r.DocumentDate == nil || *r.DocumentDate != "2026-04-01" {
		t.Errorf("DocumentDate = %v", r.DocumentDate)
	}
	if r.WfInvoiceNumber == nil || *r.WfInvoiceNumber != "F-INV-001" {
		t.Errorf("WfInvoiceNumber = %v", r.WfInvoiceNumber)
	}
	if r.WfPoNumber == nil || *r.WfPoNumber != "F-PO-001" {
		t.Errorf("WfPoNumber = %v", r.WfPoNumber)
	}
	if r.WfLineItemCount == nil || *r.WfLineItemCount != 1 {
		t.Errorf("WfLineItemCount = %v, want 1 (formatted.items preferred)", r.WfLineItemCount)
	}
	if !r.WfMLParsed {
		t.Errorf("WfMLParsed = false, want true")
	}
	if !r.WfMLReviewed {
		t.Errorf("WfMLReviewed = false, want true")
	}
	if r.FilePath != "" {
		t.Errorf("FilePath = %q, want empty so ServeFile proxies WF", r.FilePath)
	}
}

func TestMapScanPageToColumns_CoalesceFallbacks(t *testing.T) {
	invoiceDate := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	sp := &mongodb.ScanPage{
		ID:   oid("607f1f77bcf86cd799439012"),
		Type: "invoice",
		Search: mongodb.ScanPageSearch{
			Vendor:        "Search Vendor",
			InvoiceNumber: "S-INV-42",
			PoNumber:      "S-PO-42",
			SubTotal:      floatPtr(50),
			Total:         floatPtr(54),
			Tax:           floatPtr(4),
			Dates:         []time.Time{invoiceDate},
			LineItems:     []bson.M{{"q": 1}, {"q": 2}, {"q": 3}},
		},
		Fields: mongodb.ScanPageFields{},
	}
	r := MapScanPage(sp)

	if r.VendorName == nil || *r.VendorName != "Search Vendor" {
		t.Errorf("vendor fallback to search.vendor failed: %v", r.VendorName)
	}
	if r.TotalAmount == nil || *r.TotalAmount != 54 {
		t.Errorf("total fallback failed: %v", r.TotalAmount)
	}
	if r.DocumentDate == nil || *r.DocumentDate != "2026-03-15" {
		t.Errorf("date fallback failed: %v", r.DocumentDate)
	}
	if r.WfInvoiceNumber == nil || *r.WfInvoiceNumber != "S-INV-42" {
		t.Errorf("invoice fallback failed: %v", r.WfInvoiceNumber)
	}
	if r.WfPoNumber == nil || *r.WfPoNumber != "S-PO-42" {
		t.Errorf("PO fallback failed: %v", r.WfPoNumber)
	}
	if r.WfSubtotal == nil || *r.WfSubtotal != 50 {
		t.Errorf("subtotal fallback failed: %v", r.WfSubtotal)
	}
	if r.WfTax == nil || *r.WfTax != 4 {
		t.Errorf("tax failed: %v", r.WfTax)
	}
	if r.WfLineItemCount == nil || *r.WfLineItemCount != 3 {
		t.Errorf("line item fallback failed: %v", r.WfLineItemCount)
	}
}

func TestMapScanPageToColumns_ConfidenceAvg(t *testing.T) {
	sp := &mongodb.ScanPage{
		ID: oid("707f1f77bcf86cd799439013"),
		Fields: mongodb.ScanPageFields{
			Confidence: bson.M{
				"vendor":       0.9,
				"total":        0.5,
				"items":        []interface{}{0.1, 0.2}, // array — skipped
				"review":       bson.M{"nested": 0.99},  // nested map — skipped
				"out_of_range": 1.5,                     // outside [0,1] — skipped
				"null_key":     nil,                     // skipped
				"int_key":      1,                       // int, not float64 — skipped
			},
		},
	}
	r := MapScanPage(sp)

	if r.WfOCRConfidence == nil {
		t.Fatalf("expected confidence average, got nil")
	}
	got := *r.WfOCRConfidence
	want := (0.9 + 0.5) / 2
	if diff := got - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("confidence = %v, want %v", got, want)
	}
}

func TestMapScanPageToColumns_ConfidenceNoValidEntries(t *testing.T) {
	sp := &mongodb.ScanPage{
		ID: oid("807f1f77bcf86cd799439014"),
		Fields: mongodb.ScanPageFields{
			Confidence: bson.M{"foo": "bar", "baz": 1.5},
		},
	}
	r := MapScanPage(sp)
	if r.WfOCRConfidence != nil {
		t.Errorf("expected nil confidence when no valid entries, got %v", *r.WfOCRConfidence)
	}
}

func TestMapScanPageToColumns_EmptyStringsBecomeNull(t *testing.T) {
	sp := &mongodb.ScanPage{
		ID:         oid("907f1f77bcf86cd799439015"),
		LocationID: "",
		S3Key:      "",
		Type:       "",
		Search:     mongodb.ScanPageSearch{Vendor: ""},
		Fields:     mongodb.ScanPageFields{Formatted: mongodb.ScanPageFormatted{VendorName: ""}},
	}
	r := MapScanPage(sp)
	if r.WfLocationID != nil {
		t.Errorf("WfLocationID = %v, want nil for empty string", r.WfLocationID)
	}
	if r.WfS3Key != nil {
		t.Errorf("WfS3Key = %v, want nil for empty string", r.WfS3Key)
	}
	if r.DocumentType != nil {
		t.Errorf("DocumentType = %v, want nil for empty string", r.DocumentType)
	}
	if r.VendorName != nil {
		t.Errorf("VendorName = %v, want nil for empty string", r.VendorName)
	}
}

func TestMapScanPageToColumns_Truncation(t *testing.T) {
	long := ""
	for i := 0; i < 200; i++ {
		long += "x"
	}
	sp := &mongodb.ScanPage{
		ID: oid("a07f1f77bcf86cd799439016"),
		Search: mongodb.ScanPageSearch{
			InvoiceNumber: long,
		},
	}
	r := MapScanPage(sp)
	if r.WfInvoiceNumber == nil {
		t.Fatalf("WfInvoiceNumber nil")
	}
	if len(*r.WfInvoiceNumber) != lenInvoiceNumber {
		t.Errorf("invoice number not truncated: len=%d", len(*r.WfInvoiceNumber))
	}
}
