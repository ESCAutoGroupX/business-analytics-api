package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// FinancialDocTypes filters scanPage to invoice-like documents only —
// anything we'd reconcile against bank transactions or vendor statements.
var FinancialDocTypes = []string{
	"invoice", "invoice-other", "rma", "credit", "credit-other", "statement",
}

// ScanPage mirrors the subset of alpha.wickedfile.com store.scanPage that
// the Postgres sync actually reads. Fields we don't need (ocr words, raw
// phrase geometry, review trails) stay in Mongo for lazy drilldown.
type ScanPage struct {
	ID         primitive.ObjectID `bson:"_id"`
	LocationID string             `bson:"locationId"`
	S3Key      string             `bson:"s3Key"`
	Type       string             `bson:"type"`
	Ignored    *bool              `bson:"ignored"`
	MLReviewed *bool              `bson:"mlReviewed"`
	CreatedAt  time.Time          `bson:"createdAt"`
	UpdatedAt  time.Time          `bson:"updatedAt"`
	Search     ScanPageSearch     `bson:"search"`
	Fields     ScanPageFields     `bson:"fields"`
}

type ScanPageSearch struct {
	MLParsed      bool        `bson:"mlParsed"`
	Total         *float64    `bson:"total"`
	Dates         []time.Time `bson:"dates"`
	InvoiceNumber string      `bson:"invoiceNumber"`
	PoNumber      string      `bson:"poNumber"`
	Tax           *float64    `bson:"tax"`
	SubTotal      *float64    `bson:"subTotal"`
	Vendor        string      `bson:"vendor"`
	LineItems     []bson.M    `bson:"lineItems"`
}

type ScanPageFields struct {
	Formatted  ScanPageFormatted `bson:"formatted"`
	Confidence bson.M            `bson:"confidence"`
}

type ScanPageFormatted struct {
	VendorName    string     `bson:"vendorName"`
	InvoiceTotal  *float64   `bson:"invoiceTotal"`
	InvoiceDate   *time.Time `bson:"invoiceDate"`
	InvoiceID     string     `bson:"invoiceId"`
	PurchaseOrder string     `bson:"purchaseOrder"`
	SubTotal      *float64   `bson:"subTotal"`
	Items         []bson.M   `bson:"items"`
}

// ScanPageSource abstracts a Mongo cursor so the sync engine can be unit
// tested with an in-memory fake. Shape matches mongo.Cursor closely.
type ScanPageSource interface {
	Next(ctx context.Context) bool
	Decode(out *ScanPage) error
	Err() error
	Close(ctx context.Context) error
}

// NewScanPageCursor opens a filtered, updatedAt-ordered cursor over the
// scanPage collection. Pass a zero watermark for a full first-run sync.
func NewScanPageCursor(ctx context.Context, db *mongo.Database, watermark time.Time, batchSize int32) (ScanPageSource, error) {
	filter := bson.M{
		"type":    bson.M{"$in": FinancialDocTypes},
		"ignored": bson.M{"$ne": true},
	}
	if !watermark.IsZero() {
		filter["updatedAt"] = bson.M{"$gt": watermark}
	}
	// AllowDiskUse is required — updatedAt isn't indexed on scanPage, so
	// Mongo would otherwise hit its 100 MB in-memory sort limit on the
	// first run. Disk spill is cheap for a single one-shot pass.
	opts := options.Find().
		SetSort(bson.D{{Key: "updatedAt", Value: 1}}).
		SetBatchSize(batchSize).
		SetAllowDiskUse(true)
	cur, err := db.Collection("scanPage").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	return &mongoScanPageSource{cur: cur}, nil
}

type mongoScanPageSource struct {
	cur *mongo.Cursor
}

func (m *mongoScanPageSource) Next(ctx context.Context) bool  { return m.cur.Next(ctx) }
func (m *mongoScanPageSource) Decode(out *ScanPage) error     { return m.cur.Decode(out) }
func (m *mongoScanPageSource) Err() error                     { return m.cur.Err() }
func (m *mongoScanPageSource) Close(ctx context.Context) error { return m.cur.Close(ctx) }
