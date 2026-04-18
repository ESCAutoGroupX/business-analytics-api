package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PartAudit mirrors alpha.wickedfile.com store.partAudit — one record per
// line item on a scanned document. No type filter is needed when syncing
// these; every record is a real line item.
type PartAudit struct {
	ID                primitive.ObjectID  `bson:"_id"`
	ObjectID          *primitive.ObjectID `bson:"objectId"` // scanPage._id
	LocationID        string              `bson:"locationId"`
	Type              string              `bson:"type"`
	Category          string              `bson:"category"`
	ProductCode       string              `bson:"productCode"`
	ProductCodeSearch string              `bson:"productCodeSearch"`
	Description       string              `bson:"description"`
	Quantity          *float64            `bson:"quantity"`
	UnitPrice         *float64            `bson:"unitPrice"`
	Amount            *float64            `bson:"amount"`
	InvoiceID         string              `bson:"invoiceId"`
	PurchaseOrder     string              `bson:"purchaseOrder"`
	VendorName        string              `bson:"vendorName"`
	ROObjectID        *primitive.ObjectID `bson:"roObjectId"`
	RONumber          string              `bson:"roNumber"`
	StartDate         *time.Time          `bson:"startDate"`
	EndDate           *time.Time          `bson:"endDate"`
	CreatedAt         time.Time           `bson:"createdAt"`
	UpdatedAt         time.Time           `bson:"updatedAt"`
}

// PartAuditSource is the cursor interface — mirrors ScanPageSource so
// tests can feed in-memory fakes.
type PartAuditSource interface {
	Next(ctx context.Context) bool
	Decode(out *PartAudit) error
	Err() error
	Close(ctx context.Context) error
}

// NewPartAuditCursor opens a filtered, updatedAt-ordered cursor over
// partAudit. Zero watermark means full first run.
func NewPartAuditCursor(ctx context.Context, db *mongo.Database, watermark time.Time, batchSize int32) (PartAuditSource, error) {
	filter := bson.M{}
	if !watermark.IsZero() {
		filter["updatedAt"] = bson.M{"$gt": watermark}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "updatedAt", Value: 1}}).
		SetBatchSize(batchSize).
		SetAllowDiskUse(true)
	cur, err := db.Collection("partAudit").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	return &mongoPartAuditSource{cur: cur}, nil
}

type mongoPartAuditSource struct{ cur *mongo.Cursor }

func (m *mongoPartAuditSource) Next(ctx context.Context) bool       { return m.cur.Next(ctx) }
func (m *mongoPartAuditSource) Decode(out *PartAudit) error         { return m.cur.Decode(out) }
func (m *mongoPartAuditSource) Err() error                          { return m.cur.Err() }
func (m *mongoPartAuditSource) Close(ctx context.Context) error     { return m.cur.Close(ctx) }
