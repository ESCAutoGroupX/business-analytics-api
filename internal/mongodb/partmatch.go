package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PartMatch mirrors alpha.wickedfile.com store.partMatch — explicit
// part-to-part links between an invoice line and a matching RO/return/etc.
type PartMatch struct {
	ID                    primitive.ObjectID  `bson:"_id"`
	RunID                 string              `bson:"runId"`
	LocationID            string              `bson:"locationId"`
	InvoicePartAuditID    *primitive.ObjectID `bson:"invoicePartAuditId"`
	MatchPartAuditID      *primitive.ObjectID `bson:"matchPartAuditId"`
	InvoicePartLineItemID string              `bson:"invoicePartLineItemId"`
	MatchPartLineItemID   string              `bson:"matchPartLineItemId"`
	InvoiceObjectID       *primitive.ObjectID `bson:"invoiceObjectId"`
	MatchObjectID         *primitive.ObjectID `bson:"matchObjectId"`
	InvoiceType           string              `bson:"invoiceType"`
	MatchType             string              `bson:"matchType"`
	Score                 *int                `bson:"score"`
	Algo                  string              `bson:"algo"`
	MatchedBy             string              `bson:"matchedBy"`
	InvoiceAmount         *float64            `bson:"invoiceAmount"`
	MatchAmount           *float64            `bson:"matchAmount"`
	InvoiceQuantity       *float64            `bson:"invoiceQuantity"`
	MatchQuantity         *float64            `bson:"matchQuantity"`
	CreatedAt             time.Time           `bson:"createdAt"`
	UpdatedAt             time.Time           `bson:"updatedAt"`
}

// PartMatchSource mirrors the other cursor interfaces.
type PartMatchSource interface {
	Next(ctx context.Context) bool
	Decode(out *PartMatch) error
	Err() error
	Close(ctx context.Context) error
}

// NewPartMatchCursor opens a filtered, updatedAt-ordered cursor over partMatch.
func NewPartMatchCursor(ctx context.Context, db *mongo.Database, watermark time.Time, batchSize int32) (PartMatchSource, error) {
	filter := bson.M{}
	if !watermark.IsZero() {
		filter["updatedAt"] = bson.M{"$gt": watermark}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "updatedAt", Value: 1}}).
		SetBatchSize(batchSize).
		SetAllowDiskUse(true)
	cur, err := db.Collection("partMatch").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	return &mongoPartMatchSource{cur: cur}, nil
}

type mongoPartMatchSource struct{ cur *mongo.Cursor }

func (m *mongoPartMatchSource) Next(ctx context.Context) bool      { return m.cur.Next(ctx) }
func (m *mongoPartMatchSource) Decode(out *PartMatch) error        { return m.cur.Decode(out) }
func (m *mongoPartMatchSource) Err() error                         { return m.cur.Err() }
func (m *mongoPartMatchSource) Close(ctx context.Context) error    { return m.cur.Close(ctx) }
