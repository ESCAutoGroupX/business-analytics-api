package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MatchedAuditTypes are the document-side statementAudit types we sync.
// We deliberately skip type="transaction" records (60k+ of them); those
// are the reverse side of matches already captured via the document-side
// record, so including them double-counts.
var MatchedAuditTypes = []string{"invoice", "invoice-other", "credit", "credit-other", "statement"}

// StatementAudit mirrors alpha.wickedfile.com store.statementAudit. Each
// record holds three parallel match arrays — transactionMatches,
// scanMatches, accountingMatches — and the sync engine explodes each
// non-empty entry into one wf_match_results row.
type StatementAudit struct {
	ID                 primitive.ObjectID      `bson:"_id"`
	ScanPageID         *primitive.ObjectID     `bson:"scanPageId"`
	LocationID         string                  `bson:"locationId"`
	Date               *time.Time              `bson:"date"`
	Amount             *float64                `bson:"amount"`
	InvoiceID          string                  `bson:"invoiceId"`
	Vendor             string                  `bson:"vendor"`
	Type               string                  `bson:"type"`
	Category           string                  `bson:"category"`
	UpdatedAt          time.Time               `bson:"updatedAt"`
	TransactionMatches []TransactionMatchEntry `bson:"transactionMatches"`
	ScanMatches        []ScanMatchEntry        `bson:"scanMatches"`
	AccountingMatches  []AccountingMatchEntry  `bson:"accountingMatches"`
	Risk               *RiskBlock              `bson:"risk"`
}

type TransactionMatchEntry struct {
	ID         primitive.ObjectID       `bson:"id"`
	Percentage *float64                 `bson:"percentage"`
	Type       string                   `bson:"type"`
	MatchedBy  string                   `bson:"by"`
	Date       *time.Time               `bson:"date"`
	Match      *TransactionMatchPayload `bson:"match"`
}

type TransactionMatchPayload struct {
	SourceTransactionID string     `bson:"sourceTransactionId"`
	Amount              *float64   `bson:"amount"`
	Date                *time.Time `bson:"date"`
	Text                string     `bson:"text"`
	By                  string     `bson:"by"`
}

type ScanMatchEntry struct {
	ID         primitive.ObjectID `bson:"id"`
	Percentage *float64           `bson:"percentage"`
	Type       string             `bson:"type"`
	MatchedBy  string             `bson:"by"`
	Date       *time.Time         `bson:"date"`
	Match      bson.M             `bson:"match"`
}

type AccountingMatchEntry struct {
	ID         primitive.ObjectID `bson:"id"`
	Percentage *float64           `bson:"percentage"`
	Type       string             `bson:"type"`
	MatchedBy  string             `bson:"by"`
	Match      bson.M             `bson:"match"`
}

type RiskBlock struct {
	Score    *int   `bson:"score"`
	Category string `bson:"category"`
}

// StatementAuditSource abstracts a Mongo cursor so the engine can be unit
// tested with an in-memory fake (same pattern as ScanPageSource).
type StatementAuditSource interface {
	Next(ctx context.Context) bool
	Decode(out *StatementAudit) error
	Err() error
	Close(ctx context.Context) error
}

// NewStatementAuditCursor opens a filtered, updatedAt-ordered cursor over
// statementAudit for the matched + document-side types only.
func NewStatementAuditCursor(ctx context.Context, db *mongo.Database, watermark time.Time, batchSize int32) (StatementAuditSource, error) {
	filter := bson.M{
		"category": "matched",
		"type":     bson.M{"$in": MatchedAuditTypes},
	}
	if !watermark.IsZero() {
		filter["updatedAt"] = bson.M{"$gt": watermark}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "updatedAt", Value: 1}}).
		SetBatchSize(batchSize).
		SetAllowDiskUse(true)
	cur, err := db.Collection("statementAudit").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	return &mongoStatementAuditSource{cur: cur}, nil
}

type mongoStatementAuditSource struct {
	cur *mongo.Cursor
}

func (m *mongoStatementAuditSource) Next(ctx context.Context) bool      { return m.cur.Next(ctx) }
func (m *mongoStatementAuditSource) Decode(out *StatementAudit) error  { return m.cur.Decode(out) }
func (m *mongoStatementAuditSource) Err() error                        { return m.cur.Err() }
func (m *mongoStatementAuditSource) Close(ctx context.Context) error   { return m.cur.Close(ctx) }
