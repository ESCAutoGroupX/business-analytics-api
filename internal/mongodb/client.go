// Package mongodb provides a pooled, process-wide client for WickedFile's
// MongoDB replica over VPC peering. It is intentionally minimal — callers
// grab WickedFileDB() and run their own collection operations.
package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client     *mongo.Client
	clientOnce sync.Once
	clientErr  error
)

// WickedFileDB returns a handle to the ESC WickedFile database.
// Connection is pooled and shared across the process.
func WickedFileDB() (*mongo.Database, error) {
	clientOnce.Do(func() {
		uri := os.Getenv("WF_MONGO_URI")
		if uri == "" {
			uri = "mongodb://10.0.3.231:27017"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// AllowTruncatingDoubles lets the BSON decoder narrow a float64 into
		// an int struct field (truncating the fractional part). WickedFile
		// stores partMatch.score as float64 in some documents; without this
		// flag the decoder drops the whole record rather than round off.
		opts := options.Client().
			ApplyURI(uri).
			SetMaxPoolSize(20).
			SetMinPoolSize(2).
			SetServerSelectionTimeout(5 * time.Second).
			SetConnectTimeout(5 * time.Second).
			SetBSONOptions(&options.BSONOptions{AllowTruncatingDoubles: true})

		c, err := mongo.Connect(ctx, opts)
		if err != nil {
			clientErr = fmt.Errorf("mongo connect: %w", err)
			return
		}
		if err := c.Ping(ctx, nil); err != nil {
			clientErr = fmt.Errorf("mongo ping: %w", err)
			return
		}
		client = c
		log.Printf("MongoDB: connected to %s", uri)
	})
	if clientErr != nil {
		return nil, clientErr
	}
	dbName := os.Getenv("WF_MONGO_DB")
	if dbName == "" {
		dbName = "a6fadc1b-c134-4cbb-b2a2-277f0595d7d6"
	}
	return client.Database(dbName), nil
}

// Close shuts the client. Call on app shutdown.
func Close(ctx context.Context) error {
	if client == nil {
		return nil
	}
	return client.Disconnect(ctx)
}
