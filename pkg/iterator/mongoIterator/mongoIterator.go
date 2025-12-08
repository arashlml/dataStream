// TODO: package names should not be in camel case, use mongo_iterator instead of mongoIterator or mongoiterator, also the folder name should match the package name | Done✅
package mongoiterator

import (
	"context"
	"log"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TODO: better naming for this interface, maybe ZeroChecker? | Done✅

type MongoIterator struct {
	col         *mongo.Collection
	batchSize   int64
	batch       []bson.M
	lastID      primitive.ObjectID
	nextCounter int64
	cursor      *mongo.Cursor
}

func NewMongoIterator(col *mongo.Collection, batchSize int64) *MongoIterator {
	// TODO: always set defaults for your configuration values, what if the batchSize is 0 or negative? | later!
	m := &MongoIterator{
		col:       col,
		batchSize: batchSize,
	}
	return m
}

func (m *MongoIterator) Next(ctx context.Context) error {
	filter := bson.M{}
	if !m.lastID.IsZero() {
		filter["_id"] = bson.M{"$gt": m.lastID}
	}

	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(m.batchSize)
	var err error
	// TODO: close the cursor after use to avoid resource leak | Done✅
	m.cursor, err = m.col.Find(ctx, filter, opts)
	if err != nil {
		return err
	}

	m.batch = []bson.M{}

	if err := m.cursor.All(ctx, &m.batch); err != nil {
		return err
	}
	if len(m.batch) > 0 {
		doc := m.batch[len(m.batch)-1]
		// TODO: unsafe type assertion, this can lead to panic, always check the type assertion | Done✅
		if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
			log.Println("lastID is not a primitive.ObjectID")
		} else {
			m.lastID = lastID
		}
	}
	atomic.AddInt64(&m.nextCounter, 1)
	// TODO: use a proper logger instead of log.Printf, use slog.Logger | Next time !
	log.Printf("next counter: %d", atomic.LoadInt64(&m.nextCounter))
	return nil
}

// TODO: GetBatch, or CurrentBatch are better names, Batch is confusing, because it sounds it is creating a new batch | Done✅
func (m *MongoIterator) CurrentBatch() []bson.M {
	return m.batch
}

func (m *MongoIterator) HasNext(ctx context.Context) bool {
	// TODO: instead of checking for empty ID here, you can compare the length of original batch with the current batch size | Done✅
	// for example, if the original batch size is 50, and the current batch size is 43, then there is no more data to read, and this is the last batch | Done✅
	if int64(len(m.batch)) < m.batchSize && !m.lastID.IsZero() {
		if err := m.cursor.Close(ctx); err != nil {
			log.Printf("MONGO ITERATOR: ERROR CLOSING THE CURSOR --> %v \n", err)
			return false
		}
		return false
	}
	return true
}
