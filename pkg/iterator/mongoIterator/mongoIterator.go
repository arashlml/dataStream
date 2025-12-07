package mongoIterator

import (
	"context"
	"log"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type X interface {
	IsZero() bool
}

type MongoIterator struct {
	col         *mongo.Collection
	batchSize   int64
	batch       []bson.M
	lastID      primitive.ObjectID
	nextCounter int64
}

func NewMongoIterator(col *mongo.Collection, batchSize int64) *MongoIterator {
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

	cursor, err := m.col.Find(ctx, filter, opts)
	if err != nil {
		return err
	}

	m.batch = []bson.M{}

	if err := cursor.All(ctx, &m.batch); err != nil {
		return err
	}
	if len(m.batch) > 0 {
		doc := m.batch[len(m.batch)-1]
		m.lastID = doc["_id"].(primitive.ObjectID)
	}
	atomic.AddInt64(&m.nextCounter, 1)
	log.Printf("next counter: %d", atomic.LoadInt64(&m.nextCounter))
	return nil
}

func (m *MongoIterator) Batch() []bson.M {
	return m.batch
}

func (m *MongoIterator) HasNext() bool {
	if len(m.batch) == 0 && !m.lastID.IsZero() {
		return false
	}
	return true
}
