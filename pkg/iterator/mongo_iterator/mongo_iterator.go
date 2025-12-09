package mongo_iterator

import (
	"context"
	"log"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoIterator struct {
	col         *mongo.Collection
	batchSize   int64
	batch       []bson.M
	lastID      primitive.ObjectID
	nextCounter int64
	cursor      *mongo.Cursor
}

func NewMongoIterator(col *mongo.Collection, batchSize int64) *MongoIterator {
	if batchSize <= 0 {
		batchSize = 50
	}
	m := &MongoIterator{
		col:       col,
		batchSize: batchSize,
	}
	return m
}

func (m *MongoIterator) Next(ctx context.Context) error {
	if m.cursor != nil {
		if err := m.cursor.Close(ctx); err != nil {
			log.Printf("MONGO ITERATOR: ERROR CLOSING THE CURSOR --> %v \n", err)
		}
	}
	filter := bson.M{}
	if !m.lastID.IsZero() {
		filter["_id"] = bson.M{"$gt": m.lastID}
	}
	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(m.batchSize)
	var err error

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

		if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
			log.Printf("MONGO ITERATOR: ERROR: _id is not a primitive.ObjectID")
		} else {
			m.lastID = lastID
		}
	}
	atomic.AddInt64(&m.nextCounter, 1)

	return nil
}

func (m *MongoIterator) CurrentBatch() []bson.M {
	return m.batch
}

func (m *MongoIterator) HasNext(ctx context.Context) bool {
	if int64(len(m.batch)) < m.batchSize && !m.lastID.IsZero() {
		if err := m.cursor.Close(ctx); err != nil {
			log.Printf("MONGO ITERATOR: ERROR CLOSING THE CURSOR --> %v \n", err)
			return false
		}
		return false
	}
	return true
}
