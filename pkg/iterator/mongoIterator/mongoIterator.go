package mongoIterator

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type X interface {
	IsZero() bool
}

type MongoIterator struct {
	col       *mongo.Collection
	batchSize int64
	batch     []map[string]interface{}
	lastID    X
}

func NewMongoIterator(col *mongo.Collection, batchSize int64) *MongoIterator {
	m := &MongoIterator{
		col:       col,
		batchSize: batchSize,
	}
	return m
}
func (m *MongoIterator) UpdateLastID(lastID X) {
	m.lastID = lastID
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

	m.batch = []map[string]interface{}{}

	if err := cursor.All(ctx, &m.batch); err != nil {
		return err
	}
	return nil
}
func (m *MongoIterator) Batch() []map[string]interface{} {
	return m.batch
}

func (m *MongoIterator) HasNext() bool {

	if len(m.batch) == 0 && !m.lastID.IsZero() {
		return false
	}
	return true
}
