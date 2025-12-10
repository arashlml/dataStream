package mongo_iterator

import (
	"context"
	"fmt"
	"log/slog"
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
	readCounter int64
	hasNext     bool
	cursor      *mongo.Cursor
	logger      *slog.Logger
}

func NewMongoIterator(col *mongo.Collection, batchSize int64, logger *slog.Logger) *MongoIterator {
	if batchSize <= 0 {
		batchSize = 50
	}
	m := &MongoIterator{
		col:       col,
		batchSize: batchSize,
		hasNext:   true,
		logger:    logger,
	}
	return m
}

func (m *MongoIterator) Next(ctx context.Context) error {
	if m.cursor != nil {
		if err := m.cursor.Close(ctx); err != nil {
			m.logger.Error("mongo.cursor.close.error",
				"error", err)
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
		m.logger.Error("mongo.col.Find().error",
			"error", err,
			"_id", m.lastID,
		)
		return err
	}

	m.batch = []bson.M{}

	if err := m.cursor.All(ctx, &m.batch); err != nil {
		m.logger.Error("mongo.cursor.all.error",
			"error", err,
			"_id", m.lastID,
		)
		return err
	}
	if len(m.batch) > 0 {
		doc := m.batch[len(m.batch)-1]
		atomic.AddInt64(&m.readCounter, 1)
		if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
			m.logger.Warn(
				"mongo.record.invalid_id",
				"_id", lastID,
				"id_type", fmt.Sprintf("%T", lastID),
				"count", atomic.LoadInt64(&m.readCounter),
			)
		} else {

			m.logger.Info(
				"mongo.record.read",
				"_id", lastID,
				"count", atomic.LoadInt64(&m.readCounter),
			)

			m.lastID = lastID
		}
	}
	m.hasNext = m.batchSize == int64(len(m.batch))

	atomic.AddInt64(&m.nextCounter, 1)

	return nil
}

func (m *MongoIterator) CurrentBatch() []bson.M {
	return m.batch
}

func (m *MongoIterator) HasNext(ctx context.Context) bool {
	if !m.hasNext {
		m.logger.Info(
			"mongo.iteration.finished",
			"_id", m.lastID,
		)
		err := m.cursor.Close(ctx)
		if err != nil {
			m.logger.Error("mongo.cursor.close.error", "error", err)
		}
		return m.hasNext
	}
	return m.hasNext
}
