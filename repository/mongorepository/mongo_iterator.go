package mongorepository

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/mongo-reader/state"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoIterator struct {
	col       *mongo.Collection
	batchSize int64
	batch     []bson.M
	hasNext   bool
	cursor    *mongo.Cursor
	logger    *slog.Logger
	state     *state.State
}

func NewMongoIterator(col *mongo.Collection, batchSize int64, logger *slog.Logger, state *state.State) *MongoIterator {
	if batchSize <= 0 {
		batchSize = 50
	}
	m := &MongoIterator{
		col:       col,
		batchSize: batchSize,
		hasNext:   true,
		logger:    logger,
		state:     state,
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
	if !m.state.LastID.IsZero() {
		filter["_id"] = bson.M{"$gt": m.state.LastID}
	}
	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(m.batchSize)
	var err error

	m.cursor, err = m.col.Find(ctx, filter, opts)
	if err != nil {
		m.logger.Error("mongo.col.Find().error",
			"error", err,
			"_id", m.state.LastID,
		)
		return err
	}

	m.batch = []bson.M{}

	if err := m.cursor.All(ctx, &m.batch); err != nil {
		m.logger.Error("mongo.cursor.all.error",
			"error", err,
			"_id", m.state.LastID,
		)
		return err
	}
	if len(m.batch) > 0 {
		doc := m.batch[len(m.batch)-1]
		atomic.AddInt64(&m.state.TotalReadDocuments, int64(len(m.batch)))
		if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
			m.logger.Warn(
				"mongo.record.invalid_id",
				"_id", lastID,
				"id_type", fmt.Sprintf("%T", lastID),
			)
		} else {
			m.state.SetLastID(lastID)
		}
	}
	m.state.SetBsonBatch(m.batch)
	m.hasNext = m.batchSize == int64(len(m.batch))
	return nil
}

func (m *MongoIterator) CurrentBatch() []bson.M {
	return m.batch
}

func (m *MongoIterator) HasNext(ctx context.Context) bool {
	//TODO:
	if !m.hasNext {
		m.logger.Info(
			"mongo.iteration.finished",
			"_id", m.state.LastID,
		)
		err := m.cursor.Close(ctx)
		if err != nil {
			m.logger.Error("mongo.cursor.close.error", "error", err)
		}
		return m.hasNext

	}
	return m.hasNext
}
