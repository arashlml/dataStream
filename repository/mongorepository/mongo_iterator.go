package mongorepository

import (
	"context"
	"log/slog"

	"github.com/arashlml/mongo-reader/entity"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Iterator struct {
	col       *mongo.Collection
	batchSize int64
	batch     []bson.M
	hasNext   bool
	cursor    *mongo.Cursor
	logger    *slog.Logger
}

func NewIterator(col *mongo.Collection, batchSize int64, logger *slog.Logger) *Iterator {
	if batchSize <= 0 {
		batchSize = 50
	}
	m := &Iterator{
		col:       col,
		batchSize: batchSize,
		hasNext:   true,
		logger:    logger,
	}
	return m
}

func (m *Iterator) Next(ctx context.Context, lastID string) (*entity.RawCollection, error) {
	filter := bson.M{}
	if lastID != "" {
		filter["_id"] = bson.M{"$gt": lastID}
	}
	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(m.batchSize)
	var err error

	m.cursor, err = m.col.Find(ctx, filter, opts)
	if err != nil {
		m.logger.Error("mongo.col.Find().error",
			"error", err,
			"_id", lastID,
		)
		return nil, err
	}
	defer m.cursor.Close(ctx)
	m.batch = []bson.M{}

	if err := m.cursor.All(ctx, &m.batch); err != nil {
		m.logger.Error("mongo.cursor.all.error",
			"error", err,
			"_id", lastID,
		)
		return nil, err
	}

	m.hasNext = m.batchSize == int64(len(m.batch))
	convertedBatch := m.ConvertedBatch()
	return &convertedBatch, nil
}

func (m *Iterator) ConvertedBatch() entity.RawCollection {
	var convertedBatch []map[string]interface{}
	for _, doc := range m.batch {
		convertedBatch = append(convertedBatch, doc)
	}
	return convertedBatch
}

func (m *Iterator) HasNext(ctx context.Context) bool {
	return m.hasNext
}
