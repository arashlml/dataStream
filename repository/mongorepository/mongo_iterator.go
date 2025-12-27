package mongorepository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/arashlml/mongo-reader/dto"
	"github.com/arashlml/mongo-reader/metrics"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Iterator struct {
	col         *mongo.Collection
	batchSize   int64
	batch       []bson.M
	hasNext     bool
	cursor      *mongo.Cursor
	logger      *slog.Logger
	idType      string
	readTimeout time.Duration
	metrics     *metrics.Metrics
}

func NewIterator(col *mongo.Collection, batchSize int64, logger *slog.Logger, idType string, readTimeout time.Duration, metrics *metrics.Metrics) *Iterator {
	if batchSize <= 0 {
		batchSize = 50
	}
	i := &Iterator{
		col:         col,
		batchSize:   batchSize,
		hasNext:     true,
		logger:      logger,
		idType:      idType,
		readTimeout: readTimeout,
		metrics:     metrics,
	}
	return i
}
func (i *Iterator) ConvertID(lastID string) (interface{}, error) {
	switch i.idType {
	case "ObjectID":
		return primitive.ObjectIDFromHex(lastID)
	case "String":
		return lastID, nil
	default:
		return nil, fmt.Errorf("unsupported ID type")
	}
}

func (i *Iterator) Next(ctx context.Context, lastID string) (*dto.RawCollection, error) {
	filter := bson.M{}
	if lastID != "" {
		id, err := i.ConvertID(lastID)
		if err != nil {
			i.logger.Error("repository.mongo.iterator.convert.id.error",
				"error", err,
				"lastID", lastID)
		}
		filter["_id"] = bson.M{"$gt": id}
	}
	ctx, _ := context.WithTimeout(ctx, i.readTimeout*time.Second)

	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(i.batchSize)
	var err error

	i.cursor, err = i.col.Find(ctx, filter, opts)
	if err != nil {
		i.logger.Error("mongo.col.Find().error",
			"error", err,
			"_id", lastID,
		)
		return nil, err
	}
	defer i.cursor.Close(ctx)
	i.batch = []bson.M{}

	if err := i.cursor.All(ctx, &i.batch); err != nil {
		i.logger.Error("mongo.cursor.all.error",
			"error", err,
			"_id", lastID,
		)
		return nil, err
	}

	i.hasNext = i.batchSize == int64(len(i.batch))
	convertedBatch := i.ConvertedBatch()
	return &convertedBatch, nil
}

func (i *Iterator) ConvertedBatch() dto.RawCollection {
	var convertedBatch []map[string]interface{}
	for _, doc := range i.batch {
		convertedBatch = append(convertedBatch, doc)
	}
	return convertedBatch
}

func (i *Iterator) HasNext(ctx context.Context) bool {
	return i.hasNext
}
