package mongo_repository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/arashlml/data-stream/dto"
	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
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
}

func NewIterator(col *mongo.Collection, logger *slog.Logger, config model.Config) *Iterator {
	i := &Iterator{
		col:         col,
		batchSize:   config.BatchSize,
		hasNext:     true,
		logger:      logger,
		idType:      config.IDType,
		readTimeout: config.ReadTimeout,
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
		metrics.ErrorCounter.WithLabelValues("mongo_iterator.convert_id.unsupported_type", "unsupported ID type").Inc()
		return nil, fmt.Errorf("unsupported ID type")
	}
}

func (i *Iterator) Next(ctx context.Context, cursor dto.Cursor) (*dto.Collection, error) {
	filter := bson.M{}
	if cursor["lastID"] != nil {
		lastID, ok := cursor["lastID"].(string)
		if !ok {
			i.logger.Warn("repository.mongo.iterator.next.cursor[\"lastID\"].type.assertion.failed")
			return nil, fmt.Errorf(`"lastID" must be a string`)
		}
		id, err := i.ConvertID(lastID)
		if err != nil {
			i.logger.Error("repository.mongo.iterator.convert.id.error",
				"error", err,
				"lastID", cursor["lastID"])
			metrics.ErrorCounter.WithLabelValues("mongo_iterator.next.convert_id", err.Error()).Inc()
			return nil, err
		}

		filter["_id"] = bson.M{"$gt": id}
	}
	readCtx, _ := context.WithTimeout(ctx, i.readTimeout*time.Second)
	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(i.batchSize)
	var err error
	start := time.Now()
	i.cursor, err = i.col.Find(readCtx, filter, opts)
	if err != nil {
		i.logger.Error("mongo.col.Find().error",
			"error", err,
			"_id", cursor["lastID"],
		)
		metrics.ErrorCounter.WithLabelValues("mongo_iterator.next.find", err.Error()).Inc()
		return nil, err
	}
	defer i.cursor.Close(readCtx)
	i.batch = []bson.M{}

	if err := i.cursor.All(readCtx, &i.batch); err != nil {
		i.logger.Error("mongo.cursor.all.error",
			"error", err,
			"_id", cursor["lastID"],
		)
		metrics.ErrorCounter.WithLabelValues("mongo_iterator.next.cursor_all", err.Error()).Inc()
		return nil, err
	}
	elapsed := time.Since(start)
	metrics.ReadDuration.Observe(elapsed.Seconds())
	metrics.TotalReadOperations.Add(1)
	i.hasNext = i.batchSize == int64(len(i.batch))
	convertedBatch := i.ConvertedBatch()
	return &convertedBatch, nil
}

func (i *Iterator) ConvertedBatch() dto.Collection {
	var convertedBatch dto.RawCollection
	for _, doc := range i.batch {
		convertedBatch = append(convertedBatch, doc)
	}
	collection := dto.Collection{RawCollection: convertedBatch.Raw(), Cursor: map[string]interface{}{"lastID": convertedBatch.LastItemID()}}
	return collection
}

func (i *Iterator) HasNext(ctx context.Context) bool {
	return i.hasNext
}
