package mongo_repository

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/arashlml/data-stream/dto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Upsertor struct {
	col    *mongo.Collection
	logger *slog.Logger
}

func NewUpsertor(logger *slog.Logger, collection *mongo.Collection) *Upsertor {
	return &Upsertor{col: collection, logger: logger}
}

func (u *Upsertor) BulkUpsert(ctx context.Context, batch *dto.RawCollection) error {
	models := make([]mongo.WriteModel, 0, batch.Len())

	for _, doc := range batch.Raw() {
		id, ok := doc["_id"]
		if !ok {
			u.logger.Error("repository.mongo.bulk.upsert.no.id.found.in.document")
			return fmt.Errorf("document missing 'id' field: %v", doc)
		}

		filter := bson.M{"_id": id}
		update := bson.M{"$set": doc}

		model := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		models = append(models, model)
	}

	if len(models) == 0 {
		u.logger.Error("repository.mongo.bulk.upsert.no.write.models")
		return nil
	}

	opts := options.BulkWrite().SetOrdered(false)
	_, err := u.col.BulkWrite(ctx, models, opts)

	if err != nil {
		u.logger.Error("repository.mongo.upsert.bulk.write.error", "error", err)
		return fmt.Errorf("bulk upsert failed: %w", err)
	}

	return nil
}
