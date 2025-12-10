package elasticrepository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ElasticRepository struct {
	client  *elasticsearch.Client
	index   string
	counter int64
	logger  *slog.Logger
	lastID  primitive.ObjectID
}

func NewElasticRepository(client *elasticsearch.Client, index string, logger *slog.Logger) *ElasticRepository {
	r := &ElasticRepository{
		client: client,
		index:  index,
		logger: logger,
	}

	r.logger.Info(
		"elastic.repository.initialized",
		"index", index,
	)

	return r
}

func (e *ElasticRepository) BulkInsert(ctx context.Context, batch []bson.M) error {
	var buf bytes.Buffer

	if len(batch) == 0 {
		e.logger.Warn(
			"elastic.bulk.skipped",
			"reason", "empty batch",
		)
		return nil
	}
	if len(batch) > 0 {
		doc := batch[len(batch)-1]
		if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
			e.logger.Warn(
				"elasticrepository.invalid_id",
				"_id", lastID,
				"id_type", fmt.Sprintf("%T", lastID),
			)
		} else {
			e.lastID = lastID
		}
	}

	for _, doc := range batch {

		id, ok := doc["_id"]
		if !ok {
			e.logger.Error(
				"elastic.bulk.document.missing_id",
			)
			return fmt.Errorf("_id not found in document")
		}

		delete(doc, "_id")

		meta := map[string]map[string]interface{}{
			"index": {
				"_index": e.index,
				"_id":    id,
			},
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			e.logger.Error(
				"elastic.bulk.meta.marshal.failed",
				"error", err,
				"_id", e.lastID)
			return err
		}

		docBytes, err := json.Marshal(doc)
		if err != nil {
			e.logger.Error(
				"elastic.bulk.doc.marshal.failed",
				"error", err,
				"_id", e.lastID,
			)
			return err
		}

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	res, err := e.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		e.client.Bulk.WithContext(ctx),
	)
	if err != nil {
		e.logger.Error(
			"elastic.bulk.request.failed",
			"error", err,
			"batch_size", len(batch),
			"_id", e.lastID,
		)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		e.logger.Error(
			"elastic.bulk.response.error",
			"status", res.Status(),
			"_id", e.lastID,
		)
		return fmt.Errorf("elastic bulk error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		e.logger.Error(
			"elastic.bulk.decode.failed",
			"error", err,
			"_id", e.lastID,
		)
		return err
	}

	if result["errors"].(bool) {
		e.logger.Warn(
			"elastic.bulk.partial.failure",
			"index", e.index,
			"batch_size", len(batch),
			"response", result,
			"_id", e.lastID,
		)
		return fmt.Errorf("bulk insert had partial errors")
	}

	total := atomic.AddInt64(&e.counter, int64(len(batch)))

	e.logger.Info(
		"elastic.bulk.success",
		"index", e.index,
		"batch_size", len(batch),
		"_id", e.lastID,
		"total_inserted", total,
	)

	return nil
}
