package elasticrepository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/arashlml/mongo-reader/entity"
	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticRepository struct {
	client *elasticsearch.Client
	logger *slog.Logger
	index  string
}

func NewElasticRepository(client *elasticsearch.Client, logger *slog.Logger, index string) *ElasticRepository {
	r := &ElasticRepository{
		client: client,
		logger: logger,
		index:  index,
	}

	r.logger.Info(
		"elastic.repository.initialized",
		"index", index,
	)

	return r
}

func (e *ElasticRepository) Convertor(ctx context.Context, batch *entity.RawCollection) (bytes.Buffer, string, error) {
	var buf bytes.Buffer
	newDoc := map[string]interface{}{}
	lastID := batch.LastItemID()
	if batch.Len() == 0 {
		e.logger.Warn(
			"elastic.repository.convertor.bulk.skipped",
			"reason", "empty batch",
		)
		return buf, lastID, errors.New("empty batch")
	}

	for _, doc := range batch.Raw() {
		for key, value := range doc {
			newDoc[key] = value
		}
		id, ok := doc["_id"]
		if !ok {
			e.logger.Error(
				"elastic.repository.convertor.document.missing_id",
			)
			return buf, lastID, errors.New("ID type assertion failed")
		}

		delete(newDoc, "_id")

		meta := map[string]map[string]interface{}{
			"index": {
				"_index": e.index,
				"_id":    id,
			},
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			e.logger.Error(
				"elastic.repository.convertor.meta.marshal.failed",
				"error", err,
				"_id", lastID,
			)
			return buf, lastID, errors.New("marshal meta failed")
		}

		docBytes, err := json.Marshal(newDoc)
		if err != nil {
			e.logger.Error(
				"elastic.repository.convertor.doc.marshal.failed",
				"error", err,
				"_id", lastID,
			)
			return buf, lastID, err
		}

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}
	return buf, lastID, nil
}

func (e *ElasticRepository) BulkInsert(ctx context.Context, batch *entity.RawCollection) error {
	buf, lastID, err := e.Convertor(ctx, batch)
	if err != nil {
		e.logger.Error("elastic.repository.bulkInsert.error",
			"error", err,
		)
	}
	res, err := e.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		e.client.Bulk.WithContext(ctx),
	)
	e.client.Bulk.WithContext(ctx)
	if err != nil {
		e.logger.Error(
			"elastic.repository.bulk.request.failed",
			"error", err,
			"_id", lastID,
		)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		e.logger.Error(
			"elastic.repository.bulk.response.error",
			"status", res.Status(),
			"_id", lastID,
		)
		return fmt.Errorf("elastic bulk error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		e.logger.Error(
			"elastic.repository.bulk.decode.failed",
			"error", err,
			"_id", lastID,
		)
		return err
	}
	if success, ok := result["errors"].(bool); !ok {
		e.logger.Error("elastic.repository.bulk.result['errors'].type.assertion.failed",
			"_id", lastID,
		)
	} else {
		if success {
			e.logger.Error("elastic.repository.bulk.result['errors']",
				"_id", lastID,
			)
			return errors.New("elastic.repository.bulk.result['errors']")
		}

	}
	return nil
}
