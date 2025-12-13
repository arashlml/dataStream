package elasticrepository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticRepository struct {
	client       *elasticsearch.Client
	index        string
	totalCounter int64
	logger       *slog.Logger
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

func (e *ElasticRepository) BulkInsert(ctx context.Context, buf bytes.Buffer, lastID string, lengthOfBatch int64) error {
	res, err := e.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		e.client.Bulk.WithContext(ctx),
	)
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

	if result["errors"].(bool) {
		e.logger.Warn(
			"elastic.repository.bulk.partial.failure",
			"index", e.index,
			"response", result,
			"_id", lastID,
		)
		return fmt.Errorf("bulk insert had partial errors")
	}

	total := atomic.AddInt64(&e.totalCounter, lengthOfBatch)

	e.logger.Info(
		"elastic.repository.bulk.success",
		"index", e.index,
		"_id", lastID,
		"total_inserted", total,
	)

	return nil
}
