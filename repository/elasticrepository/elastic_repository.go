package elasticrepository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/arashlml/mongo-reader/dto"
	"github.com/arashlml/mongo-reader/metrics"
	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticRepository struct {
	client        *elasticsearch.Client
	logger        *slog.Logger
	index         string
	insertTimeOut time.Duration
	retryAttempts int
	retryInterval float64
	metrics       *metrics.Metrics
}

func NewElasticRepository(client *elasticsearch.Client, logger *slog.Logger, index string, insertTimeOut time.Duration, retryAttempts int, retryInterval float64, metrics *metrics.Metrics) *ElasticRepository {
	r := &ElasticRepository{
		client:        client,
		logger:        logger,
		index:         index,
		insertTimeOut: insertTimeOut,
		retryAttempts: retryAttempts,
		retryInterval: retryInterval,
		metrics:       metrics,
	}

	r.logger.Info(
		"elastic.repository.initialized",
		"index", index,
	)

	return r
}

func (e *ElasticRepository) Convertor(ctx context.Context, batch *dto.RawCollection) (bytes.Buffer, string, error) {
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

func (e *ElasticRepository) BulkInsert(ctx context.Context, batch *dto.RawCollection) error {
	buf, lastID, err := e.Convertor(ctx, batch)
	if err != nil {
		e.logger.Error("elastic.repository.bulkInsert.error",
			"error", err,
		)
	}
	insertCtx, _ := context.WithTimeout(ctx, e.insertTimeOut*time.Second)
	res, err := e.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		e.client.Bulk.WithContext(insertCtx),
	)
	if insertCtx.Err() != nil {
		e.logger.Error(
			"elastic.repository.bulk.request.failed",
			"error", insertCtx.Err(),
			"_id", lastID,
		)
		e.retry(ctx, buf, lastID)
		return insertCtx.Err()
	}
	if err != nil {
		e.logger.Error(
			"elastic.repository.bulk.request.failed",
			"error", err,
			"_id", lastID,
		)
		e.retry(ctx, buf, lastID)
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
func (e *ElasticRepository) retry(ctx context.Context, buf bytes.Buffer, lastID string) {
	for attempt := 0; attempt <= e.retryAttempts; attempt++ {
		retryCtx, _ := context.WithTimeout(ctx, e.insertTimeOut*time.Second)
		res, err := e.client.Bulk(
			bytes.NewReader(buf.Bytes()),
			e.client.Bulk.WithContext(retryCtx),
		)
		if err == nil && !res.IsError() {
			e.logger.Info("elastic.repository.bulk.request.retry.successes",
				"attempt", attempt,
				"_id", lastID)
			return
		}
		if attempt < e.retryAttempts {
			<-time.After(time.Duration(float64(attempt)*e.retryInterval) * time.Second)
			e.logger.Error("elastic.repository.bulk.request.retry.error",
				"attempt", attempt,
				"error", err,
				"_id", lastID)
			continue
		}
		return
	}
}
