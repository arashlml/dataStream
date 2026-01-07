package elastic_repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticRepository struct {
	client        *elasticsearch.Client
	logger        *slog.Logger
	index         string
	insertTimeOut time.Duration
	retryAttempts int
	retryInterval float64
}

func NewElasticRepository(client *elasticsearch.Client, logger *slog.Logger, config *Config) *ElasticRepository {
	r := &ElasticRepository{
		client:        client,
		logger:        logger,
		index:         config.Index,
		insertTimeOut: config.InsertTimeout,
		retryAttempts: config.RetryAttempts,
		retryInterval: config.RetryInterval,
	}

	r.logger.Info(
		"elastic.repository.initialized",
		"index", config.Index,
	)

	return r
}

func (e *ElasticRepository) Convertor(ctx context.Context, batch *model.RawCollection) (bytes.Buffer, string, error) {
	var buf bytes.Buffer
	newDoc := map[string]interface{}{}
	lastID := batch.LastItemID()
	if batch.Len() == 0 {
		e.logger.Warn(
			"elastic.repository.convertor.bulk.skipped",
			"reason", "empty batch",
		)
		metrics.ErrorCounter.WithLabelValues("elastic_repository.convertor.empty_batch", "empty batch").Inc()
		return buf, lastID, errors.New("empty batch")
	}

	for _, doc := range batch.Raw() {
		for key, value := range doc {
			newDoc[key] = value
		}
		id, ok := doc["id"]
		if !ok {
			e.logger.Error(
				"elastic.repository.convertor.document.missing_id",
			)
			metrics.ErrorCounter.WithLabelValues("elastic_repository.convertor.missing_id", "document missing _id").Inc()
			return buf, lastID, errors.New("ID type assertion failed")
		}

		delete(newDoc, "id")

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
			metrics.ErrorCounter.WithLabelValues("elastic_repository.convertor.meta_marshal", err.Error()).Inc()
			return buf, lastID, errors.New("marshal meta failed")
		}

		docBytes, err := json.Marshal(newDoc)
		if err != nil {
			e.logger.Error(
				"elastic.repository.convertor.doc.marshal.failed",
				"error", err,
				"_id", lastID,
			)
			metrics.ErrorCounter.WithLabelValues("elastic_repository.convertor.doc_marshal", err.Error()).Add(1)
			return buf, lastID, err
		}

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}
	return buf, lastID, nil
}

func (e *ElasticRepository) BulkUpsert(ctx context.Context, batch *model.RawCollection) error {
	if len(batch.Raw()) == 0 {
		return errors.New("empty batch")
	}
	buf, lastID, err := e.Convertor(ctx, batch)
	if err != nil {
		e.logger.Error("elastic.repository.bulkInsert.error",
			"error", err,
		)
		metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.convertor_failed", err.Error()).Inc()
		return err
	}
	insertCtx, _ := context.WithTimeout(ctx, e.insertTimeOut*time.Second)
	start := time.Now()
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
		metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.timeout", insertCtx.Err().Error()).Inc()
		e.retry(ctx, buf, lastID)
		return insertCtx.Err()
	}

	if err != nil {
		e.logger.Error(
			"elastic.repository.bulk.request.failed",
			"error", err,
			"_id", lastID,
		)
		metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.bulk_failed", err.Error()).Inc()
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		e.logger.Error(
			"elastic.repository.bulk.response.error",
			"status", res.Status(),
			"_id", lastID,
		)
		metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.response_error", res.String()).Inc()
		return fmt.Errorf("elastic bulk error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		e.logger.Error(
			"elastic.repository.bulk.decode.failed",
			"error", err,
			"_id", lastID,
		)
		metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.decode_failed", err.Error()).Inc()
		return err
	}

	if success, ok := result["errors"].(bool); !ok {
		e.logger.Error("elastic.repository.bulk.result['errors'].type.assertion.failed",
			"_id", lastID,
		)
		metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.result_assertion_failed", "result['errors'] type assertion failed").Inc()
	} else {
		if success {
			e.logger.Error("elastic.repository.bulk.result['errors']",
				"_id", lastID,
			)
			metrics.ErrorCounter.WithLabelValues("elastic_repository.bulk_insert.result_has_errors", "bulk result contained errors").Inc()
			return errors.New("elastic.repository.bulk.result['errors']")
		}

	}
	elapsed := time.Since(start)
	metrics.WriteDuration.Observe(elapsed.Seconds())
	metrics.TotalWrittenOperations.Add(1)
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
		metrics.TotalFailedDocuments.Add(1)
		return
	}
}
