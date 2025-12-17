package elasticrepository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/mongo-reader/state"
	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticRepository struct {
	client *elasticsearch.Client
	logger *slog.Logger
	state  *state.State
}

func NewElasticRepository(client *elasticsearch.Client, index string, logger *slog.Logger, state *state.State) *ElasticRepository {
	r := &ElasticRepository{
		client: client,
		logger: logger,
		state:  state,
	}

	r.logger.Info(
		"elastic.repository.initialized",
		"index", index,
	)

	return r
}
func (e *ElasticRepository) lastIDFinder(batch []map[string]interface{}) string {
	if len(batch) == 0 {
		return "no valid last_id"
	}
	if lastID, ok := batch[len(batch)-1]["_id"].(string); ok {
		return lastID
	} else {
		return "no valid last_id"
	}
}
func (e *ElasticRepository) Convertor(batch []map[string]interface{}) (bytes.Buffer, string, error) {
	var buf bytes.Buffer
	newDoc := map[string]interface{}{}
	lastID := e.lastIDFinder(batch)
	if len(batch) == 0 {
		e.logger.Warn(
			"elastic.repository.convertor.bulk.skipped",
			"reason", "empty batch",
		)
		return buf, lastID, errors.New("empty batch")
	}

	for _, doc := range batch {
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
				"_index": e.state.Index,
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

func (e *ElasticRepository) BulkInsert(ctx context.Context, batch []map[string]interface{}) error {
	buf, lastID, err := e.Convertor(batch)
	if err != nil {
		e.logger.Error("elastic.repository.bulkInsert.error",
			"error", err,
		)
	}
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
	atomic.AddInt64(&e.state.TotalWrittenDocuments, int64(len(batch)))
	e.state.SetLastInsertedID(lastID)
	return nil
}
