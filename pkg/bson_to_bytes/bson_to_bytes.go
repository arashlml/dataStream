package bson_to_bytes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Convertor struct {
	index  string
	logger *slog.Logger
	lastID primitive.ObjectID
}

func NewConvertor(index string, logger *slog.Logger) *Convertor {
	return &Convertor{
		index:  index,
		logger: logger,
	}
}

func (c *Convertor) ConvertToBytes(batch []bson.M) (bytes.Buffer, string, error) {
	var buf bytes.Buffer

	if len(batch) == 0 {
		c.logger.Warn(
			"bson_to_bytes.bulk.skipped",
			"reason", "empty batch",
		)
		return buf, "", errors.New("empty batch")
	}
	if len(batch) > 0 {
		doc := batch[len(batch)-1]
		if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
			c.logger.Warn(
				"bson_to_bytes.invalid_id",
				"_id", lastID,
				"id_type", fmt.Sprintf("%T", lastID),
			)
		} else {
			c.lastID = lastID
		}
	}

	for _, doc := range batch {

		id, ok := doc["_id"]
		if !ok {
			c.logger.Error(
				"bson_to_bytes.bulk.document.missing_id",
			)
			return buf, "", fmt.Errorf("_id not found in document")
		}

		delete(doc, "_id")

		meta := map[string]map[string]interface{}{
			"index": {
				"_index": c.index,
				"_id":    id,
			},
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			c.logger.Error(
				"bson_to_bytes.bulk.meta.marshal.failed",
				"error", err,
				"_id", c.lastID,
			)
			return buf, "", err
		}

		docBytes, err := json.Marshal(doc)
		if err != nil {
			c.logger.Error(
				"bson_to_bytes.bulk.doc.marshal.failed",
				"error", err,
				"_id", c.lastID,
			)
			return buf, "", err
		}

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}
	return buf, c.lastID.String(), nil
}
