package elastic

import (
	"bytes"
	"encoding/json"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func PrepareBulkBody(docs []bson.M, indexName string) ([]byte, error) {
	var buf bytes.Buffer

	for _, doc := range docs {
		id := ""
		if oid, ok := doc["_id"].(primitive.ObjectID); ok {
			id = oid.Hex()
			doc["_id"] = id
		} else if strID, ok := doc["_id"].(string); ok {
			id = strID
		} else {
			log.Printf("WARNING: document missing _id or unknown type, skipping: %#v", doc)
			continue
		}

		// Action line
		meta := map[string]map[string]string{
			"index": {"_index": indexName, "_id": id},
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		// Document line
		docJSON, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		buf.Write(docJSON)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}
