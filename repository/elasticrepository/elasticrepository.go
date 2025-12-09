package elasticrepository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"go.mongodb.org/mongo-driver/bson"
)

type ElasticRepository struct {
	client  *elasticsearch.Client
	index   string
	counter int64
}

func NewElasticRepository(client *elasticsearch.Client, index string) *ElasticRepository {
	return &ElasticRepository{client: client, index: index}
}

func (e *ElasticRepository) BulkInsert(ctx context.Context, batch []bson.M) error {
	var buf bytes.Buffer

	for _, doc := range batch {

		id, ok := doc["_id"]
		if !ok {
			return fmt.Errorf("_id not found in document")
		}

		delete(doc, "_id")

		meta := map[string]map[string]interface{}{
			"index": {
				"_index": e.index,
				"_id":    id,
			},
		}

		metaBytes, _ := json.Marshal(meta)
		docBytes, _ := json.Marshal(doc)

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}
	res, err := e.client.Bulk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Printf("REPOSITORY: BULK INSERT ERROR --> %v \n", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Error bulk insert: %s", res.String())
	}
	var result map[string]interface{}
	json.NewDecoder(res.Body).Decode(&result)

	if result["errors"].(bool) {
		log.Println("❌ BULK PARTIAL FAILURE:", result)
		return fmt.Errorf("bulk insert had errors")
	}

	log.Println("✅ BULK INSERT SUCCESS")
	return nil
}
