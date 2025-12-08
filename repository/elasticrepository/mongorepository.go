package elasticrepository

import (
	"bytes"
	"context"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticRepository struct {
	client *elasticsearch.Client
	index  string
}

func NewElasticRepository(client *elasticsearch.Client, index string) *ElasticRepository {
	return &ElasticRepository{client: client, index: index}
}

func (r *ElasticRepository) BulkInsert(ctx context.Context, body []byte) error {
	res, err := r.client.Bulk(bytes.NewReader(body), r.client.Bulk.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Bulk insert error: %s", res.Status())
	}

	return nil
}
