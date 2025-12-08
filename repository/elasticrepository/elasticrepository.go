package elasticrepository

import (
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

func connect(uri, username, password string) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{uri},
		Username:  username,
		Password:  password,
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Printf("Error creating the client: %s", err)
		return nil, err
	}
	res, err := es.Ping()
	if err != nil {
		log.Printf("Error pinging Elasticsearch: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Elasticsearch returned error status: %s", res.Status())
		return nil, fmt.Errorf("ping error: %s", res.Status())
	}

	log.Println("Connected to Elasticsearch successfully")
	return es, nil
}
