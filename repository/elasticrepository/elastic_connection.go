package elasticrepository

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticConnector struct {
	uri      string
	username string
	password string
	logger   *slog.Logger
}

func NewElasticConnector(uri string, username string, password string, logger *slog.Logger) *ElasticConnector {
	return &ElasticConnector{uri: uri, username: username, password: password, logger: logger}
}

func (e *ElasticConnector) Connect() (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{e.uri},
		Username:  e.username,
		Password:  e.password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		e.logger.Error("Elastic.connector.new.server.error",
			"error", err)
		return nil, err
	}
	res, err := es.Ping()
	if err != nil {
		e.logger.Error("Elastic.connector.pinging.server.error.1",
			"error", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		e.logger.Error("Elastic.connector.pinging.server.error",
			"error", res.String(),
		)
		return nil, fmt.Errorf("ping error: %s", res.Status())
	}
	e.logger.Info("Elastic.connector.connecting.server.success")
	return es, nil
}
