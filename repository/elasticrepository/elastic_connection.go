package elasticrepository

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/arashlml/mongo-reader/metrics"
	"github.com/elastic/go-elasticsearch/v8"
)

type Config struct {
	Uri           string        `koanf:"uri"`
	Username      string        `koanf:"username"`
	Password      string        `koanf:"password"`
	Index         string        `koanf:"index"`
	RetryAttempts int           `koanf:"retryAttempts"`
	PingTimeout   time.Duration `koanf:"pingTimeout"`
	InsertTimeout time.Duration `koanf:"insertTimeout"`
	RetryInterval float64       `koanf:"retryInterval"`
}

type Connector struct {
	uri         string
	username    string
	password    string
	attempts    int
	logger      *slog.Logger
	pingTimeout time.Duration
	metrics     *metrics.Metrics
}

func NewConnector(uri string, username string, password string, logger *slog.Logger, pingTimeout time.Duration, metrics *metrics.Metrics) *Connector {
	return &Connector{uri: uri, username: username, password: password, logger: logger, pingTimeout: pingTimeout, metrics: metrics}
}

func (e *Connector) Connect(ctx context.Context) (*elasticsearch.Client, error) {
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
		e.logger.Error("Elastic.connector.new.Client.error",
			"error", err,
		)
		e.metrics.ErrorCounter.WithLabelValues("elastic_connector.connect.new_client", "", err.Error()).Inc()
		return nil, err
	}

	pingCtx, _ := context.WithTimeout(ctx, e.pingTimeout*time.Second)

	res, err := es.Ping(es.Ping.WithContext(pingCtx))

	if err != nil {
		e.logger.Error("Elastic.connector.pinging.server.error",
			"error", err,
		)
		e.metrics.ErrorCounter.WithLabelValues("elastic_connector.connect.ping_failed", "", err.Error()).Inc()
		return nil, err
	}

	defer res.Body.Close()

	if res.IsError() {
		e.logger.Error("Elastic.connector.pinging.server.error",
			"error", res.String(),
		)
		e.metrics.ErrorCounter.WithLabelValues("elastic_connector.connect.ping_response_error", "", res.String()).Inc()
		return nil, fmt.Errorf("ping error: %s", res.Status())
	}
	e.logger.Info("Elastic.connector.connecting.server.success")
	return es, nil
}
