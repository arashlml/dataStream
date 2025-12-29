package elastic_repository

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/arashlml/data-stream/metrics"
	"github.com/elastic/go-elasticsearch/v8"
)

type Config struct {
	Uri           string        `koanf:"uri" validate:"required,uri"`
	Username      string        `koanf:"username"`
	Password      string        `koanf:"password"`
	Index         string        `koanf:"index" validate:"required"`
	RetryAttempts int           `koanf:"retryAttempts" validate:"gte=0"`
	PingTimeout   time.Duration `koanf:"pingTimeout" validate:"gte=0"`
	InsertTimeout time.Duration `koanf:"insertTimeout" validate:"gte=0"`
	RetryInterval float64       `koanf:"retryInterval" validate:"gte=0"`
}

type Connector struct {
	uri         string
	username    string
	password    string
	attempts    int
	index       string
	logger      *slog.Logger
	pingTimeout time.Duration
	metrics     *metrics.Metrics
}

func NewConnector(logger *slog.Logger, metrics *metrics.Metrics, config Config) *Connector {
	return &Connector{uri: config.Uri, username: config.Username, password: config.Password, index: config.Index, logger: logger, pingTimeout: config.PingTimeout, metrics: metrics}
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
