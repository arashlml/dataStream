package elasticrepository

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type Config struct {
	Uri         string        `koanf:"uri"`
	Username    string        `koanf:"username"`
	Password    string        `koanf:"password"`
	Index       string        `koanf:"index"`
	Attempts    int           `koanf:"attempts"`
	PingTimeout time.Duration `koanf:"pingTimeout"`
}

type Connector struct {
	uri         string
	username    string
	password    string
	attempts    int
	logger      *slog.Logger
	pingTimeout time.Duration
}

func NewConnector(uri string, username string, password string, attempts int, logger *slog.Logger, pingTimeout time.Duration) *Connector {
	return &Connector{uri: uri, username: username, password: password, attempts: attempts, logger: logger, pingTimeout: pingTimeout}
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
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, e.pingTimeout*time.Second)

	res, err := es.Ping(es.Ping.WithContext(ctx))
	cancel()

	if err != nil {
		e.logger.Error("Elastic.connector.pinging.server.error",
			"error", err,
		)
		cancel()
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
