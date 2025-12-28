package mongorepository

import (
	"context"
	"log"

	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Uri                  string        `koanf:"uri" validate:"required,uri"`
	Username             string        `koanf:"username"`
	Password             string        `koanf:"password"`
	Db                   string        `koanf:"db" validate:"required"`
	Collection           string        `koanf:"collection" validate:"required"`
	Attempts             int           `koanf:"attempts" validate:"gte=0"`
	BatchSize            int64         `koanf:"batchSize" validate:"gt=0"`
	PingTimeout          time.Duration `koanf:"pingTimeout" validate:"gte=0"`
	CountDocQueryTimeout time.Duration `koanf:"countDocTimeout" validate:"gte=0"`
	ConnectTimeout       time.Duration `koanf:"connectTimeout" validate:"gte=0"`
	ReadTimeout          time.Duration `koanf:"readTimeout" validate:"gte=0"`
	IDType               string        `koanf:"idType" validate:"required,oneof=ObjectID String"`
}
type Connector struct {
	uri                  string
	username             string
	password             string
	dbName               string
	collectionName       string
	attempts             int
	logger               *slog.Logger
	pingTimeout          time.Duration
	countDocQueryTimeout time.Duration
	connectTimeout       time.Duration
}

func NewConnector(uri, username, password, dbName, collectionName string, attempts int, logger *slog.Logger, pingTimeout time.Duration, countDocQueryTimeout time.Duration, connectTimeout time.Duration) *Connector {
	return &Connector{
		uri:                  uri,
		username:             username,
		password:             password,
		dbName:               dbName,
		collectionName:       collectionName,
		attempts:             attempts,
		logger:               logger,
		pingTimeout:          pingTimeout,
		countDocQueryTimeout: countDocQueryTimeout,
		connectTimeout:       connectTimeout,
	}
}

func (m *Connector) ConnectAndMakeCollection(ctx context.Context) (*mongo.Collection, error) {
	connectCtx, _ := context.WithTimeout(ctx, m.connectTimeout*time.Second)
	connOpts := options.Client().ApplyURI(m.uri)
	if m.username != "" || m.password != "" {
		connOpts = connOpts.SetAuth(options.Credential{Username: m.username, Password: m.password})
	}
	client, err := mongo.Connect(connectCtx, connOpts)
	if err != nil {
		m.logger.Error("mongo.connect.connecting.to.server.error",
			"error", err)
		client, err = m.retry(ctx)
		if err != nil {
			m.logger.Error("mongo.connect.connecting.to.server.error",
				"error", err,
			)
			return nil, err
		}
		return nil, err
	}
	PingCtx, _ := context.WithTimeout(ctx, m.pingTimeout*time.Second)
	if err := client.Ping(PingCtx, nil); err != nil {
		m.logger.Error("mongo.connect.pinging.server.error",
			"error", err)
		return nil, err
	}
	m.logger.Info("mongo.connect.connecting.server.success")
	col := m.MakeCollection(ctx, client)

	return col, nil
}

func (m *Connector) MakeCollection(ctx context.Context, client *mongo.Client) *mongo.Collection {
	col := client.Database(m.dbName).Collection(m.collectionName)
	return col
}

func (m *Connector) retry(ctx context.Context) (*mongo.Client, error) {
	for i := 0; i < m.attempts; i++ {
		connectCtx, _ := context.WithTimeout(ctx, m.connectTimeout*time.Second)
		client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(m.uri).SetAuth(options.Credential{Username: m.username, Password: m.password}))
		if err == nil {
			log.Println("mongo.connect.connecting.success")
			return client, nil
		}
		<-time.After(time.Duration(i*5) * time.Second)
		m.logger.Error("mongo.connect.error",
			"attempt", i,
			"error", err)
	}
	return nil, nil
}
