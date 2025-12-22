package mongorepository

import (
	"context"
	"log"

	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Uri                  string        `koanf:"uri"`
	Username             string        `koanf:"username"`
	Password             string        `koanf:"password"`
	Db                   string        `koanf:"db"`
	Collection           string        `koanf:"collection"`
	Attempts             int           `koanf:"attempts"`
	BatchSize            int64         `koanf:"batchSize"`
	PingTimeout          time.Duration `koanf:"pingTimeout"`
	CountDocQueryTimeout time.Duration `koanf:"countDocTimeout"`
	ConnectTimeout       time.Duration `koanf:"connectTimeout"`
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

func (m *Connector) Connect(ctx context.Context) (*mongo.Client, error) {
	connectCtx, cancel := context.WithTimeout(ctx, m.connectTimeout*time.Second)
	defer cancel()
	// .SetAuth(options.Credential{Username: m.username, Password: m.password}
	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(m.uri).SetAuth(options.Credential{Username: m.username, Password: m.password}))
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
	PingCtx, PingCancel := context.WithTimeout(ctx, m.pingTimeout*time.Second)
	defer PingCancel()
	if err := client.Ping(PingCtx, nil); err != nil {
		m.logger.Error("mongo.connect.pinging.server.error",
			"error", err)
		return nil, err
	}
	m.logger.Info("mongo.connect.connecting.server.success")
	return client, nil
}

func (m *Connector) MakeCollection(ctx context.Context, client *mongo.Client) *mongo.Collection {
	col := client.Database(m.dbName).Collection(m.collectionName)
	return col
}

func (m *Connector) CountDocuments(ctx context.Context, col *mongo.Collection, lastID string) (int64, error) {
	filter := bson.M{}
	if lastID != "" {
		lastID := lastID
		filter["_id"] = bson.M{"$gt": lastID}
	}
	ctx, cancel := context.WithTimeout(ctx, m.countDocQueryTimeout*time.Second)
	defer cancel()
	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		m.logger.Error("mongo.connector.count-document.error",
			"error", err,
		)
		return 0, err
	}
	m.logger.Info("mongo.connector.connecting.collection.success")
	return count, nil
}
func (m *Connector) retry(ctx context.Context) (*mongo.Client, error) {
	for i := 0; i < m.attempts; i++ {
		ctx, cancel := context.WithTimeout(ctx, m.connectTimeout*time.Second)
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(m.uri).SetAuth(options.Credential{Username: m.username, Password: m.password}))
		cancel()
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
