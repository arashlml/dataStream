package mongorepository

import (
	"context"

	"github.com/arashlml/mongo-reader/state"
	"go.mongodb.org/mongo-driver/bson"

	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Uri        string `koanf:"uri"`
	Username   string `koanf:"username"`
	Password   string `koanf:"password"`
	Db         string `koanf:"db"`
	Collection string `koanf:"collection"`
}
type Connector struct {
	uri            string
	dbName         string
	collectionName string
	logger         *slog.Logger
	state          *state.State
}

func NewConnector(uri string, dbName string, collectionName string, logger *slog.Logger, state *state.State) *Connector {
	return &Connector{
		uri:            uri,
		dbName:         dbName,
		collectionName: collectionName,
		logger:         logger,
		state:          state,
	}
}

func (m *Connector) Connect() (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(m.uri))
	if err != nil {
		m.logger.Error("mongo.connect.connecting.to.server.error",
			"error", err)
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		m.logger.Error("mongo.connect.pinging.server.error",
			"error", err)
		return nil, err
	}
	m.logger.Info("mongo.connect.connecting.server.success")
	return client, nil
}

func (m *Connector) MakeCollection(client *mongo.Client) *mongo.Collection {
	col := client.Database(m.dbName).Collection(m.collectionName)
	filter := bson.M{}
	if !m.state.LastID.IsZero() {
		filter["_id"] = bson.M{"$gt": m.state.LastID}
	}
	count, err := col.CountDocuments(context.Background(), filter)
	if err != nil {
		m.logger.Error("mongo.connector.count-collection.error")
	}
	m.state.SetTotalDocuments(count)
	m.logger.Info("mongo.connector.connecting.collection.success")
	return col
}
