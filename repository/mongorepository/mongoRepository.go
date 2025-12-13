package mongorepository

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoConnector struct {
	uri            string
	dbName         string
	collectionName string
	logger         *slog.Logger
}

func NewMongoConnector(uri string, dbName string, collectionName string, logger *slog.Logger) *MongoConnector {
	return &MongoConnector{
		uri:            uri,
		dbName:         dbName,
		collectionName: collectionName,
		logger:         logger,
	}
}

func (m *MongoConnector) Connect() (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(m.uri))
	if err != nil {
		m.logger.Error("mongo.connector.connecting.to.server.error",
			"error", err)
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		m.logger.Error("mongo.connector.pinging.server.error",
			"error", err)
		return nil, err
	}
	m.logger.Info("mongo.connector.connecting.server.success")
	return client, nil
}

func (m *MongoConnector) MakeMongoCollection(client *mongo.Client) *mongo.Collection {
	col := client.Database(m.dbName).Collection(m.collectionName)
	m.logger.Info("mongo.connector.connecting.collection.success")
	return col
}
