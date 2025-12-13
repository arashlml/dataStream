package main

import (
	"log/slog"
	"os"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	"github.com/arashlml/mongo-reader/pkg/bson_to_bytes"
	mongoiterator "github.com/arashlml/mongo-reader/pkg/iterator/mongo_iterator"
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	mongoUri := "mongodb://localhost:27017"
	dbName := "mydb"
	collectionName := "users"
	elasticUri := "https://localhost:9200"
	elasticUsername := "elastic"
	elasticPassword := "HUarUJmrvXjpwU7d+ji7"
	elasticIndex := "users"

	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
	MongoConnector := mongorepository.NewMongoConnector(mongoUri, dbName, collectionName, logger)
	client, err := MongoConnector.Connect()
	if err != nil {
		logger.Error("main.connecting.mongo.server.failed",
			"error", err,
		)
	}
	col := MongoConnector.MakeMongoCollection(client)
	it := mongoiterator.NewMongoIterator(col, 5000, logger)
	elasticConnector := elasticrepository.NewElasticConnector(elasticUri, elasticUsername, elasticPassword, logger)
	elasticClient, err := elasticConnector.Connect()
	if err != nil {
		logger.Error("main.connecting.elastic.server.failed",
			"error", err,
		)
	}
	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, elasticIndex, logger)
	bp := BackPressure.NewBackPressure[[]bson.M](5000, logger)
	convertor := bson_to_bytes.NewConvertor(elasticIndex, logger)
	service := syncservice.NewService(it, elasticRepo, bp, logger, convertor)

	service.Wait()

	logger.Info("migration is finished")

}
