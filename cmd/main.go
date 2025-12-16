package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	mongoiterator "github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service"
	"github.com/arashlml/mongo-reader/state"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

var k = koanf.New(".")

func main() {
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
	if err := k.Load(file.Provider("config/config.yaml"), yaml.Parser()); err != nil {
		logger.Error("error loading config: ", err)
	}
	// joda kardan config mongo az elastic hamash dakhel root config
	mongoUri := k.String("mongoUri")
	dbName := k.String("dbName")
	collectionName := k.String("collectionName")
	elasticUri := k.String("elasticUri")
	elasticUsername := k.String("elasticUsername")
	elasticPassword := k.String("elasticPassword")
	elasticIndex := k.String("elasticIndex")
	filePath := k.String("filePath")
	readFromFile := k.Bool("readFromFile")
	attempts := k.Int64("attempts")
	batchSize := k.Int64("batchSize")
	bufferSize := k.Int64("bufferSize")

	st := state.NewState(attempts, elasticIndex, logger, filePath, readFromFile)
	MongoConnector := mongorepository.NewMongoConnector(mongoUri, dbName, collectionName, logger, st)
	client, err := MongoConnector.Connect()
	if err != nil {
		logger.Error("main.connecting.mongo.server.failed",
			"error", err,
		)
	}
	col := MongoConnector.MakeMongoCollection(client)
	it := mongoiterator.NewMongoIterator(col, batchSize, logger, st)
	elasticConnector := elasticrepository.NewElasticConnector(elasticUri, elasticUsername, elasticPassword, logger)
	elasticClient, err := elasticConnector.Connect()
	if err != nil {
		logger.Error("main.connecting.elastic.server.failed",
			"error", err,
		)
	}
	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, elasticIndex, logger, st)
	bp := BackPressure.NewBackPressure[[]map[string]interface{}](bufferSize, logger)
	ctx, cancel := context.WithCancel(context.Background())
	go st.ProgressWithCancel(ctx)
	service := syncservice.NewService(it, elasticRepo, bp, logger, st)
	service.Wait()
	time.Sleep(3 * time.Second)
	cancel()
	logger.Info("migration is finished")

}
