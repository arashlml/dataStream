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
)

func main() {
	mongoUri := "mongodb://localhost:27017"
	dbName := "mydb"
	collectionName := "users"
	elasticUri := "https://localhost:9200"
	elasticUsername := "elastic"
	elasticPassword := "8*jidDJpxKBs0=aQ*9CS"
	elasticIndex := "users"
	filePath := "output.csv"
	readFromFile := true
	attempts := int64(5)
	batchSize := int64(5000)
	bufferSize := int64(50)

	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
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
	bp := BackPressure.NewBackPressure[state.Batch](bufferSize, logger)
	ctx, cancel := context.WithCancel(context.Background())
	go st.ProgressWthCancel(ctx)
	service := syncservice.NewService(it, elasticRepo, bp, logger, st)
	service.Wait()
	service.Close()
	time.Sleep(3 * time.Second)
	cancel()
	logger.Info("migration is finished")

}
