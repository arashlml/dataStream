package main

import (
	"log"
	"log/slog"
	"os"
	"time"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	mongoiterator "github.com/arashlml/mongo-reader/pkg/iterator/mongo_iterator"
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service"
	"go.mongodb.org/mongo-driver/bson"
)

// TODO : USING SLUG FOR LOGS | GEMINI, PLEASE SKIPP THIS
// TODO : MAKING THE CONFIG VALIDATOR | GEMINI, PLEASE SKIP THIS
func main() {
	mongoUri := "mongodb://localhost:27017"
	elasticUri := "https://localhost:9200"
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
	client, err := mongorepository.Connect(mongoUri)
	if err != nil {
		log.Fatalf("ERROR FROM MONGO CONNECT -> %v", err)
	}
	col := mongorepository.MakeMongoCollection(client, "mydb", "users")
	it := mongoiterator.NewMongoIterator(col, 10, logger)
	elasticClient, err := elasticrepository.Connect(elasticUri, "elastic", "HUarUJmrvXjpwU7d+ji7")
	if err != nil {
		log.Fatalf("ERROR FROM ELASTIC CONNECT -> %v", err)
	}
	//TODO: INDEX NAME
	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, "users", logger)
	bp := BackPressure.NewBackPressure[[]bson.M](100, logger)
	_ = syncservice.NewService(it, elasticRepo, bp, logger)
	time.Sleep(1 * time.Hour)
}
