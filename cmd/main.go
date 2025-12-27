package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/arashlml/mongo-reader/config"
	"github.com/arashlml/mongo-reader/metrics"
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	mongoiterator "github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service/reader_service"
	syncservice "github.com/arashlml/mongo-reader/service/sync_service"
	"github.com/arashlml/mongo-reader/service/writer_service"
	"github.com/arashlml/mongo-reader/storage"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startMetricsServer(port string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics server starting on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Failed to start metrics.go server: %v", err)
		}
	}()
}

var k = koanf.New(".")

func main() {
	startMetricsServer("9091")
	appMetrics := metrics.New("myapp", "pipeline")

	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
	if err := k.Load(file.Provider("config/config.yaml"), yaml.Parser()); err != nil {
		logger.Error("error loading config: ", err)
	}

	var cfg config.Config
	if err := k.Unmarshal("", &cfg); err != nil {
		logger.Error("error unmarshalling config: ", err)
	}

	store := storage.NewStorage(logger, cfg.Storage.FilePath)

	MongoConnector := mongorepository.NewConnector(cfg.Mongo.Uri, cfg.Mongo.Username, cfg.Mongo.Password, cfg.Mongo.Db, cfg.Mongo.Collection, cfg.Mongo.Attempts, logger, cfg.Mongo.PingTimeout, cfg.Mongo.CountDocQueryTimeout, cfg.Mongo.ConnectTimeout)
	col, err := MongoConnector.ConnectAndMakeCollection(context.Background())
	if err != nil {
		logger.Error("main.connecting.mongo.server.failed",
			"error", err,
		)
	}

	it := mongoiterator.NewIterator(col, cfg.Mongo.BatchSize, logger, cfg.Mongo.IDType, cfg.Mongo.ReadTimeout, appMetrics)

	elasticConnector := elasticrepository.NewConnector(cfg.Elastic.Uri, cfg.Elastic.Username, cfg.Elastic.Password, logger, cfg.Elastic.PingTimeout)

	elasticClient, err := elasticConnector.Connect(context.Background())
	if err != nil {
		logger.Error(
			"main.connecting.elastic.server.failed",
			"error", err,
		)
	}

	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, logger, cfg.Elastic.Index, cfg.Elastic.InsertTimeout, cfg.Elastic.RetryAttempts, cfg.Elastic.RetryInterval, appMetrics)

	readService := reader_service.New(store, it, appMetrics, logger)

	writeService := writer_service.New(store, elasticRepo, appMetrics, logger)

	service := syncservice.NewSyncService(readService, writeService, logger, cfg.Service.BufferSize, appMetrics)
	service.Start()
	service.Wait()
	logger.Info("migration is finished")
	logger.Info("shutting down the application...")
}
