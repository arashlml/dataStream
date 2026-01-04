package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/arashlml/data-stream/config"
	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/repository/mongo_repository"
	"github.com/arashlml/data-stream/repository/typesense_file_repository"
	"github.com/arashlml/data-stream/service/reader_service"
	"github.com/arashlml/data-stream/service/sync_service"
	"github.com/arashlml/data-stream/service/writer_service"
	"github.com/arashlml/data-stream/storage"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

var K = koanf.New(".")

func main() {
	appMetrics := metrics.New("myapp", "pipeline")
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
	if err := K.Load(file.Provider("config/config.yaml"), yaml.Parser()); err != nil {
		logger.Error("error loading config: ", err)
	}
	var cfg config.Config
	if err := K.Unmarshal("", &cfg); err != nil {
		logger.Error("error unmarshalling config: ", err)
	}
	typesenseRepo := typesense_file_repository.New(logger, cfg.Typesense)
	store := storage.NewStorage(logger, appMetrics, cfg.Storage)
	mongoConnector := mongo_repository.NewConnector(logger, cfg.Mongo)
	col, err := mongoConnector.ConnectAndMakeCollection(context.Background())
	if err != nil {
		logger.Error("main.creating.collection.failed", "error", err)
	}
	mongoRepo := mongo_repository.NewUpsertor(logger, col)
	readerService := reader_service.New(store, typesenseRepo, appMetrics, logger, cfg.ReadService)
	writerService := writer_service.New(store, mongoRepo, appMetrics, logger)
	syncService := sync_service.NewSyncService(readerService, writerService, logger, appMetrics, cfg.SyncService)
	syncService.Start()
	syncService.Wait()
	logger.Info("migration.finished")
}
