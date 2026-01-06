package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/arashlml/data-stream/config"
	"github.com/arashlml/data-stream/factory/iterator_factory"
	"github.com/arashlml/data-stream/factory/repository_factory"
	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/service/reader_service"
	syncservice "github.com/arashlml/data-stream/service/sync_service"
	"github.com/arashlml/data-stream/service/writer_service"
	"github.com/arashlml/data-stream/storage"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

var K = koanf.New(".")

func main() {
	metrics.StartMetricsServer("9091")

	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
		Level: slog.LevelDebug,
	})
	logger := slog.New(logHandler)
	if err := K.Load(file.Provider("config/config.yaml"), yaml.Parser()); err != nil {
		logger.Error("error loading config: ", err)
		metrics.ErrorCounter.WithLabelValues("main.load_config", err.Error()).Inc()
	}

	var cfg config.Config
	if err := K.Unmarshal("", &cfg); err != nil {
		logger.Error("error unmarshalling config: ", err)
		metrics.ErrorCounter.WithLabelValues("main.unmarshal_config", err.Error()).Inc()
	}

	if err := cfg.ValidateConfig(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	store := storage.NewStorage(logger, cfg.Storage)

	it, err := iterator_factory.Newfactory(logger, cfg.FactoryIterator).NewIterator(cfg.FactoryIterator)
	if err != nil {
		logger.Error("error creating iterator factory: ", err)
		return
	}
	repo, err := repository_factory.NewRepoFactory(logger, cfg.FactoryRepository).NewRepository(cfg.FactoryRepository)
	if err != nil {
		logger.Error("error creating repository factory: ", err)
		return
	}
	readService := reader_service.New(store, it, logger, cfg.ReadService)

	writeService := writer_service.New(store, logger, repo)

	service := syncservice.NewSyncService(readService, writeService, logger, cfg.SyncService)
	service.Start()
	service.Wait()
	logger.Info("migration is finished")
	logger.Info("shutting down the application...")
}
