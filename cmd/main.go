package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/arashlml/data-stream/config"
	"github.com/arashlml/data-stream/factory/destination_factory"
	"github.com/arashlml/data-stream/factory/iterator_factory"
	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
	"github.com/arashlml/data-stream/service/reader_service"
	"github.com/arashlml/data-stream/service/sync_service"
	syncservice "github.com/arashlml/data-stream/service/sync_service"
	"github.com/arashlml/data-stream/service/writer_service"
	"github.com/arashlml/data-stream/storage"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

var K = koanf.New(".")

type repositories struct {
	it         model.Iterator
	repository model.WriteRepository
	store      *storage.FileStorage
}
type services struct {
	reader *reader_service.Service
	writer *writer_service.Service
	syncer *sync_service.Service
}

func main() {
	metrics.StartMetricsServer("9091")

	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
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
	repos := buildRepositories(logger, cfg)
	services := buildServices(logger, cfg, repos)
	services.syncer.Run()
	logger.Info("migration is finished")
	logger.Info("shutting down the application...")
}

// TODO : change the iterator and repository
func buildRepositories(logger *slog.Logger, cfg config.Config) *repositories {
	store := storage.NewStorage(logger, cfg.Storage)

	it, err := source_factory.Newfactory(logger, cfg.FactoryIterator, cfg.Mongo, cfg.TypesenseFile).NewIterator(cfg.FactoryIterator)
	if err != nil {
		logger.Error("error creating iterator factory: ", err)
		return nil
	}
	repo, err := destination_factory.NewRepoFactory(logger, cfg.FactoryRepository, cfg.Mongo, cfg.Elastic).NewRepository(cfg.FactoryRepository)
	if err != nil {
		logger.Error("error creating repository factory: ", err)
		return nil
	}
	return &repositories{
		it:         it,
		repository: repo,
		store:      store,
	}
}

func buildServices(logger *slog.Logger, cfg config.Config, repos *repositories) *services {
	readService := reader_service.New(repos.store, repos.it, logger, cfg.ReadService)

	writeService := writer_service.New(repos.store, logger, repos.repository)

	service := syncservice.NewSyncService(readService, writeService, logger, cfg.SyncService)
	return &services{
		reader: readService,
		writer: writeService,
		syncer: service,
	}
}
