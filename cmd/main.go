package main

// import (
//
//	"context"
//	"log"
//	"log/slog"
//	"os"
//
//	"github.com/arashlml/data-stream/config"
//	"github.com/arashlml/data-stream/metrics"
//	"github.com/arashlml/data-stream/repository/elastic_repository"
//	"github.com/arashlml/data-stream/repository/mongo_repository"
//	mongoiterator "github.com/arashlml/data-stream/repository/mongo_repository"
//	"github.com/arashlml/data-stream/service/reader_service"
//	syncservice "github.com/arashlml/data-stream/service/sync_service"
//	"github.com/arashlml/data-stream/service/writer_service"
//	"github.com/arashlml/data-stream/storage"
//	"github.com/knadh/koanf/parsers/yaml"
//	"github.com/knadh/koanf/providers/file"
//	"github.com/knadh/koanf/v2"
//
// )
//var K = koanf.New(".")

//
//func main() {
//	metrics.StartMetricsServer("9091")
//	appMetrics := metrics.New("myapp", "pipeline")
//
//	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: false,
//		Level: slog.LevelDebug,
//	})
//	logger := slog.New(logHandler)
//	if err := K.Load(file.Provider("config/config.yaml"), yaml.Parser()); err != nil {
//		logger.Error("error loading config: ", err)
//		appMetrics.ErrorCounter.WithLabelValues("main.load_config", err.Error()).Inc()
//	}
//
//	var cfg config.Config
//	if err := K.Unmarshal("", &cfg); err != nil {
//		logger.Error("error unmarshalling config: ", err)
//		appMetrics.ErrorCounter.WithLabelValues("main.unmarshal_config", err.Error()).Inc()
//	}
//
//	if err := cfg.ValidateConfig(); err != nil {
//		log.Fatalf("Configuration error: %v", err)
//	}
//
//	store := storage.NewStorage(logger, appMetrics, cfg.Storage)
//
//	MongoConnector := mongo_repository.NewConnector(logger, cfg.Mongo)
//	col, err := MongoConnector.ConnectAndMakeCollection(context.Background())
//	if err != nil {
//		logger.Error("main.connecting.mongo.server.failed",
//			"error", err,
//		)
//		appMetrics.ErrorCounter.WithLabelValues("main.connect_mongo", err.Error()).Inc()
//	}
//
//	it := mongoiterator.NewIterator(col, logger, appMetrics, cfg.Mongo)
//
//	elasticConnector := elastic_repository.NewConnector(logger, appMetrics, cfg.Elastic)
//
//	elasticClient, err := elasticConnector.Connect(context.Background())
//	if err != nil {
//		logger.Error(
//			"main.connecting.elastic.server.failed",
//			"error", err,
//		)
//		appMetrics.ErrorCounter.WithLabelValues("main.connect_elastic", err.Error()).Inc()
//	}
//
//	elasticRepo := elastic_repository.NewElasticRepository(elasticClient, logger, cfg.Elastic.Index, cfg.Elastic.InsertTimeout, cfg.Elastic.RetryAttempts, cfg.Elastic.RetryInterval, appMetrics)
//
//	readService := reader_service.New(store, it, appMetrics, logger, cfg.ReadService)
//
//	writeService := writer_service.New(store, elasticRepo, appMetrics, logger)
//
//	service := syncservice.NewSyncService(readService, writeService, logger, appMetrics, cfg.SyncService)
//	service.Start()
//	service.Wait()
//	logger.Info("migration is finished")
//	logger.Info("shutting down the application...")
//}
