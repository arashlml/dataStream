package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"

	"fmt"

	"github.com/arashlml/data-stream/config"
	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/repository/elasticrepository"
	"github.com/arashlml/data-stream/repository/mongorepository"
	mongoiterator "github.com/arashlml/data-stream/repository/mongorepository"
	"github.com/arashlml/data-stream/service/reader_service"
	syncservice "github.com/arashlml/data-stream/service/sync_service"
	"github.com/arashlml/data-stream/service/writer_service"
	"github.com/arashlml/data-stream/storage"
	"github.com/go-playground/validator/v10"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func validateConfig(cfg config.Config) error {
	validate := validator.New()
	err := validate.Struct(cfg)
	if err != nil {
		var errorMsg string
		for _, err := range err.(validator.ValidationErrors) {
			errorMsg += fmt.Sprintf("Field validation for '%s' failed on the '%s' tag; ", err.StructNamespace(), err.Tag())
		}
		return fmt.Errorf("configuration validation failed: %s", errorMsg)
	}
	return nil
}

func startMetricsServer(port string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics server starting on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
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
		appMetrics.ErrorCounter.WithLabelValues("main.load_config", err.Error()).Inc()
	}

	var cfg config.Config
	if err := k.Unmarshal("", &cfg); err != nil {
		logger.Error("error unmarshalling config: ", err)
		appMetrics.ErrorCounter.WithLabelValues("main.unmarshal_config", err.Error()).Inc()
	}

	if err := validateConfig(cfg); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	store := storage.NewStorage(logger, cfg.Storage.FilePath, appMetrics)

	MongoConnector := mongorepository.NewConnector(cfg.Mongo.Uri, cfg.Mongo.Username, cfg.Mongo.Password, cfg.Mongo.Db, cfg.Mongo.Collection, cfg.Mongo.Attempts, logger, cfg.Mongo.PingTimeout, cfg.Mongo.CountDocQueryTimeout, cfg.Mongo.ConnectTimeout)
	col, err := MongoConnector.ConnectAndMakeCollection(context.Background())
	if err != nil {
		logger.Error("main.connecting.mongo.server.failed",
			"error", err,
		)
		appMetrics.ErrorCounter.WithLabelValues("main.connect_mongo", err.Error()).Inc()
	}

	it := mongoiterator.NewIterator(col, cfg.Mongo.BatchSize, logger, cfg.Mongo.IDType, cfg.Mongo.ReadTimeout, appMetrics)

	elasticConnector := elasticrepository.NewConnector(cfg.Elastic.Uri, cfg.Elastic.Username, cfg.Elastic.Password, cfg.Elastic.Index, logger, cfg.Elastic.PingTimeout, appMetrics)

	elasticClient, err := elasticConnector.Connect(context.Background())
	if err != nil {
		logger.Error(
			"main.connecting.elastic.server.failed",
			"error", err,
		)
		appMetrics.ErrorCounter.WithLabelValues("main.connect_elastic", err.Error()).Inc()
	}

	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, logger, cfg.Elastic.Index, cfg.Elastic.InsertTimeout, cfg.Elastic.RetryAttempts, cfg.Elastic.RetryInterval, appMetrics)

	readService := reader_service.New(store, it, appMetrics, logger, cfg.ReadService.ResumeCapability)

	writeService := writer_service.New(store, elasticRepo, appMetrics, logger)

	service := syncservice.NewSyncService(readService, writeService, logger, cfg.SyncService.BufferSize, appMetrics)
	service.Start()
	service.Wait()
	logger.Info("migration is finished")
	logger.Info("shutting down the application...")
}
