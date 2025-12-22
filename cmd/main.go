package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/arashlml/mongo-reader/config"
	"github.com/arashlml/mongo-reader/metrics"
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	mongoiterator "github.com/arashlml/mongo-reader/repository/mongorepository"
	syncservice "github.com/arashlml/mongo-reader/service"
	"github.com/arashlml/mongo-reader/state"
	"github.com/arashlml/mongo-reader/state/storage"
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
	st := state.NewState(cfg.State.Attempts, cfg.Elastic.Index, logger, cfg.State.ResumeCapability, store, appMetrics)
	MongoConnector := mongorepository.NewConnector(cfg.Mongo.Uri, cfg.Mongo.Username, cfg.Mongo.Password, cfg.Mongo.Db, cfg.Mongo.Collection, cfg.Mongo.Attempts, logger, cfg.Mongo.PingTimeout, cfg.Mongo.CountDocQueryTimeout, cfg.Mongo.ConnectTimeout)
	ctx := context.Background()
	client, err := MongoConnector.Connect(ctx)
	if err != nil {
		logger.Error("main.connecting.mongo.server.failed",
			"error", err,
		)
	}
	lastID := st.GetLastID()

	col := MongoConnector.MakeCollection(ctx, client)
	count, err := MongoConnector.CountDocuments(ctx, col, lastID)
	if err != nil {
		logger.Error("main.mongo.connector.count_documents.failed")
	}
	st.SetTotalDocuments(count)
	it := mongoiterator.NewIterator(col, cfg.Mongo.BatchSize, logger)
	elasticConnector := elasticrepository.NewConnector(cfg.Elastic.Uri, cfg.Elastic.Username, cfg.Elastic.Password, cfg.Elastic.Attempts, logger, cfg.Elastic.PingTimeout)
	elasticClient, err := elasticConnector.Connect(ctx)
	if err != nil {
		logger.Error("main.connecting.elastic.server.failed",
			"error", err,
		)
	}
	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, logger, cfg.Elastic.Index)
	progressCtx, cancel := context.WithCancel(ctx)
	go st.ProgressWithCancel(progressCtx)
	service := syncservice.NewService(it, elasticRepo, cfg.Service.InsertTimeout, cfg.Service.ReadTimeout, cfg.Service.RetryInterval, logger, cfg.Service.BufferSize, st)
	service.Start()
	service.Wait()
	time.Sleep(3 * time.Second)
	logger.Info("migration is finished")

	cancel()
	logger.Info("shutting down the application...")
}
