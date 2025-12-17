package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/arashlml/mongo-reader/config"
	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	mongoiterator "github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service"
	"github.com/arashlml/mongo-reader/state"
	"github.com/arashlml/mongo-reader/state/storage"
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
	var cfg config.Config
	if err := k.Unmarshal("", &cfg); err != nil {
		logger.Error("error unmarshalling config: ", err)
	}
	storage := storage.NewStorage(logger, cfg.FilePath)
	st := state.NewState(cfg.Attempts, cfg.Elastic.Index, logger, cfg.ReadFromFile, storage)
	MongoConnector := mongorepository.NewConnector(cfg.Mongo.Uri, cfg.Mongo.Username, cfg.Mongo.Password, cfg.Mongo.Db, cfg.Mongo.Collection, logger, st)
	client, err := MongoConnector.Connect()
	if err != nil {
		logger.Error("main.connecting.mongo.server.failed",
			"error", err,
		)
	}
	col := MongoConnector.MakeCollection(client)
	it := mongoiterator.NewIterator(col, cfg.BatchSize, logger, st)
	elasticConnector := elasticrepository.NewConnector(cfg.Elastic.Uri, cfg.Elastic.Username, cfg.Elastic.Password, logger)
	elasticClient, err := elasticConnector.Connect()
	if err != nil {
		logger.Error("main.connecting.elastic.server.failed",
			"error", err,
		)
	}
	elasticRepo := elasticrepository.NewElasticRepository(elasticClient, cfg.Elastic.Index, logger, st)
	bp := BackPressure.NewBackPressure[[]map[string]interface{}](cfg.BufferSize, logger)
	ctx, cancel := context.WithCancel(context.Background())
	go st.ProgressWithCancel(ctx)
	service := syncservice.NewService(it, elasticRepo, bp, logger, st)
	service.Wait()
	time.Sleep(3 * time.Second)
	cancel()
	logger.Info("migration is finished")

}
