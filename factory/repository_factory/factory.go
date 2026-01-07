package repository_factory

import (
	"context"
	"errors"
	"log/slog"

	"github.com/arashlml/data-stream/model"
	"github.com/arashlml/data-stream/repository/elastic_repository"
	"github.com/arashlml/data-stream/repository/mongo_repository"
)

type Config struct {
	Driver string `koanf:"driver" validate:"oneof=elastic mongo"`
}

type RepoFactory struct {
	logger        *slog.Logger
	MongoConfig   *mongo_repository.Config
	ElasticConfig *elastic_repository.Config
	driver        string
}

func NewRepoFactory(logger *slog.Logger, config Config, mongoConfig *mongo_repository.Config, elasticConfig *elastic_repository.Config) *RepoFactory {
	return &RepoFactory{
		logger:        logger,
		MongoConfig:   mongoConfig,
		ElasticConfig: elasticConfig,
		driver:        config.Driver,
	}
}

func (f *RepoFactory) NewRepository(config Config) (model.WriteRepository, error) {
	switch f.driver {
	case "elastic":
		client, err := elastic_repository.NewConnector(f.logger, f.ElasticConfig).Connect(context.Background())
		if err != nil {
			f.logger.Error("factory.repository_factory.elastic.connect.error",
				"error", err)
			return nil, errors.New("factory.repository_factory.elastic.connect.error")
		}
		repo := elastic_repository.NewElasticRepository(client, f.logger, f.ElasticConfig)
		return repo, nil
	case "mongo":
		col, err := mongo_repository.NewConnector(f.logger, f.MongoConfig).ConnectAndMakeCollection(context.Background())
		if err != nil {
			f.logger.Error("factory.repository_factory.mongo.connect.error",
				"error", err)
			return nil, errors.New("factory.repository_factory.mongo.connect.error")
		}
		repo := mongo_repository.NewUpsertor(f.logger, col)
		return repo, nil
	}
	return nil, errors.New("driver not supported")
}
