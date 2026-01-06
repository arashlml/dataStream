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
	Driver  string                    `koanf:"driver" validate:"oneof=elastic mongo"`
	Mongo   mongo_repository.Config   `koanf:"mongo"`
	Elastic elastic_repository.Config `koanf:"elastic"`
}

type RepoFactory struct {
	logger *slog.Logger
	driver string
}

func NewRepoFactory(logger *slog.Logger, config Config) *RepoFactory {
	return &RepoFactory{
		logger: logger,
		driver: config.Driver,
	}
}

func (f *RepoFactory) NewRepository(config Config) (model.WriteRepository, error) {
	switch f.driver {
	case "elastic":
		client, err := elastic_repository.NewConnector(f.logger, config.Elastic).Connect(context.Background())
		if err != nil {
			f.logger.Error("factory.repository_factory.elastic.connect.error",
				"error", err)
			return nil, errors.New("factory.repository_factory.elastic.connect.error")
		}
		repo := elastic_repository.NewElasticRepository(client, f.logger, config.Elastic)
		return repo, nil
	case "mongo":
		col, err := mongo_repository.NewConnector(f.logger, config.Mongo).ConnectAndMakeCollection(context.Background())
		if err != nil {
			f.logger.Error("factory.repository_factory.mongo.connect.error",
				"error", err)
			return nil, errors.New("factory.repository_factory.mongo.connect.error")
		}
		repo := mongo_repository.NewUpsertor(f.logger, col)
		return repo, nil
	}
	return nil, errors.New("factory.repository_factory.driver not supported")
}
