package iterator_factory

import (
	"context"
	"errors"
	"log/slog"

	"github.com/arashlml/data-stream/model"
	"github.com/arashlml/data-stream/repository/mongo_repository"
	"github.com/arashlml/data-stream/repository/typesense_file_repository"
)

type Config struct {
	Driver string `koanf:"driver" validate:"oneof=typesense_file mongo"`
}
type IteratorFactory struct {
	driver              string
	TypesenseFileConfig *typesense_file_repository.Config
	MongoConfig         *mongo_repository.Config
	logger              *slog.Logger
}

func Newfactory(logger *slog.Logger, config Config, mongoConfig *mongo_repository.Config, typesenseFileConfig *typesense_file_repository.Config) *IteratorFactory {
	return &IteratorFactory{driver: config.Driver, TypesenseFileConfig: typesenseFileConfig, MongoConfig: mongoConfig, logger: logger}
}

func (it *IteratorFactory) NewIterator(config Config) (model.Iterator, error) {
	switch it.driver {
	case "typesense_file":
		iterator := typesense_file_repository.New(it.logger, it.TypesenseFileConfig)
		return iterator, nil
	case "mongo":
		col, err := mongo_repository.NewConnector(it.logger, it.MongoConfig).ConnectAndMakeCollection(context.Background())
		if err != nil {
			it.logger.Error("factory.iterator_factory.new.iterator.mongo.error",
				"error", err)
			return nil, err
		}
		iterator := mongo_repository.NewIterator(col, it.logger, it.MongoConfig)
		return iterator, nil
	}
	return nil, errors.New("not supported driver")
}
