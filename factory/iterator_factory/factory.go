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
	Driver        string                           `koanf:"driver" validate:"oneof=typesense_file mongo"`
	TypesenseFile typesense_file_repository.Config `koanf:"typesense_file" validate:"omitempty"`
	Mongo         mongo_repository.Config          `koanf:"mongo" validate:"omitempty"`
}
type IteratorFactory struct {
	driver string
	logger *slog.Logger
}

func Newfactory(logger *slog.Logger, config Config) *IteratorFactory {
	return &IteratorFactory{driver: config.Driver, logger: logger}
}

func (it *IteratorFactory) NewIterator(config Config) (model.Iterator, error) {
	switch it.driver {
	case "typesense_file":
		iterator := typesense_file_repository.New(it.logger, config.TypesenseFile)
		return iterator, nil
	case "mongo":
		col, err := mongo_repository.NewConnector(it.logger, config.Mongo).ConnectAndMakeCollection(context.Background())
		if err != nil {
			it.logger.Error("factory.iterator_factory.new.iterator.mongo.error",
				"error", err)
			return nil, err
		}
		iterator := mongo_repository.NewIterator(col, it.logger, config.Mongo)
		return iterator, nil
	}
	return nil, errors.New("not supported driver")
}
