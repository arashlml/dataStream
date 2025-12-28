package config

import (
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service/reader_service"
	syncservice "github.com/arashlml/mongo-reader/service/sync_service"
	"github.com/arashlml/mongo-reader/storage"
)

type Config struct {
	Storage     storage.Config           `koanf:"storage"`
	ReadService reader_service.Config    `koanf:"read_service"`
	SyncService syncservice.Config       `koanf:"sync_service"`
	Mongo       mongorepository.Config   `koanf:"mongo"`
	Elastic     elasticrepository.Config `koanf:"elastic"`
}
