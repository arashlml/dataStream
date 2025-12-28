package config

import (
	"github.com/arashlml/data-stream/repository/elasticrepository"
	"github.com/arashlml/data-stream/repository/mongorepository"
	"github.com/arashlml/data-stream/service/reader_service"
	syncservice "github.com/arashlml/data-stream/service/sync_service"
	"github.com/arashlml/data-stream/storage"
)

type Config struct {
	Storage     storage.Config           `koanf:"storage"`
	SyncService syncservice.Config       `koanf:"sync-service"`
	ReadService reader_service.Config    `koanf:"read-service"`
	Mongo       mongorepository.Config   `koanf:"mongo"`
	Elastic     elasticrepository.Config `koanf:"elastic"`
}
