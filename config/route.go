package config

import (
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	syncservice "github.com/arashlml/mongo-reader/service/sync_service"
	"github.com/arashlml/mongo-reader/storage"
)

type Config struct {
	Storage storage.Config           `koanf:"storage"`
	Service syncservice.Config       `koanf:"service"`
	Mongo   mongorepository.Config   `koanf:"mongo"`
	Elastic elasticrepository.Config `koanf:"elastic"`
}
