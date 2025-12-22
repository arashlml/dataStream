package config

import (
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	syncservice "github.com/arashlml/mongo-reader/service"
	"github.com/arashlml/mongo-reader/state"
	"github.com/arashlml/mongo-reader/state/storage"
)

type Config struct {
	State   state.Config             `koanf:"state"`
	Storage storage.Config           `koanf:"storage"`
	Service syncservice.Config       `koanf:"service"`
	Mongo   mongorepository.Config   `koanf:"mongo"`
	Elastic elasticrepository.Config `koanf:"elastic"`
}
