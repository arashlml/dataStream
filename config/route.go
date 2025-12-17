package config

import (
	"github.com/arashlml/mongo-reader/repository/elasticrepository"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
)

type Config struct {
	FilePath     string                   `koanf:"filePath"`
	ReadFromFile bool                     `koanf:"readFromFile"`
	Attempts     int64                    `koanf:"attempts"`
	BatchSize    int64                    `koanf:"batchSize"`
	BufferSize   int64                    `koanf:"bufferSize"`
	Mongo        mongorepository.Config   `koanf:"mongo"`
	Elastic      elasticrepository.Config `koanf:"elastic"`
}
