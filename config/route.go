package config

import (
	"fmt"

	"github.com/arashlml/data-stream/factory/repository_factory"
	"github.com/arashlml/data-stream/factory/writer"
	"github.com/arashlml/data-stream/repository/elastic_repository"
	"github.com/arashlml/data-stream/repository/mongo_repository"
	"github.com/arashlml/data-stream/repository/typesense_file_repository"
	"github.com/arashlml/data-stream/service/reader_service"
	syncservice "github.com/arashlml/data-stream/service/sync_service"
	"github.com/arashlml/data-stream/service/writer_service"
	"github.com/arashlml/data-stream/storage"
	"github.com/go-playground/validator/v10"
)

type Config struct {
	Storage           storage.Config            `koanf:"storage"`
	SyncService       syncservice.Config        `koanf:"sync-service"`
	ReadService       reader_service.Config     `koanf:"read-service"`
	FactoryIterator   iterator_factory.Config   `koanf:"iterator-factory"`
	FactoryRepository repository_factory.Config `koanf:"factory_repository"`
}

func (c *Config) ValidateConfig() error {
	validate := validator.New()
	err := validate.Struct(c)
	if err != nil {
		var errorMsg string
		for _, err := range err.(validator.ValidationErrors) {
			errorMsg += fmt.Sprintf("Field validation for '%s' failed on the '%s' tag; ", err.StructNamespace(), err.Tag())
		}
		return fmt.Errorf("configuration validation failed: %s", errorMsg)
	}
	return nil
}
