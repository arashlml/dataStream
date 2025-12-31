package typesense_file_repository

import (
	"context"

	"github.com/arashlml/data-stream/dto"
)

type Config struct {
	FilePath string `koanf:"file_path"`
}

type Typesense struct {
	filePath string
}

func New(config Config) *Typesense {
	return &Typesense{filePath: config.FilePath}
}

func (t Typesense) Next(ctx context.Context, metaData dto.MetaData) (*dto.Collection, error) {
	panic("implement me")
}

func (t Typesense) HasNext(ctx context.Context) bool {
	panic("implement me")
}
