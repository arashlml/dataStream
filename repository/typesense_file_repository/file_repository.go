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

func New(config Config) {

}

func (t Typesense) Next(ctx context.Context, metaData dto.MetaData) (*dto.Collection, error) {
	//TODO implement me
	panic("implement me")
}

func (t Typesense) HasNext(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}
