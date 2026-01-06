package model

import (
	"context"

	"github.com/arashlml/data-stream/dto"
)

type Iterator interface {
	Next(ctx context.Context, cursor dto.Cursor) (*dto.Collection, error)
	HasNext(ctx context.Context) bool
}
