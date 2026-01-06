package model

import (
	"context"

	"github.com/arashlml/data-stream/dto"
)

type WriteRepository interface {
	BulkUpsert(ctx context.Context, batch *dto.RawCollection) error
}
