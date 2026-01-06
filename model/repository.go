package model

import (
	"context"
)

type WriteRepository interface {
	BulkUpsert(ctx context.Context, batch *RawCollection) error
}
