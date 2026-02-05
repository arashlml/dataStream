package model

import (
	"context"
)

type Destination interface {
	BulkUpsert(ctx context.Context, batch *RawCollection) error
}
