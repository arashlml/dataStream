package model

import (
	"context"
)

type Iterator interface {
	Next(ctx context.Context, cursor Cursor) (*Collection, error)
	HasNext(ctx context.Context) bool
}
