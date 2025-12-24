package reader_service

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/mongo-reader/dto"
	"github.com/arashlml/mongo-reader/metrics"
)

type Storage interface {
	LoadLastID() (string, error)
}

type Iterator interface {
	Next(ctx context.Context, lastID string) (*dto.RawCollection, error)
	HasNext(ctx context.Context) bool
}

type ReaderService struct {
	lastID      string
	readCounter int64
	store       Storage
	iterator    Iterator
	metric      *metrics.Metrics
	logger      *slog.Logger
}

func New(store Storage, iterator Iterator, metrics *metrics.Metrics, logger *slog.Logger) *ReaderService {
	r := &ReaderService{
		store:    store,
		iterator: iterator,
		metric:   metrics,
		logger:   logger,
	}
	lastID, err := r.store.LoadLastID()
	if err != nil {
		r.logger.Error("service.reader.service.new.loadLastID.error",
			"error", err.Error())
	}
	r.lastID = lastID
	return r
}

func (r *ReaderService) Read(ctx context.Context) (*dto.RawCollection, error) {
	batch, err := r.iterator.Next(ctx, r.lastID)
	if err != nil {
		r.logger.Error("read.service.next.error",
			"error", err,
			"lastID", r.lastID)
		return nil, err
	}
	atomic.AddInt64(&r.readCounter, int64(batch.Len()))
	if !r.iterator.HasNext(ctx) {
		r.logger.Info("read.service.has.next.no.batch.left")
		return nil, nil
	}
	r.lastID = batch.LastItemID()
	r.logger.Info("read.service.read.counter",
		"lastID", r.lastID,
		"readCounter", atomic.LoadInt64(&r.readCounter),
	)
	return batch, nil
}
