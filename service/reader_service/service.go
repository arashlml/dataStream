package reader_service

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
)

type Config struct {
	ResumeCapability bool `koanf:"resume_capability"`
}
type Storage interface {
	LoadCursor() (model.Cursor, error)
}

type Service struct {
	cursor      model.Cursor
	readCounter int64
	store       Storage
	iterator    model.Source
	logger      *slog.Logger
	resumeCap   bool
}

func New(store Storage, iterator model.Source, logger *slog.Logger, config Config) *Service {
	r := &Service{
		store:     store,
		iterator:  iterator,
		logger:    logger,
		resumeCap: config.ResumeCapability,
	}
	if r.resumeCap {
		cursor, err := r.store.LoadCursor()
		if err != nil {
			r.logger.Error("Service.reader.Service.new.loadLastID.error",
				"error", err.Error())
			metrics.ErrorCounter.WithLabelValues("reader_service.new.LoadCursor", err.Error()).Inc()
		}
		r.logger.Info("Service.reader.Service.new.meta.data.success", "metadata", cursor)
		r.cursor = cursor
	}

	return r
}
func (r *Service) Read(ctx context.Context) (*model.Collection, error) {
	collection, err := r.iterator.Next(ctx, r.cursor)
	if err != nil {
		r.logger.Error(
			"read.Service.next.error",
			"error", err,
			"meta_data", r.cursor,
		)
		metrics.ErrorCounter.WithLabelValues("reader_service.read.iterator_next", err.Error()).Inc()
		return nil, err
	}
	atomic.AddInt64(&r.readCounter, int64(collection.RawCollection.Len()))
	metrics.TotalReadDocuments.Add(float64(collection.RawCollection.Len()))
	if !r.iterator.HasNext(ctx) {
		r.logger.Info("read.Service.has.next.no.documents.left")
		return nil, nil
	}
	lastID := collection.RawCollection.LastItemID()
	r.cursor = collection.Cursor
	if atomic.LoadInt64(&r.readCounter)%10_000 == 0 {

		r.logger.Info("read.Service.read.counter",
			"lastID", lastID,
			"cursor", r.cursor,
			"readCounter", atomic.LoadInt64(&r.readCounter),
		)
	}
	return collection, nil
}
