package writer_service

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
)

type Service struct {
	writeCounter int64
	store        Storage
	repo         model.Destination
	logger       *slog.Logger
}

func New(store Storage, logger *slog.Logger, repo model.Destination) *Service {
	return &Service{
		store:  store,
		logger: logger,
		repo:   repo,
	}
}

type Storage interface {
	Save(cursor model.Cursor) error
}

func (s *Service) Write(ctx context.Context, batch *model.Collection) error {
	err := s.repo.BulkUpsert(ctx, &batch.RawCollection)
	if err != nil {
		s.logger.Error("writer.service.bulk.insert.error",
			"error", err,
			"last.ID", batch.RawCollection.LastItemID())
		metrics.ErrorCounter.WithLabelValues("writer_service.write.bulk_insert", err.Error()).Inc()
		return err
	}
	err = s.store.Save(batch.Cursor)
	if err != nil {
		s.logger.Error("writer.service.store.write.error",
			"error", err,
			"last.ID", batch.RawCollection.LastItemID())
		metrics.ErrorCounter.WithLabelValues("writer_service.write.store_save", err.Error()).Inc()
		return err
	}
	atomic.AddInt64(&s.writeCounter, int64(batch.RawCollection.Len()))
	metrics.TotalWrittenDocuments.Add(float64(batch.RawCollection.Len()))
	if atomic.LoadInt64(&s.writeCounter)%10_000 == 0 {
		s.logger.Info("writer.service.write.success",
			"last.ID", batch.RawCollection.LastItemID(),
			"writer.Counter", atomic.LoadInt64(&s.writeCounter))
	}
	return nil
}
