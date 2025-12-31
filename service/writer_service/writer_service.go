package writer_service

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/data-stream/dto"
	"github.com/arashlml/data-stream/metrics"
)

type Repository interface {
	BulkInsert(ctx context.Context, batch *dto.RawCollection) error
}

type Storage interface {
	Save(metaData dto.MetaData) error
}

type WriterService struct {
	writeCounter int64
	store        Storage
	repo         Repository
	metric       *metrics.Metrics
	logger       *slog.Logger
}

func New(store Storage, repo Repository, metric *metrics.Metrics, logger *slog.Logger) *WriterService {
	return &WriterService{store: store, repo: repo, metric: metric, logger: logger}
}

func (s *WriterService) Write(ctx context.Context, batch *dto.Collection) error {
	err := s.repo.BulkInsert(ctx, &batch.RawCollection)
	if err != nil {
		s.logger.Error("writer.service.bulk.insert.error",
			"error", err,
			"last.ID", batch.RawCollection.LastItemID())
		s.metric.ErrorCounter.WithLabelValues("writer_service.write.bulk_insert", err.Error()).Inc()
		return err
	}
	err = s.store.Save(batch.MetaData)
	if err != nil {
		s.logger.Error("writer.service.store.write.error",
			"error", err,
			"last.ID", batch.RawCollection.LastItemID())
		s.metric.ErrorCounter.WithLabelValues("writer_service.write.store_save", err.Error()).Inc()
		return err
	}
	atomic.AddInt64(&s.writeCounter, int64(batch.RawCollection.Len()))
	s.metric.TotalWrittenDocuments.Add(float64(batch.RawCollection.Len()))
	if atomic.LoadInt64(&s.writeCounter)%10000 == 0 {
		s.logger.Info("writer.service.store.write.success",
			"last.ID", batch.RawCollection.LastItemID(),
			"witer.Counter", atomic.LoadInt64(&s.writeCounter))
	}
	return nil
}
