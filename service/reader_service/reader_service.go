package reader_service

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/arashlml/data-stream/dto"
	"github.com/arashlml/data-stream/metrics"
)

type Config struct {
	ResumeCapability bool `koanf:"resume_capability"`
}
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
	resumeCap   bool
}

func New(store Storage, iterator Iterator, metrics *metrics.Metrics, logger *slog.Logger, Config Config) *ReaderService {
	r := &ReaderService{
		store:     store,
		iterator:  iterator,
		metric:    metrics,
		logger:    logger,
		resumeCap: Config.ResumeCapability,
	}
	if r.resumeCap {
		lastID, err := r.store.LoadLastID()
		if err != nil {
			r.logger.Error("service.reader.service.new.loadLastID.error",
				"error", err.Error())
			r.metric.ErrorCounter.WithLabelValues("reader_service.new.load_last_id", err.Error()).Inc()
		}
		r.lastID = lastID
	}
	return r
}

func (r *ReaderService) Read(ctx context.Context) (*dto.RawCollection, error) {
	batch, err := r.iterator.Next(ctx, r.lastID)
	if err != nil {
		r.logger.Error("read.service.next.error",
			"error", err,
			"lastID", r.lastID)
		r.metric.ErrorCounter.WithLabelValues("reader_service.read.iterator_next", err.Error()).Inc()
		return nil, err
	}
	atomic.AddInt64(&r.readCounter, int64(batch.Len()))
	r.metric.TotalReadDocuments.Add(float64(batch.Len()))
	if !r.iterator.HasNext(ctx) {
		r.logger.Info("read.service.has.next.no.batch.left")
		return nil, nil
	}
	r.lastID = batch.LastItemID()
	if atomic.LoadInt64(&r.readCounter)%10000 == 0 {

		r.logger.Info("read.service.read.counter",
			"lastID", r.lastID,
			"readCounter", atomic.LoadInt64(&r.readCounter),
		)
	}
	return batch, nil
}
