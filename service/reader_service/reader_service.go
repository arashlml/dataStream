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
	LoadMetaData() (dto.MetaData, error)
}

type Iterator interface {
	Next(ctx context.Context, metaData dto.MetaData) (*dto.Collection, error)
	HasNext(ctx context.Context) bool
}

type ReaderService struct {
	metaData    dto.MetaData
	readCounter int64
	store       Storage
	iterator    Iterator
	metric      *metrics.Metrics
	logger      *slog.Logger
	resumeCap   bool
}

func New(store Storage, iterator Iterator, metrics *metrics.Metrics, logger *slog.Logger, config Config) *ReaderService {
	r := &ReaderService{
		store:     store,
		iterator:  iterator,
		metric:    metrics,
		logger:    logger,
		resumeCap: config.ResumeCapability,
	}
	if r.resumeCap {
		metaData, err := r.store.LoadMetaData()
		if err != nil {
			r.logger.Error("service.reader.service.new.loadLastID.error",
				"error", err.Error())
			r.metric.ErrorCounter.WithLabelValues("reader_service.new.LoadMetaData", err.Error()).Inc()
		}
		r.logger.Info("service.reader.service.new.meta.data.success", "metadata", metaData)
		r.metaData = metaData
	}

	return r
}

func (r *ReaderService) Read(ctx context.Context) (*dto.Collection, error) {
	collection, err := r.iterator.Next(ctx, r.metaData)
	if err != nil {
		r.logger.Error("read.service.next.error",
			"error", err,
			"meta_data", r.metaData)
		r.metric.ErrorCounter.WithLabelValues("reader_service.read.iterator_next", err.Error()).Inc()
		return nil, err
	}
	atomic.AddInt64(&r.readCounter, int64(collection.RawCollection.Len()))
	r.metric.TotalReadDocuments.Add(float64(collection.RawCollection.Len()))
	if !r.iterator.HasNext(ctx) {
		r.logger.Info("read.service.has.next.no.documents.left")
		return nil, nil
	}
	lastID := collection.RawCollection.LastItemID()
	r.metaData = collection.MetaData
	if atomic.LoadInt64(&r.readCounter)%1 == 0 {

		r.logger.Info("read.service.read.counter",
			"lastID", lastID,
			"meta_data", r.metaData,
			"readCounter", atomic.LoadInt64(&r.readCounter),
		)
	}
	return collection, nil
}
