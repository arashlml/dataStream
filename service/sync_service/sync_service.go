package sync_service

import (
	"context"
	"log/slog"
	"sync"

	"github.com/arashlml/mongo-reader/dto"
	"github.com/arashlml/mongo-reader/metrics"
)

type Config struct {
	BufferSize int `koanf:"bufferSize"`
}
type Reader interface {
	Read(ctx context.Context) (*dto.RawCollection, error)
}

type Writer interface {
	Write(ctx context.Context, batch *dto.RawCollection) error
}

type SyncService struct {
	reader              Reader
	writer              Writer
	readCtx             context.Context
	writeCtx            context.Context
	wg                  *sync.WaitGroup
	logger              *slog.Logger
	backPressureChannel chan *dto.RawCollection
	metrics             *metrics.Metrics
}

func NewSyncService(reader Reader, writer Writer, logger *slog.Logger, bufferSize int, metrics *metrics.Metrics) *SyncService {
	s := &SyncService{
		reader:              reader,
		writer:              writer,
		readCtx:             context.Background(),
		writeCtx:            context.Background(),
		wg:                  &sync.WaitGroup{},
		logger:              logger,
		metrics:             metrics,
		backPressureChannel: make(chan *dto.RawCollection, bufferSize),
	}
	return s
}
func (s *SyncService) Start() {
	s.wg.Add(2)
	go s.readLoop(s.readCtx)
	go s.writeLoop(s.writeCtx)
}

func (s *SyncService) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer close(s.backPressureChannel)
	for {
		batch, err := s.reader.Read(ctx)
		if err != nil {
			s.logger.Error("sync.service.readLoops.error", "error", err)
			s.metrics.ErrorCounter.WithLabelValues("sync_service.read_loop.read_failed", "", err.Error()).Inc()
		}
		if batch == nil {
			s.logger.Info("sync.service.readLoops.emptyBatch")
			return
		}
		s.backPressureChannel <- batch
	}
}

func (s *SyncService) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	for batch := range s.backPressureChannel {
		lastID := batch.LastItemID()
		err := s.writer.Write(ctx, batch)
		if err != nil {
			s.logger.Error(
				"service.writeLoop.bulkInsertError",
				"error", err,
				"_id", lastID,
			)
			s.metrics.ErrorCounter.WithLabelValues("sync_service.write_loop.write_failed", lastID, err.Error()).Inc()
		}
	}
}
func (s *SyncService) Wait() {
	s.wg.Wait()
}
