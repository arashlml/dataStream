package sync_service

import (
	"context"
	"log/slog"
	"sync"

	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
)

type Config struct {
	BufferSize int `koanf:"bufferSize" validate:"gt=0"`
}
type Reader interface {
	Read(ctx context.Context) (*model.Collection, error)
}

type Writer interface {
	Write(ctx context.Context, batch *model.Collection) error
}

type SyncService struct {
	reader              Reader
	writer              Writer
	readCtx             context.Context
	writeCtx            context.Context
	wg                  *sync.WaitGroup
	logger              *slog.Logger
	backPressureChannel chan *model.Collection
}

func NewSyncService(reader Reader, writer Writer, logger *slog.Logger, config Config) *SyncService {
	s := &SyncService{
		reader:              reader,
		writer:              writer,
		readCtx:             context.Background(),
		writeCtx:            context.Background(),
		wg:                  &sync.WaitGroup{},
		logger:              logger,
		backPressureChannel: make(chan *model.Collection, config.BufferSize),
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
		if batch == nil && err == nil {
			s.logger.Info("sync.service.empty.batch")
			return
		}
		if err != nil {
			s.logger.Error("sync.service.readLoops.error", "error", err)
			metrics.ErrorCounter.WithLabelValues("sync_service.read_loop.read_failed", err.Error()).Inc()
		}
		s.backPressureChannel <- batch
	}
}

func (s *SyncService) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	for batch := range s.backPressureChannel {
		lastID := batch.RawCollection.LastItemID()
		err := s.writer.Write(ctx, batch)
		if err != nil {
			s.logger.Error(
				"service.writeLoop.bulkInsertError",
				"error", err,
				"_id", lastID,
			)
			metrics.ErrorCounter.WithLabelValues("sync_service.write_loop.write_failed", err.Error()).Inc()
		}
	}
}
func (s *SyncService) Wait() {
	s.wg.Wait()
}
