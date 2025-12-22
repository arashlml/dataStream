package syncservice

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/arashlml/mongo-reader/entity"
	"github.com/arashlml/mongo-reader/state"
)

type Config struct {
	BufferSize    int           `koanf:"bufferSize"`
	InsertTimeout time.Duration `koanf:"insertTimeout"`
	ReadTimeout   time.Duration `koanf:"readTimeout"`
	RetryInterval time.Duration `koanf:"retryInterval"`
}
type Reader interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context, lastID string) (*entity.RawCollection, error)
}

type Writer interface {
	BulkInsert(ctx context.Context, batch *entity.RawCollection) error
}

type Service struct {
	reader                    Reader
	writer                    Writer
	readCtx                   context.Context
	insertTimeOutTimeInterval time.Duration
	readTImeOutTimeInterval   time.Duration
	retryInterval             time.Duration
	writeCtx                  context.Context
	wg                        *sync.WaitGroup
	logger                    *slog.Logger
	backPressureChannel       chan *entity.RawCollection
	state                     *state.State
}

func NewService(reader Reader, writer Writer, insertTimeOutTimeInterval time.Duration, readTImeOutTimeInterval time.Duration, retryInterval time.Duration, logger *slog.Logger, bufferSize int, state *state.State) *Service {
	s := &Service{
		reader:                    reader,
		writer:                    writer,
		readCtx:                   context.Background(),
		writeCtx:                  context.Background(),
		insertTimeOutTimeInterval: insertTimeOutTimeInterval,
		readTImeOutTimeInterval:   readTImeOutTimeInterval,
		retryInterval:             retryInterval,
		wg:                        &sync.WaitGroup{},
		logger:                    logger,
		backPressureChannel:       make(chan *entity.RawCollection, bufferSize),
		state:                     state,
	}
	return s
}
func (s *Service) Start() {
	s.wg.Add(2)
	go s.readLoop(s.readCtx)
	go s.writeLoop(s.writeCtx)
}

func (s *Service) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer close(s.backPressureChannel)
	for {
		start := time.Now()
		loopCtx, cancel := context.WithTimeout(ctx, s.readTImeOutTimeInterval*time.Second)
		batch, err := s.reader.Next(loopCtx, s.state.LastID)
		cancel()
		if err != nil {
			s.logger.Error("service.readLoop.error", "error", err)
		} else {
			s.state.AddTotalReadDocuments(int64(batch.Len()))
		}
		s.state.SetLastID(batch.LastItemID())
		s.backPressureChannel <- batch
		elapsed := time.Since(start)
		s.state.AddReadDuration(elapsed)
		if !s.reader.HasNext(loopCtx) {
			s.logger.Info("service.readLoop.done")
			return
		}
	}
}

func (s *Service) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	for batch := range s.backPressureChannel {
		lastID := batch.LastItemID()
		start := time.Now()
		insertCtx, cancel := context.WithTimeout(ctx, s.insertTimeOutTimeInterval*time.Second)
		err := s.writer.BulkInsert(insertCtx, batch)
		cancel()
		if err != nil {
			s.logger.Error("service.writeLoop.bulkInsertError",
				"error", err,
				"_id", lastID,
			)
			s.state.SetTotalFailedDocuments(int64(batch.Len()))
			s.retry(ctx, s.state.Attempts, s.retryInterval*time.Second, batch)
		} else {
			s.state.AddTotalWrittenDocuments(int64(batch.Len()))
			s.state.SetLastInsertedID(lastID)
			elapsed := time.Since(start)
			s.state.AddWriteDuration(elapsed)
		}
	}
}

func (s *Service) retry(ctx context.Context, attempts int64, interval time.Duration, batch *entity.RawCollection) {
	var err error
	var attempt int64
	lastID := batch.LastItemID()
	for attempt = 1; attempt <= attempts; attempt++ {
		insertCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = s.writer.BulkInsert(insertCtx, batch)
		cancel()
		if err == nil {
			s.logger.Info(
				"service.retry.success",
				"attempt", attempt,
				"_id", lastID,
			)
			return
		}
		if attempt < attempts {
			select {
			case <-time.After(time.Duration(attempt) * interval):
			case <-ctx.Done():
				s.logger.Error("service.retry.failed",
					"attempt", attempt,
					"_id", lastID,
					"error", err,
				)
				return
			}
		}
	}

	s.logger.Warn("service.retry.failed",
		"attempt", attempts,
		"_id", lastID,
		"error", err,
	)
	s.state.SetTotalFailedDocuments(int64(batch.Len()))
	return
}
func (s *Service) Wait() {
	s.wg.Wait()
}
