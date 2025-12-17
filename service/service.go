package syncservice

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arashlml/mongo-reader/state"
)

type Iterator interface {
	HasNext(ctx context.Context) bool
	CurrentBatch() []map[string]interface{}
	Next(ctx context.Context) error
}

type Writer interface {
	BulkInsert(ctx context.Context, batch []map[string]interface{}) error
}

type Service struct {
	iterator            Iterator
	writer              Writer
	readCtx             context.Context
	writeCtx            context.Context
	wg                  *sync.WaitGroup
	logger              *slog.Logger
	backPressureChannel chan []map[string]interface{}
	state               *state.State
}

func NewService(iterator Iterator, writer Writer, logger *slog.Logger, bufferSize int, state *state.State) *Service {
	s := &Service{
		iterator:            iterator,
		writer:              writer,
		readCtx:             context.Background(),
		writeCtx:            context.Background(),
		wg:                  &sync.WaitGroup{},
		logger:              logger,
		backPressureChannel: make(chan []map[string]interface{}, bufferSize),
		state:               state,
	}

	s.wg.Add(2)
	go s.readLoop(s.readCtx)
	go s.writeLoop(s.writeCtx)

	return s
}
func (s *Service) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer close(s.backPressureChannel)
	for {
		err := s.iterator.Next(ctx)
		if err != nil {
			s.logger.Error("service.readLoop.error", "error", err)
		}
		batch := s.iterator.CurrentBatch()
		s.backPressureChannel <- batch
		if !s.iterator.HasNext(ctx) {
			s.logger.Info("service.readLoop.done")
			return
		}
	}
}

// HELPER
func (s *Service) lastIDFinder(batch []map[string]interface{}) string {
	if len(batch) == 0 {
		return "no valid last_id"
	}
	if lastID, ok := batch[len(batch)-1]["_id"].(string); ok {
		return lastID
	} else {
		return "no valid last_id"
	}
}

func (s *Service) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	for batch := range s.backPressureChannel {
		if len(batch) == 0 {
			continue
		}
		lastID := s.lastIDFinder(batch)
		batchForRepository := batch
		err := s.writer.BulkInsert(ctx, batchForRepository)
		if err != nil {
			s.logger.Error("service.writeLoop.bulkInsertError",
				"error", err,
				"_id", lastID,
			)
			s.retry(ctx, s.state.Attempts, 5*time.Second, batchForRepository)
		}
	}
}

func (s *Service) retry(ctx context.Context, attempts int64, interval time.Duration, batch []map[string]interface{}) {
	var err error
	var attempt int64
	lastID := s.lastIDFinder(batch)
	for attempt = 1; attempt <= attempts; attempt++ {
		err = s.writer.BulkInsert(ctx, batch)
		if err == nil {
			s.logger.Info(
				"service.retry.success",
				"attempt", attempt,
				"_id", lastID,
			)
			return
		}
		atomic.AddInt64(&s.state.TotalFailedDocuments, int64(len(batch)))
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
	return
}
func (s *Service) Wait() {
	s.wg.Wait()
}
