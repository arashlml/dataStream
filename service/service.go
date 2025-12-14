package syncservice

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	"github.com/arashlml/mongo-reader/state"
	"go.mongodb.org/mongo-driver/bson"
)

type Reader interface {
	HasNext(ctx context.Context) bool
	CurrentBatch() []bson.M
	Next(ctx context.Context) error
}
type Writer interface {
	BulkInsert(ctx context.Context, buf bytes.Buffer, lastID string, lengthOfBatch int64) error
}

type Service struct {
	reader      Reader
	writer      Writer
	bp          *BackPressure.BackPressure[state.Batch]
	readCtx     context.Context
	readCancel  context.CancelFunc
	writeCtx    context.Context
	writeCancel context.CancelFunc
	wg          *sync.WaitGroup
	logger      *slog.Logger
	state       *state.State
}

func NewService(reader Reader, writer Writer, bp *BackPressure.BackPressure[state.Batch], logger *slog.Logger, state *state.State) *Service {
	readCtx, readCancelFunc := context.WithCancel(context.Background())
	writeCtx, writeCancelFunc := context.WithCancel(context.Background())
	s := &Service{
		reader:      reader,
		writer:      writer,
		bp:          bp,
		readCtx:     readCtx,
		readCancel:  readCancelFunc,
		writeCtx:    writeCtx,
		writeCancel: writeCancelFunc,
		wg:          &sync.WaitGroup{},
		logger:      logger,
		state:       state,
	}

	s.wg.Add(2)
	go s.readLoop(s.readCtx)
	go s.writeLoop(s.writeCtx)

	return s
}
func (s *Service) Close() {
	s.readCancel()
	s.writeCancel()
}

func (s *Service) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer s.bp.Close()
	for {

		err := s.reader.Next(ctx)
		if err != nil {
			s.logger.Error("service.readLoop.error", "error", err)
		}

		s.state.SetElasticBatch()
		s.state.SetBatchSize()
		s.state.DeleteBsonBatch()
		s.bp.Add(s.state.Batch)
		if !s.reader.HasNext(ctx) {
			s.logger.Info("service.readLoop.done")
			return
		}
	}
}

func (s *Service) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	channel := s.bp.Out()
	for batch := range channel {
		err := s.writer.BulkInsert(ctx, batch.ElasticBatch, batch.LastID.Hex(), batch.BatchSize)
		if err != nil {
			s.logger.Error("service.writeLoop.bulkInsertError",
				"error", err,
				"_id", batch.LastID,
			)
			s.retry(ctx, s.state.Attempts, 5*time.Second, batch.ElasticBatch, batch.LastID.Hex(), batch.BatchSize)
		}
	}
}

func (s *Service) retry(ctx context.Context, attempts int64, interval time.Duration, buf bytes.Buffer, lastID string, lengthOfBatch int64) {
	var err error
	var attempt int64
	for attempt = 1; attempt <= attempts; attempt++ {
		err = s.writer.BulkInsert(ctx, buf, lastID, lengthOfBatch)
		if err == nil {
			s.logger.Info(
				"service.retry.success",
				"attempt", attempt,
				"_id", lastID,
			)
			return
		}
		atomic.AddInt64(&s.state.TotalFailedDocuments, lengthOfBatch)
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
