package syncservice

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	"github.com/arashlml/mongo-reader/pkg/bson_to_bytes"
	"go.mongodb.org/mongo-driver/bson/primitive"

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
	reader          Reader
	writer          Writer
	bp              *BackPressure.BackPressure[[]bson.M]
	readCtx         context.Context
	readCancel      context.CancelFunc
	writeCtx        context.Context
	writeCancel     context.CancelFunc
	wg              *sync.WaitGroup
	consumeCounter  int64
	producerCounter int64
	logger          *slog.Logger
	convertor       *bson_to_bytes.Convertor
}

func NewService(reader Reader, writer Writer, bp *BackPressure.BackPressure[[]bson.M], logger *slog.Logger, convertor *bson_to_bytes.Convertor) *Service {
	readCtx, readCancelFunc := context.WithCancel(context.Background())

	s := &Service{
		reader:     reader,
		writer:     writer,
		bp:         bp,
		readCtx:    readCtx,
		readCancel: readCancelFunc,
		writeCtx:   context.Background(),
		wg:         &sync.WaitGroup{},
		logger:     logger,
		convertor:  convertor,
	}

	s.wg.Add(2)
	go s.readLoop(s.readCtx)
	go s.writeLoop(s.writeCtx)

	return s
}
func (s *Service) Close() {
	s.readCancel()
}

func (s *Service) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer s.bp.Close()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("service.readLoop.stopped")
			return
		default:
			if !s.reader.HasNext(ctx) {
				s.logger.Info("service.readLoop.done")
				return
			}
			err := s.reader.Next(ctx)
			if err != nil {
				s.logger.Error("service.readLoop.error", "error", err)
			}
			items := s.reader.CurrentBatch()
			select {
			case <-ctx.Done():
				return
			default:
				atomic.AddInt64(&s.producerCounter, 1)
				s.bp.Add(items)
				if len(items) > 0 {
					doc := items[len(items)-1]
					if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
						s.logger.Warn(
							"service.reader.invalid_id",
							"_id", lastID,
							"id_type", fmt.Sprintf("%T", lastID),
						)
					} else {
						s.logger.Info("service.readLoop.channelCounter",
							"_id", lastID,
							"produce_counter", atomic.LoadInt64(&s.producerCounter),
						)
					}
				}
			}
		}
	}
}

func (s *Service) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	channel := s.bp.Out()
	for items := range channel {
		if len(items) > 0 {
			doc := items[len(items)-1]
			if lastID, ok := doc["_id"].(primitive.ObjectID); !ok {
				s.logger.Warn(
					"service.writeLoop.invalid_id",
					"_id", lastID,
					"id_type", fmt.Sprintf("%T", lastID),
				)
			} else {
				count := atomic.AddInt64(&s.consumeCounter, 1)
				s.logger.Info("service.writeLoop.channelCounter",
					"_id", lastID,
					"consume_counter", count,
				)
				buf, id, err := s.convertor.ConvertToBytes(items)
				if err != nil {
					s.logger.Error("service.writeLoop.error",
						"error", err,
						"_id", lastID)
				}
				err = s.writer.BulkInsert(ctx, buf, id, int64(len(items)))

				if err != nil {
					s.logger.Error("service.writeLoop.bulkInsertError",
						"_id", lastID,
						"error", err,
					)

				}
			}
		}
	}
	s.logger.Info("service.writeLoop.done")
}

func (s *Service) Start(ctx context.Context, attempts int, interval time.Duration, fn func() error, lastID string) error {
	var err error

	for attempt := 1; attempt <= attempts; attempt++ {
		err = fn()
		if err == nil {
			s.logger.Info(
				"service.retry.success",
				"attempt", attempt,
				"_id", lastID,
			)
			return nil
		}

		s.logger.Warn(
			"service.retry.failed",
			"attempt", attempt,
			"error", err,
			"_id", lastID,
		)

		if attempt < attempts {
			select {
			case <-time.After(time.Duration(attempt) * interval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return err
}
func (s *Service) Wait() {
	s.wg.Wait()
}
