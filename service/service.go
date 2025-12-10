package syncservice

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
)

type Reader interface {
	HasNext(ctx context.Context) bool
	CurrentBatch() []bson.M
	Next(ctx context.Context) error
}
type Writer interface {
	BulkInsert(ctx context.Context, batch []bson.M) error
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
	indexName       string
	doneChan        chan struct{}
	logger          *slog.Logger
}

func NewService(reader Reader, writer Writer, bp *BackPressure.BackPressure[[]bson.M], logger *slog.Logger) *Service {
	readCtx, readCancelFunc := context.WithCancel(context.Background())

	s := &Service{
		reader:     reader,
		writer:     writer,
		bp:         bp,
		readCtx:    readCtx,
		readCancel: readCancelFunc,
		writeCtx:   context.Background(),
		wg:         &sync.WaitGroup{},
		doneChan:   make(chan struct{}),
		logger:     logger,
	}

	s.wg.Add(2)
	go s.readLoop(s.readCtx)
	go s.writeLoop(s.writeCtx)

	return s
}
func (s *Service) Done() <-chan struct{} {
	return s.doneChan
}

func (s *Service) readLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("service.readLoop.stopped")
			return
		default:
			if !s.reader.HasNext(ctx) {
				s.logger.Info("service.readLoop.done")
				close(s.doneChan)
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
				err := s.writer.BulkInsert(ctx, items)
				if err != nil {
					s.logger.Error("service.writeLoop.bulkInsertError", "error", err)
				}
			}
		}
	}
}

func (s *Service) Close() {
	s.logger.Info("service.closing")
	s.readCancel()
	s.bp.Close()
	s.wg.Wait()
}
