package syncservice

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	BackPressure "github.com/arashlml/mongo-reader/pkg/back_pressure"

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
}

func NewService(reader Reader, writer Writer, bp *BackPressure.BackPressure[[]bson.M], indexName string) *Service {
	readCtx, readCancelFunc := context.WithCancel(context.Background())

	s := &Service{
		reader:     reader,
		writer:     writer,
		bp:         bp,
		readCtx:    readCtx,
		readCancel: readCancelFunc,
		writeCtx:   context.Background(),
		wg:         &sync.WaitGroup{},
		indexName:  indexName,
		doneChan:   make(chan struct{}),
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
			return
		default:
			if !s.reader.HasNext(ctx) {
				close(s.doneChan)
				return
			}
			err := s.reader.Next(ctx)

			if err != nil {
				log.Printf("SERVICE: ERROR FOR READING --> %v \n", err)
			}

			batch := s.reader.CurrentBatch()
				select {
				case <-ctx.Done():
					return
				default:
					s.producerCounter++
					//log.Printf("SERVICE: SEND TO BACKPRESSURE COUNTER --> %v \n", s.producerCounter)
					s.bp.Add(batch)
				}
			}
		}
	}

}

func (s *Service) writeLoop(ctx context.Context) {
	defer s.wg.Done()
	channel := s.bp.Out()
	for items := range channel {
		atomic.AddInt64(&s.consumeCounter, 1)
		log.Printf("SERVICE: Channel write counter %v \n", atomic.LoadInt64(&s.consumeCounter))
		time.Sleep(1 * time.Second)
		err := s.writer.BulkInsert(ctx, items)
		if err != nil {
			log.Printf("SERVICE: Error inserting item %v \n", err)
		}
	}
}

func (s *Service) Close() {
	log.Println("SERVICE: Closing Service")
	s.readCancel()
	s.bp.Close()
	s.wg.Wait()
}
