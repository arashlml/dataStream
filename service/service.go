package syncservice

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	BackPressure "github.com/arashlml/back-pressure"

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
	ctx             context.Context
	cancel          context.CancelFunc
	wg              *sync.WaitGroup
	consumeCounter  int64
	producerCounter int64
	indexName       string
}

func NewService(reader Reader, writer Writer, bp *BackPressure.BackPressure[[]bson.M], indexName string) *Service {
	ctx, cancelFunc := context.WithCancel(context.Background())
	s := &Service{
		reader:    reader,
		writer:    writer,
		bp:        bp,
		ctx:       ctx,
		cancel:    cancelFunc,
		wg:        &sync.WaitGroup{},
		indexName: indexName,
	}

	s.wg.Add(2)
	go s.readLoop(s.ctx)

	go s.writeLoop(s.ctx)

	return s
}

func (s *Service) readLoop(ctx context.Context) {

	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !s.reader.HasNext(ctx) {
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
			log.Printf("SERVICE: SEND TO BACKPRESSURE COUNTER --> %v \n", s.producerCounter)
			s.bp.Add(batch)
		}
	}
}

func (s *Service) writeLoop(ctx context.Context) {
	channel := s.bp.Out()
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case items := <-channel:
			atomic.AddInt64(&s.consumeCounter, 1)
			//log.Printf("SERVICE: Consumed %v items \n", atomic.LoadInt64(&s.consumeCounter))
			err := s.writer.BulkInsert(ctx, items)
			if err != nil {
				//log.Printf("SERVICE: Error inserting item %v \n", err)
			}

		case <-ctx.Done():

			log.Println("SERVICE: context canceled")

			s.bp.Close()

			return
		default:
			//log.Println("nothing to write")
		}
	}
}

func (s *Service) Close() {
	s.cancel()
	s.wg.Wait()
}
