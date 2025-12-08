// TODO: better naming for this package, syncerservice maybe?
package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	BackPressure "github.com/arashlml/back-pressure"
	"go.mongodb.org/mongo-driver/bson"
)

// TODO: redundant X interface, remove it
type X interface {
	IsZero() bool
}

type Reader interface {
	HasNext() bool
	Batch() []bson.M
	Next(ctx context.Context) error
}
type Writer interface {
	BatchWrite(ctx context.Context, batch []map[string]interface{}) error
}

type Service struct {
	// TODO: no need to make Reader and Writer fields public if they are not accessed outside the package
	Reader         Reader
	Writer         Writer
	bp             *BackPressure.BackPressure[[]bson.M]
	batchSize      int
	ctx            context.Context
	cancel         context.CancelFunc
	wg             *sync.WaitGroup
	consumeCounter int64
}

func NewService(reader Reader, writer Writer, batchSize int, bp *BackPressure.BackPressure[[]bson.M]) *Service {
	ctx, cancelFunc := context.WithCancel(context.Background())
	s := &Service{
		Reader:    reader,
		Writer:    writer,
		bp:        bp,
		batchSize: batchSize,
		ctx:       ctx,
		cancel:    cancelFunc,
		wg:        &sync.WaitGroup{},
	}

	// TODO: batchSize is not used, either remove it or use it in the Reader (better to use it in the Reader)
	go s.readLoop(s.ctx, s.batchSize)
	go s.writeLoop(s.ctx)

	return s
}

func (s *Service) readLoop(ctx context.Context, batchSize int) {
	// TODO: track the read loop with the wait group and handle context cancelation properly , if not tracking, this can cause goroutine leaks (wg.Add(1), ...)
	for s.Reader.HasNext() {
		err := s.Reader.Next(ctx)

		if err != nil {
			log.Printf("SERVICE: ERROR FOR READING --> %v \n", err)
		}

		batch := s.Reader.Batch()
		s.bp.Add(batch)
	}
}

func (s *Service) writeLoop(ctx context.Context) {
	channel := s.bp.Out()

	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case _ = <-channel:
			atomic.AddInt64(&s.consumeCounter, 1)
			log.Printf("SERVICE: Consumed %v items \n", atomic.LoadInt64(&s.consumeCounter))
			time.Sleep(1 * time.Second)

			//err := s.Writer.BatchWrite(ctx, item)
			//if err != nil {
			//	log.Printf("SERVICE: ERROR FROM BATCH WRITE --> %v \n", err)
			//}
		// TODO: when the context is done, the Done channel of context will be closed, so this case will be selected repeatedly, causing a busy loop, handle it properly by returning from the function
		case <-ctx.Done():

			fmt.Println("SERVICE: context canceled")

			s.bp.Close()
		}
	}
}

func (s *Service) Close() {
	s.cancel()
	s.wg.Wait()
}
