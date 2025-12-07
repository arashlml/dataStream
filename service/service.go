package service

import (
	"context"
	"log"

	BackPressure "github.com/arashlml/back-pressure"
)

type Reader interface {
	BatchRead(ctx context.Context, batchSize int) ([]map[string]interface{}, error)
}
type Writer interface {
	BatchWrite(ctx context.Context, batch []map[string]interface{}) error
}

type Service struct {
	Reader    Reader
	Writer    Writer
	bp        *BackPressure.BackPressure[[]map[string]interface{}]
	batchSize int
	ctx       context.Context
}

func NewService(reader Reader, writer Writer, batchSize int, bp *BackPressure.BackPressure[[]map[string]interface{}]) *Service {
	s := &Service{
		Reader:    reader,
		Writer:    writer,
		bp:        bp,
		batchSize: batchSize,
		ctx:       context.Background(),
	}
	go s.Read(s.ctx, s.batchSize)
	go s.Write(s.ctx)
	return s
}

func (s *Service) Read(ctx context.Context, batchSize int) {
	batch, err := s.Reader.BatchRead(ctx, batchSize)
	if err != nil {
		log.Printf("SERVICE: ERROR FROM BATCH READ --> %v \n", err)
	}
	s.bp.Add(batch)
}

func (s *Service) Write(ctx context.Context) {
	channel := s.bp.Out()

	item := <-channel

	err := s.Writer.BatchWrite(ctx, item)
	if err != nil {
		log.Printf("SERVICE: ERROR FROM BATCH WRITE --> %v \n", err)
	}
}
