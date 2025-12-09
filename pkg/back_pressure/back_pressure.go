package back_pressure

import (
	"log"
	"sync/atomic"
)

type BackPressure[T any] struct {
	bufferSize int64
	channel    chan T
	quit       chan struct{}
	AddCounter int64
}

func NewBackPressure[T any](bufferSize int64) *BackPressure[T] {
	b := &BackPressure[T]{
		bufferSize: bufferSize,
		channel:    make(chan T, bufferSize),
		quit:       make(chan struct{}),
	}
	return b
}

func (b *BackPressure[T]) Add(item T) {

	if int64(len(b.channel)) == b.bufferSize {
		log.Printf("BACK PRESSURE: Buffer is full... producer is screaming internally 😤")
	}
	select {
	case b.channel <- item:
		atomic.AddInt64(&b.AddCounter, 1)
		log.Printf("BACK PRESSURE: Added %v items \n", atomic.LoadInt64(&b.AddCounter))
	case <-b.quit:
		close(b.channel)
	}

}

func (b *BackPressure[T]) Out() chan T {
	return b.channel
}

func (b *BackPressure[T]) Close() {
	close(b.quit)

}
