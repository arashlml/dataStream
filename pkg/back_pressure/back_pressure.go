package back_pressure

import (
	"log/slog"
	"sync/atomic"
)

type BackPressure[T any] struct {
	bufferSize int64
	channel    chan T
	quit       chan struct{}
	AddCounter int64
	logger     *slog.Logger
}

func NewBackPressure[T any](bufferSize int64, logger *slog.Logger) *BackPressure[T] {
	if bufferSize <= 0 {
		bufferSize = 1
	}

	b := &BackPressure[T]{
		bufferSize: bufferSize,
		channel:    make(chan T, bufferSize),
		quit:       make(chan struct{}),
		logger:     logger,
	}

	b.logger.Info(
		"backpressure.initialized",
		"buffer_size", bufferSize,
	)

	return b
}

func (b *BackPressure[T]) Add(item T) {

	// ⚠️ اگر بافر پر شده
	if int64(len(b.channel)) == b.bufferSize {
		b.logger.Warn(
			"backpressure.buffer.full",
			"buffer_size", b.bufferSize,
			"current_len", len(b.channel),
		)
	}

	select {
	case b.channel <- item:
		count := atomic.AddInt64(&b.AddCounter, 1)

		b.logger.Info(
			"backpressure.item.added",
			"total_added", count,
			"buffer_len", len(b.channel),
			"buffer_size", b.bufferSize,
		)

	case <-b.quit:
		b.logger.Warn(
			"backpressure.add.rejected",
			"reason", "backpressure is closed",
		)
		close(b.channel)
	}
}

func (b *BackPressure[T]) Out() chan T {
	b.logger.Info(
		"backpressure.channel.out.requested",
		"current_len", len(b.channel),
		"buffer_size", b.bufferSize,
	)
	return b.channel
}

func (b *BackPressure[T]) Close() {
	b.logger.Info(
		"backpressure.close.requested",
		"total_added", atomic.LoadInt64(&b.AddCounter),
		"buffer_len", len(b.channel),
	)

	close(b.quit)
}
