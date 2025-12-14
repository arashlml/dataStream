package back_pressure

import (
	"log/slog"
)

type BackPressure[T any] struct {
	bufferSize int64
	channel    chan T
	quit       chan struct{}
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
	b.channel <- item
}

func (b *BackPressure[T]) Out() chan T {
	return b.channel
}

func (b *BackPressure[T]) Close() {
	close(b.channel)
}
