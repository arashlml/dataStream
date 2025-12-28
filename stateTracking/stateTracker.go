package stateTracking

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"time"
)

type Tracker struct {
	start     time.Time
	elapsed   time.Duration
	batchSize int
	path      string
	logger    *slog.Logger
}

func NewTracker(batchSize int, path string, logger *slog.Logger) *Tracker {
	return &Tracker{
		batchSize: batchSize,
		path:      path,
		logger:    logger,
	}
}
func (t *Tracker) PutStart() {
	t.start = time.Now()
}
func (t *Tracker) PutEnd() {
	t.elapsed = time.Since(t.start)
	_ = t.Save(t.batchSize, t.elapsed)
}
func (t *Tracker) Save(batchSize int, elapsed time.Duration) error {
	f, err := os.OpenFile(t.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.logger.Error(
			"state.OpenFile.failed",
			"error", err,
		)
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)

	err = writer.Write([]string{fmt.Sprintf("%d", batchSize), elapsed.String()})
	if err != nil {
		t.logger.Error("stateTracker.writeToFile.write.failed",
			"error", err,
		)
		return err
	}
	defer writer.Flush()

	return nil
}
