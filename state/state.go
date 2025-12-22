package state

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/arashlml/mongo-reader/metrics"
)

type storage interface {
	Write(lastInsertedID string) error
	Read() string
}

type Config struct {
	ResumeCapability bool  `koanf:"resumeCapability"`
	Attempts         int64 `koanf:"attempts"`
}
type State struct {
	LastID                string
	Attempts              int64
	TotalReadDocuments    int64
	TotalDocuments        int64
	TotalWrittenDocuments int64
	TotalFailedDocuments  int64
	Index                 string
	logger                *slog.Logger
	LastInsertedID        string
	resumeCapability      bool
	storage               storage
	metrics               *metrics.Metrics
	ProgressIntervalLog   time.Duration
}

func NewState(attempts int64, index string, logger *slog.Logger, resumeCapability bool, storage storage, metrics *metrics.Metrics) *State {
	s := &State{
		Attempts:         attempts,
		Index:            index,
		logger:           logger,
		resumeCapability: resumeCapability,
		storage:          storage,
		metrics:          metrics,
	}
	if resumeCapability {
		id := s.storage.Read()
		s.LastID = id
	}
	return s
}
func (s *State) GetLastID() string {
	return s.LastID
}
func (s *State) SetLastID(id string) {
	s.LastID = id
}
func (s *State) SetTotalFailedDocuments(count int64) {
	atomic.AddInt64(&s.TotalFailedDocuments, count)
	s.metrics.TotalFailedDocuments.Add(float64(count))
}
func (s *State) AddTotalReadDocuments(count int64) {
	atomic.AddInt64(&s.TotalReadDocuments, count)
	s.metrics.TotalReadDocuments.WithLabelValues("/health", "500").Add(float64(count))
}
func (s *State) AddTotalWrittenDocuments(count int64) {
	atomic.AddInt64(&s.TotalWrittenDocuments, count)
	s.metrics.TotalWrittenDocuments.Add(float64(count))
}

func (s *State) SetLastInsertedID(id string) {
	s.LastInsertedID = id
	if err := s.storage.Write(id); err != nil {
		s.logger.Error("state.SetLastInsertedID.writing.to.csv.failed",
			"last_id", id,
			"error", err,
		)
	}
}
func (s *State) AddReadDuration(d time.Duration) {
	s.metrics.ReadDuration.Observe(float64(d))
}
func (s *State) AddWriteDuration(d time.Duration) {
	s.metrics.WriteDuration.Observe(float64(d))
}
func (s *State) SetTotalDocuments(total int64) {
	s.TotalDocuments = total
	s.metrics.TotalDocuments.Set(float64(total))
}
func (s *State) progress() {
	read := atomic.LoadInt64(&s.TotalReadDocuments)
	written := atomic.LoadInt64(&s.TotalWrittenDocuments)
	failed := atomic.LoadInt64(&s.TotalFailedDocuments)
	total := atomic.LoadInt64(&s.TotalDocuments)
	percent := float64(0)
	processed := written + failed
	if total != 0 {
		percent = (float64(processed) / float64(total)) * 100
	}

	barWidth := 50
	filled := int(percent / 100 * float64(barWidth))

	bar := strings.Repeat("=", filled) + ">" +
		strings.Repeat(" ", barWidth-filled)

	fmt.Print("\033[2K\r")
	fmt.Printf(
		"[%s] %.1f%% | read:%d | written:%d | fail:%d | total: %d",
		bar,
		percent,
		read,
		written,
		failed,
		total,
	)
}
func (s *State) ProgressWithCancel(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.progress()
			return
		default:
			time.Sleep(500 * time.Millisecond)
			s.progress()
		}
	}
}
