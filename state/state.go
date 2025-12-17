package state

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

type storage interface {
	Write(lastInsertedID string) error
	Read() string
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
	readFromFile          bool
	storage               storage
}

func NewState(attempts int64, index string, logger *slog.Logger, readFromFile bool, storage storage) *State {
	s := &State{
		Attempts:     attempts,
		Index:        index,
		logger:       logger,
		readFromFile: readFromFile,
		storage:      storage,
	}
	if readFromFile {
		id := s.storage.Read()
		var err error
		s.LastID = id
		if err != nil {
			s.logger.Error(
				"state.newState.readFromFile.Failed",
				"error", err)
		}
	}
	return s
}

func (s *State) SetLastID(id string) {
	s.LastID = id
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
func (s *State) SetTotalDocuments(total int64) {
	s.TotalDocuments = total
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
			time.Sleep(100 * time.Millisecond)
			s.progress()
		}
	}
}
