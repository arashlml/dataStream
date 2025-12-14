package state

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Batch struct {
	LastID       primitive.ObjectID
	BatchSize    int64
	BsonBatch    []bson.M
	ElasticBatch bytes.Buffer
}
type State struct {
	Attempts              int64
	TotalReadDocuments    int64
	TotalDocuments        int64
	TotalWrittenDocuments int64
	TotalFailedDocuments  int64
	Index                 string
	logger                *slog.Logger
	LastInsertedID        string
	readFromFile          bool
	path                  string
	Batch
}

func NewState(attempts int64, index string, logger *slog.Logger, path string, readFromFile bool) *State {
	s := &State{
		Attempts:     attempts,
		Index:        index,
		logger:       logger,
		readFromFile: readFromFile,
		path:         path,
	}
	if readFromFile {
		id := s.ReadFromFile()
		var err error
		s.LastID, err = primitive.ObjectIDFromHex(id)
		if err != nil {
			s.logger.Error(
				"state.newState.readFromFile.Failed",
				"error", err)
		}
	}
	return s
}

func (s *State) SetElasticBatch() {
	var buf bytes.Buffer

	if len(s.BsonBatch) == 0 {
		s.logger.Warn(
			"State.convert.bulk.skipped",
			"reason", "empty batch",
		)
		return
	}

	for _, doc := range s.BsonBatch {
		id, ok := doc["_id"]
		if !ok {
			s.logger.Error(
				"state.bulk.document.missing_id",
			)
			return
		}

		delete(doc, "_id")

		meta := map[string]map[string]interface{}{
			"index": {
				"_index": s.Index,
				"_id":    id,
			},
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			s.logger.Error(
				"state.bulk.meta.marshal.failed",
				"error", err,
				"_id", s.LastID,
			)
			return
		}

		docBytes, err := json.Marshal(doc)
		if err != nil {
			s.logger.Error(
				"state.bulk.doc.marshal.failed",
				"error", err,
				"_id", s.LastID,
			)
			return
		}

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}
	s.ElasticBatch = buf
	return
}

func (s *State) SetLastID(id primitive.ObjectID) {
	s.LastID = id
}
func (s *State) SetBatchSize() {
	s.BatchSize = int64(len(s.BsonBatch))
}
func (s *State) SetBsonBatch(batch []bson.M) {
	s.BsonBatch = batch
}

func (s *State) SetLastInsertedID(id string) {
	s.LastInsertedID = id
	s.WriteToFile()

}
func (s *State) DeleteBsonBatch() {
	s.BsonBatch = nil
}
func (s *State) SetTotalDocuments(total int64) {
	s.TotalDocuments = total
}

func (s *State) progress() {
	read := atomic.LoadInt64(&s.TotalReadDocuments)
	written := atomic.LoadInt64(&s.TotalWrittenDocuments)
	failed := atomic.LoadInt64(&s.TotalFailedDocuments)
	total := atomic.LoadInt64(&s.TotalDocuments)
	processed := written + failed
	percent := float64(processed) / float64(total)

	if total == 0 {
		percent = 0
	}

	fmt.Printf(
		"\r read:%d | written:%d | failed:%d | total:%d | progress: %.1f %%",
		read,
		written,
		failed,
		total,
		percent*100,
	)
}
func (s *State) ProgressWthCancel(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(100 * time.Millisecond)
			s.progress()
		}
	}
}

func (s *State) WriteToFile() {
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		s.logger.Error(
			"state.OpenFile.failed",
			"error", err,
		)
	}
	writer := csv.NewWriter(f)

	err = writer.Write([]string{s.LastInsertedID})
	if err != nil {
		s.logger.Error("state.writeToFile.write.failed",
			"_id", s.LastInsertedID,
			"error", err,
		)
	}
	defer f.Close()
	defer writer.Flush()
}

func (s *State) ReadFromFile() string {
	f, err := os.Open(s.path)
	if err != nil {
		s.logger.Error("state.readFromFile.open.failed", "error", err)
		return ""
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		s.logger.Error("state.readFromFile.read.failed", "error", err)
		return ""
	}

	if len(rows) == 0 {
		return ""
	}

	last := rows[len(rows)-1]
	if len(last) == 0 || last[0] == "" {
		s.logger.Error("state.readFromFile.invalid.row", "row", last)
		return ""
	}

	return strings.TrimSpace(last[0])
}
