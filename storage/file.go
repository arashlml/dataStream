package storage

import (
	"encoding/csv"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/arashlml/data-stream/metrics"
	"github.com/arashlml/data-stream/model"
)

type Config struct {
	FilePath string `koanf:"filePath" validate:"required"`
}

type FileStorage struct {
	logger *slog.Logger
	path   string
}

func NewStorage(logger *slog.Logger, config Config) *FileStorage {
	return &FileStorage{logger: logger, path: config.FilePath}
}

func (s *FileStorage) Save(cursor model.Cursor) error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		s.logger.Error(
			"state.OpenFile.failed",
			"error", err,
		)
		metrics.ErrorCounter.WithLabelValues("storage.save.open_file", err.Error()).Inc()
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	marshaledMetaData, err := json.Marshal(cursor)
	stringMetaData := string(marshaledMetaData)
	SliceString := []string{stringMetaData}
	err = writer.Write(SliceString)
	if err != nil {
		s.logger.Error("state.writeToFile.write.failed",
			"meta_data", cursor,
			"error", err,
		)
		metrics.ErrorCounter.WithLabelValues("storage.save.write_file", err.Error()).Inc()
		return err
	}
	writer.Flush()

	return nil
}

func (s *FileStorage) LoadCursor() (model.Cursor, error) {
	f, err := os.Open(s.path)
	if err != nil {
		s.logger.Error("state.readFromFile.open.failed", "error", err)
		metrics.ErrorCounter.WithLabelValues("storage.loadMetaData.open_file", err.Error()).Inc()
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		s.logger.Error("state.readFromFile.read.failed", "error", err)
		metrics.ErrorCounter.WithLabelValues("storage.loadMetaData.read_all", err.Error()).Inc()
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	last := rows[len(rows)-1]
	if len(last) == 0 || last[0] == "" {
		s.logger.Error("state.readFromFile.invalid.row", "row", last)
		metrics.ErrorCounter.WithLabelValues("storage.loadMetaData.invalid_row", "invalid row in last_inserted_id.csv").Inc()
		return nil, nil
	}
	var cursor model.Cursor
	err = json.Unmarshal([]byte(last[0]), &cursor)
	if err != nil {
		s.logger.Error("storage.file.LoadCursor.unmarshal.failed", "error", err)
		return nil, err
	}
	return cursor, nil
}
