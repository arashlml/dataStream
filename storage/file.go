package storage

import (
	"encoding/csv"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/arashlml/data-stream/dto"
	"github.com/arashlml/data-stream/metrics"
)

type Config struct {
	FilePath string `koanf:"filePath" validate:"required"`
}

type FileStorage struct {
	logger  *slog.Logger
	path    string
	metrics *metrics.Metrics
}

func NewStorage(logger *slog.Logger, metrics *metrics.Metrics, config Config) *FileStorage {
	return &FileStorage{logger: logger, path: config.FilePath, metrics: metrics}
}

func (s *FileStorage) LoadMetaData(metaData dto.MetaData) error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		s.logger.Error(
			"state.OpenFile.failed",
			"error", err,
		)
		s.metrics.ErrorCounter.WithLabelValues("storage.save.open_file", err.Error()).Inc()
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	json, err := json.Marshal(metaData)
	stringMetaData := string(json)

	err = writer.Write([]string{stringMetaData})
	if err != nil {
		s.logger.Error("state.writeToFile.write.failed",
			"meta_data", metaData,
			"error", err,
		)
		s.metrics.ErrorCounter.WithLabelValues("storage.save.write_file", err.Error()).Inc()
		return err
	}
	defer writer.Flush()

	return nil
}

func (s *FileStorage) Save() (*dto.MetaData, error) {
	f, err := os.Open(s.path)
	if err != nil {
		s.logger.Error("state.readFromFile.open.failed", "error", err)
		s.metrics.ErrorCounter.WithLabelValues("storage.load_last_id.open_file", err.Error()).Inc()
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		s.logger.Error("state.readFromFile.read.failed", "error", err)
		s.metrics.ErrorCounter.WithLabelValues("storage.load_last_id.read_all", err.Error()).Inc()
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	last := rows[len(rows)-1]
	if len(last) == 0 || last[0] == "" {
		s.logger.Error("state.readFromFile.invalid.row", "row", last)
		s.metrics.ErrorCounter.WithLabelValues("storage.load_last_id.invalid_row", "invalid row in last_inserted_id.csv").Inc()
		return nil, nil
	}
	meta, err := json.Marshal(last[0])
	if err != nil {
		s.logger.Error("storage.file.load_meta_data.json.marshal.failed", "error", err)
		return nil, err
	}
	var metaData dto.MetaData
	err = json.Unmarshal(meta, &metaData)
	if err != nil {
		s.logger.Error("storage.file.load_meta_data.json.unmarshal.failed", "error", err)
		return nil, err
	}
	return &metaData, nil
}
