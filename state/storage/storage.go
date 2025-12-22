package storage

import (
	"encoding/csv"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	FilePath string `koanf:"filePath"`
}

type Storage struct {
	logger *slog.Logger
	path   string
}

func NewStorage(logger *slog.Logger, path string) *Storage {
	return &Storage{logger: logger, path: path}
}

// TODO : write the collection name
func (s *Storage) Write(lastInsertedID string) error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		s.logger.Error(
			"state.OpenFile.failed",
			"error", err,
		)
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)

	err = writer.Write([]string{lastInsertedID})
	if err != nil {
		s.logger.Error("state.writeToFile.write.failed",
			"_id", lastInsertedID,
			"error", err,
		)
		return err
	}
	defer writer.Flush()

	return nil
}

func (s *Storage) Read() string {
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
