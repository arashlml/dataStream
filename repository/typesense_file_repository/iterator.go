package typesense_file_repository

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/arashlml/data-stream/dto"
)

type Config struct {
	FilePath  string `koanf:"file_path" validate:"required"`
	BatchSize int    `koanf:"batch_size" validate:"required,gt=0"`
}

type Typesense struct {
	filePath  string
	fileNames []string
	logger    *slog.Logger
	batchSize int
	cursor    map[string]interface{}
	reader    *bufio.Reader
	file      *os.File
}

func New(logger *slog.Logger, config Config) *Typesense {
	dirs, err := os.ReadDir(config.FilePath)
	if err != nil {
		logger.Error("repository.typesense_file_repository.New.ReadDir.error", "error", err)
		return nil
	}

	files := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if !d.IsDir() {
			files = append(files, d.Name())
		}
	}
	sort.Strings(files)

	return &Typesense{
		filePath:  config.FilePath,
		fileNames: files,
		logger:    logger,
		batchSize: config.BatchSize,
	}
}
func (t *Typesense) ConvertType() {
	switch r := t.cursor["start"].(type) {
	case int:
		t.cursor["start"] = r
	case float64:
		t.cursor["start"] = int(r)
	}
	switch r := t.cursor["fileIndex"].(type) {
	case int:
		t.cursor["fileIndex"] = r
	case float64:
		t.cursor["fileIndex"] = int(r)
	}
}

func (t *Typesense) HasNext(ctx context.Context) bool {
	fileIndex := t.cursor["fileIndex"].(int)
	return fileIndex < len(t.fileNames)
}
func (t *Typesense) UpdateReader(fileIndex int) error {
	filePath := filepath.Join(t.filePath, t.fileNames[fileIndex])
	var err error
	t.file, err = os.Open(filePath)
	if err != nil {
		t.logger.Error("repository.typesense_file_repository.UpdateReader.error", "err", err)
		return err
	}
	t.reader = bufio.NewReader(t.file)
	return nil
}
func (t *Typesense) CloseFile() {
	err := t.file.Close()
	if err != nil {
		t.logger.Error("repository.typesense_file_repository.Close.error", "err", err)
	}
}

func (t *Typesense) Next(ctx context.Context, cursor dto.Cursor) (*dto.Collection, error) {
	t.cursor = cursor
	if cursor != nil {
		t.ConvertType()
	} else {
		cursor = dto.Cursor{"fileIndex": 0, "start": 0}
	}

	fileIndex := cursor["fileIndex"].(int)
	lineStart := cursor["start"].(int)

	if fileIndex >= len(t.fileNames) {
		return nil, io.EOF
	}
	err := t.UpdateReader(fileIndex)
	if err != nil {
		t.logger.Error("repository.typesense_file_repository.UpdateReader.error", "err", err)

		return nil, err
	}
	defer t.CloseFile()
	documents := make([]map[string]interface{}, 0, t.batchSize)
	for i := 0; i < lineStart; i++ {
		if _, err := t.reader.ReadString('\n'); err != nil {
			if err == io.EOF {
				cursor["fileIndex"] = fileIndex + 1
				cursor["start"] = 0

				return &dto.Collection{RawCollection: documents, Cursor: cursor}, nil
			}
			return nil, err
		}
	}

	for len(documents) < t.batchSize {
		line, err := t.reader.ReadString('\n')
		if err == io.EOF {
			if len(line) > 0 {
				var doc map[string]interface{}
				if json.Unmarshal([]byte(line), &doc) == nil {
					documents = append(documents, doc)
				}
			}
			fileIndex++
			err := t.UpdateReader(fileIndex)
			if err != nil {
				t.logger.Error("repository.typesense_file_repository.UpdateReader.error", "error", err)
				return nil, err
			}
			lineStart = 0
		}
		if err != nil && err != io.EOF {
			t.logger.Error("repository.typesense_file_repository.ReadString.error", "err", err)
			return nil, err
		}

		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			lineStart++
			continue
		}

		documents = append(documents, doc)
		lineStart++
	}

	cursor["fileIndex"] = fileIndex
	cursor["start"] = lineStart
	t.cursor = cursor
	return &dto.Collection{
		RawCollection: documents,
		Cursor:        cursor,
	}, nil
}
