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
	FilePath  string `koanf:"file_path"`
	BatchSize int    `koanf:"batch_size"`
}

type Typesense struct {
	filePath  string
	fileNames []string
	logger    *slog.Logger
	pageSize  int
	metaData  map[string]interface{}
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
		pageSize:  config.BatchSize,
	}
}
func (t *Typesense) ConvertType() {
	switch r := t.metaData["start"].(type) {
	case int:
		t.metaData["start"] = r
	case float64:
		t.metaData["start"] = int(r)
	}
	switch r := t.metaData["fileIndex"].(type) {
	case int:
		t.metaData["fileIndex"] = r
	case float64:
		t.metaData["fileIndex"] = int(r)
	}
}

func (t *Typesense) HasNext(ctx context.Context) bool {
	fileIndex, ok := t.metaData["fileIndex"].(int)
	if !ok {
		t.logger.Error("repository.typesense_file.HasNext.type.assertion.failed", "fileIndex", fileIndex)
		return false
	}
	return fileIndex < len(t.fileNames)
}

func (t *Typesense) Next(ctx context.Context, meta dto.MetaData) (*dto.Collection, error) {
	t.metaData = meta
	if meta != nil {
		t.ConvertType()
	} else {
		meta = dto.MetaData{"fileIndex": int(0), "start": int(0)}
	}

	fileIndex := meta["fileIndex"].(int)
	lineStart := meta["start"].(int)

	if fileIndex >= len(t.fileNames) {
		return nil, io.EOF
	}

	filePath := filepath.Join(t.filePath, t.fileNames[fileIndex])
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	documents := make([]map[string]interface{}, 0, t.pageSize)

	for i := 0; i < lineStart; i++ {
		if _, err := reader.ReadString('\n'); err != nil {
			if err == io.EOF {
				meta["fileIndex"] = float64(fileIndex + 1)
				meta["start"] = float64(0)
				return &dto.Collection{RawCollection: documents, MetaData: meta}, nil
			}
			return nil, err
		}
	}

	for len(documents) < t.pageSize {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					var doc map[string]interface{}
					if json.Unmarshal([]byte(line), &doc) == nil {
						documents = append(documents, doc)
					}
				}
				fileIndex++
				lineStart = 0
				break
			}
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

	meta["fileIndex"] = fileIndex
	meta["start"] = lineStart
	t.metaData = meta
	return &dto.Collection{
		RawCollection: documents,
		MetaData:      meta,
	}, nil
}
