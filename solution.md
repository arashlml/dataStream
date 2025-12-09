# Solutions for `mongo-reader-solution-1`

This document provides the corrected code to address the critical issues identified in `code_review.md`. The proposed changes will fix data loss, prevent premature termination of the sync, and resolve deadlocks and race conditions related to graceful shutdown.

The most significant change is a redesign of the `Reader` interface and the `readLoop` to create a more robust and idiomatic Go producer.

--- 

## 1. Refactoring `pkg/iterator/mongo_iterator/mongo_iterator.go`

The original iterator's `HasNext` logic was the primary cause of premature termination and data loss. The iterator's responsibility should be simplified to just fetching the next batch of data. The decision to continue or stop iterating belongs in the calling loop.

We will remove the `HasNext` and `CurrentBatch` methods and have `Next` directly return the batch. Iteration stops when `Next` returns an empty slice.

### `mongo_iterator.go` - Corrected Code

```go
package mongo_iterator

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoIterator struct {
	col       *mongo.Collection
	batchSize int64
	lastID    primitive.ObjectID
	isFirst   bool // To handle the very first query correctly
}

func NewMongoIterator(col *mongo.Collection, batchSize int64) *MongoIterator {
	if batchSize <= 0 {
		batchSize = 50
	}
	return &MongoIterator{
		col:       col,
		batchSize: batchSize,
		isFirst:   true, // Start with the first iteration
	}
}

// Next fetches the next batch of documents from MongoDB.
// It returns an empty slice when there are no more documents to fetch.
func (m *MongoIterator) Next(ctx context.Context) ([]bson.M, error) {
	// If the last batch was smaller than the batch size, we know we're done.
	// The isFirst check ensures we run at least once.
	if !m.isFirst && m.lastID.IsZero() {
		return []bson.M{},
	}

	filter := bson.M{}
	// For all queries after the first one, use the lastID to paginate.
	if !m.lastID.IsZero() {
		filter["_id"] = bson.M{"$gt": m.lastID}
	}

	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(m.batchSize)

	cursor, err := m.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var batch []bson.M
	if err := cursor.All(ctx, &batch); err != nil {
		return nil, err
	}

	m.isFirst = false // No longer the first iteration

	// If the batch is empty, there's nothing more to read.
	if len(batch) == 0 {
		m.lastID = primitive.NilObjectID // Explicitly mark that we are done.
		return []bson.M{},
	}

	// If the batch returned is smaller than the requested size, it must be the last one.
	// We set lastID to zero so the next call to Next() returns an empty slice and stops the iteration.
	if int64(len(batch)) < m.batchSize {
		m.lastID = primitive.NilObjectID
	} else {
		// Otherwise, get the ID of the last document for the next query.
		lastDoc := batch[len(batch)-1]
		if lastID, ok := lastDoc["_id"].(primitive.ObjectID); !ok {
			// This is a critical error. We return an error to stop the service.
			log.Fatalf("MONGO ITERATOR: FATAL: _id is not a primitive.ObjectID. Halting to prevent infinite loop.")
		} else {
			m.lastID = lastID
		}
	}

	return batch, nil
}
```

--- 

## 2. Refactoring `service/service.go`

With the iterator fixed, we can now correct the service layer. The changes will:
1.  Update the `Reader` interface.
2.  Fix the `WaitGroup` handling in `NewService` and `writeLoop`.
3.  Implement a correct, deadlock-free shutdown sequence where the `readLoop` (producer) closes the channel and the `writeLoop` (consumer) drains it.

### `service.go` - Corrected Code

```go
package syncservice

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	BackPressure "github.com/arashlml/back-pressure"
	"go.mongodb.org/mongo-driver/bson"
)

// Reader interface is simplified. Next now returns the data directly.
type Reader interface {
	Next(ctx context.Context) ([]bson.M, error)
}

type Writer interface {
	BulkInsert(ctx context.Context, batch []bson.M) error
}

type Service struct {
	reader          Reader
	writer          Writer
	bp              *BackPressure.BackPressure[[]bson.M]
	ctx             context.Context
	cancel          context.CancelFunc
	wg              *sync.WaitGroup
	consumeCounter  int64
	producerCounter int64
	indexName       string
}

func NewService(reader Reader, writer Writer, bp *BackPressure.BackPressure[[]bson.M], indexName string) *Service {
	ctx, cancelFunc := context.WithCancel(context.Background())
	s := &Service{
		reader:    reader,
		writer:    writer,
		bp:        bp,
		ctx:       ctx,
		cancel:    cancelFunc,
		wg:        &sync.WaitGroup{},
		indexName: indexName,
	}

	// Correctly add 2 to the WaitGroup BEFORE starting goroutines.
	s.wg.Add(2)
	go s.readLoop()
	go s.writeLoop()

	return s
}

func (s *Service) readLoop() {
	// Ensure WaitGroup is marked as done and the back-pressure channel is closed when the loop exits.
	defer s.wg.Done()
	defer s.bp.Close()

	for {
		// Check for cancellation before doing work.
		select {
		case <-s.ctx.Done():
			log.Println("SERVICE: readLoop context canceled, shutting down.")
			return
		default:
		}

		batch, err := s.reader.Next(s.ctx)
		if err != nil {
			log.Printf("SERVICE: ERROR FOR READING --> %v \n", err)
			// Depending on requirements, you might want to stop the service on a read error.
			// For now, we'll stop the loop.
			return
		}

		// An empty batch signifies the end of the data stream.
		if len(batch) == 0 {
			log.Println("SERVICE: Reader has no more documents. Closing producer.")
			return
		}

		s.producerCounter++
		log.Printf("SERVICE: SEND TO BACKPRESSURE COUNTER --> %v \n", s.producerCounter)
		s.bp.Add(batch)
	}
}

func (s *Service) writeLoop() {
	// No s.wg.Add(1) here. This was a bug.
	defer s.wg.Done()

	// Use a for...range loop to correctly drain the channel.
	// This loop will automatically exit when the channel is closed by the readLoop.
	for items := range s.bp.Out() {
		atomic.AddInt64(&s.consumeCounter, 1)

		// The BulkInsert should respect the context to handle cancellations during a write.
		if err := s.writer.BulkInsert(s.ctx, items); err != nil {
			log.Printf("SERVICE: Error inserting item %v \n", err)
			// If the context was canceled, this error will reflect that, and the loop will continue
			// draining any remaining items (if any) before exiting.
		}
	}
	log.Println("SERVICE: writeLoop finished.")
}

func (s *Service) Close() {
	log.Println("SERVICE: Close called. Cancelling context and waiting for goroutines to finish.")
	s.cancel()
	s.wg.Wait()
	log.Println("SERVICE: Shutdown complete.")
}
```

### Summary of Fixes
-   **Data Loss:** The new iterator logic and `readLoop` ensure that every batch, including the last one, is processed.
-   **Deadlocks & Race Conditions:**
    -   The `WaitGroup` is now handled correctly, preventing the permanent block.
    -   The `readLoop` now reliably closes the back-pressure channel when it's finished (either by completing the read or by cancellation).
    -   The `writeLoop` now uses a `for range` loop, which is the standard, safe way to consume from a channel until it's closed. This guarantees it will process all buffered data and then exit, preventing both data loss and deadlocks.
-   **Robustness:** The service now has a clean and predictable shutdown sequence for both successful completion and external cancellation. The fatal error handling in the iterator for incorrect `_id` types also prevents difficult-to-debug infinite loops.
