package reader

import (
	"context"
	"log"
	"sync/atomic"

	backPressure "github.com/arashlml/back-pressure"
	"github.com/arashlml/mongo-reader/entity"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Reader struct {
	col            *mongo.Collection
	bp             *backPressure.BackPressure[[]entity.User]
	batch          []entity.User
	readCounter    int64
	sendingCounter int64
}

func NewReader(collection *mongo.Collection, bp *backPressure.BackPressure[[]entity.User]) *Reader {
	r := &Reader{
		col: collection,
		bp:  bp,
	}
	return r
}

func (r *Reader) Read(ctx context.Context, batchSize int64) error {
	var lastID primitive.ObjectID

	for {
		filter := bson.M{}
		if !lastID.IsZero() {
			filter["_id"] = bson.M{"$gt": lastID}
		}

		opts := options.Find().SetSort(bson.M{"_id": 1}).SetLimit(batchSize)

		cursor, err := r.col.Find(ctx, filter, opts)
		if err != nil {
			return err
		}

		var batch []bson.M

		if err := cursor.All(ctx, &batch); err != nil {
			return err
		}

		var users []entity.User

		for _, doc := range batch {
			bytes, err := bson.Marshal(doc)
			if err != nil {
				log.Fatal(err)
			}

			var user entity.User
			err = bson.Unmarshal(bytes, &user)
			if err != nil {
				log.Fatal(err)
			}

			users = append(users, user)
		}

		r.batch = users

		if len(batch) < int(batchSize) {
			log.Println("READER: FINISHED ALL THE READING DOCUMENTS!")
			log.Printf("READER: READ COUNTER IS EQUAL TO: %v \n", r.readCounter)
			r.bp.Add(r.batch)
			return nil
		}

		r.bp.Add(r.batch)

		atomic.AddInt64(&r.readCounter, 1)

		log.Printf("READER: Processing batch of %d documents\n", len(batch))

		lastID = batch[len(batch)-1]["_id"].(primitive.ObjectID)

		if int64(len(r.batch)) < batchSize {
			log.Println("READER: Reached end of collection.")
			return nil
		}
	}
}
