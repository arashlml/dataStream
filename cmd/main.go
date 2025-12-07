package main

import (
	"log"
	"time"

	BackPressure "github.com/arashlml/back-pressure"
	"github.com/arashlml/mongo-reader/pkg/iterator/mongoIterator"
	"github.com/arashlml/mongo-reader/repository/mongorepository"
	"github.com/arashlml/mongo-reader/service"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	uri := "mongodb://localhost:27017"
	client, err := mongorepository.Connect(uri)
	if err != nil {
		log.Fatalf("ERROR FROM CONNECT -> %v", err)
	}
	col := mongorepository.MakeMongoCollection(client, "mydb", "users")
	it := mongoIterator.NewMongoIterator(col, 50)
	bp := BackPressure.NewBackPressure[[]bson.M](10, 50, 30*time.Second)
	_ = service.NewService(it, nil, 50, bp)
	time.Sleep(1 * time.Hour)
}
