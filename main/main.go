package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	backPressure "github.com/arashlml/back-pressure"
	"github.com/arashlml/mongo-reader/entity"
	mongoReader "github.com/arashlml/mongo-reader/reader"
	"github.com/arashlml/mongo-reader/repository"
)

func main() {
	uri := "mongodb://localhost:27017"

	client, err := repository.ConnectMongo(uri)
	if err != nil {
		log.Fatalf("error happened! %v ", err)
	}

	bp := backPressure.NewBackPressure[[]entity.User](10, 50, 5*time.Second)
	reader := mongoReader.NewReader(client, "mydb", "users", bp)
	ctx := context.Background()
	err = reader.Read(ctx, 50)
	if err != nil {
		log.Fatalf("error happened! %v ", err)
	}
	channel := bp.Out()
	var num int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		item := <-channel
		log.Printf("CONSUMER: ITEM LENGHT RECIEVED: %v", len(item))
		atomic.AddInt64(&num, 1)
	}()
	wg.Wait()
	log.Printf("CONSUMER : RECEIVED %v ITEMS \n", num)
	log.Println("CONSUMER: done")

}
