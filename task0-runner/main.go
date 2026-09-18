package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// insert the main data to the pipeline here
func InsertDataToPipeline(ctx context.Context, data string, inputChan chan<- string) {
	for {
		select {
		case <-ctx.Done():
			log.Println("error from done channal", ctx.Err())
			return
		case inputChan <- data:
		}
	}
}

// push worker
func pushWorker(ctx context.Context, wg *sync.WaitGroup, stream <-chan string) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Println("stopping email sub worker", ctx.Err())
			return;
		case data,ok := <-stream:
			if !ok {
				log.Println("something went wrong in push worker")
				return;
			}
			log.Println(data)
		}
	}

}

// email worker
func emailWorker(parentCtx context.Context, parentWg *sync.WaitGroup, stream <-chan string) {
	defer parentWg.Done()
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(parentCtx,time.Second * 5)
	defer cancel()
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go emailSubWorker(ctx, &wg, stream)
	}
	wg.Wait()

}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	emailNotificationChannals := make(chan string)
	pushNotificationChannals := make(chan string)
	go InsertDataToPipeline(ctx, "email", emailNotificationChannals)
	go InsertDataToPipeline(ctx, "push", pushNotificationChannals)

	wg.Add(1)
	go pushWorker(ctx,&wg,pushNotificationChannals)
	wg.Add(1)
	go emailWorker(ctx,&wg,emailNotificationChannals)

	wg.Wait()
}

func emailSubWorker(ctx context.Context, wg *sync.WaitGroup, stream <-chan string) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Println("stopping email sub worker", ctx.Err())
			return
		case data, ok := <-stream:
			if !ok {
				log.Println("something went wrong in recving data in email sub worker")
				return
			}
			log.Println(data)
		}
	}
}
