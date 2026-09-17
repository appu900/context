package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(3)
	go worker(ctx,1,&wg)
	go worker(ctx,2,&wg)
	go worker(ctx,3,&wg)

	time.Sleep(time.Second * 5)
	fmt.Println("stopping all the workers")
	cancel()
	wg.Wait()
	fmt.Println("all the worker stopped")
}

func worker(ctx context.Context, workerId int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("worker", workerId, "stopped")
			return
		default:
			fmt.Println("worker", workerId, "is working..")
			time.Sleep(time.Second)
		}
	}
}
