package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/soyacen/easypubsub"
	redissubscriber "github.com/soyacen/easypubsub/redis/subscriber"
)

func main() {
	//sample()
	pattern()
}

func sample() {
	subscriber := redissubscriber.New(
		redissubscriber.SampleClient(&redis.Options{Addr: "localhost:6379"}),
		redissubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	defer func(subscriber easypubsub.Subscriber) {
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}(subscriber)

	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ctx := context.Background()
	msgC, errC := subscriber.Subscribe(ctx, "awesome")
	count := 0
out:
	for {
		select {
		case msg, ok := <-msgC:
			if !ok {
				fmt.Println("break on msg chan")
				break out
			}
			count++
			fmt.Println("===============", count, "===============")
			fmt.Println(string(msg.Body()))
			intn := rand.Intn(4)
			fmt.Println(intn)
			if intn == 0 {
				response := msg.Ack()
				fmt.Println(response)
			} else {
				response := msg.Nack()
				fmt.Println(response)
			}
			<-time.After(time.Millisecond)
		case err, ok := <-errC:
			if !ok {
				fmt.Println("break on error chan")
				break out
			}
			fmt.Println("consume error", err)
		}
	}
	fmt.Println("count: ", count)
}

func pattern() {
	subscriber := redissubscriber.New(
		redissubscriber.SampleClient(&redis.Options{Addr: "localhost:6379"}),
		redissubscriber.WithEnablePatternSubscribe(),
		redissubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	defer func(subscriber easypubsub.Subscriber) {
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}(subscriber)

	go func() {
		time.Sleep(time.Second * 10)
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}()

	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ctx := context.Background()
	msgC, errC := subscriber.Subscribe(ctx, "*some")
	count := 0
out:
	for {
		select {
		case msg, ok := <-msgC:
			if !ok {
				fmt.Println("break on msg chan")
				break out
			}
			count++
			fmt.Println("===============", count, "===============")
			fmt.Println(string(msg.Body()))
			intn := rand.Intn(4)
			fmt.Println(intn)
			if intn == 0 {
				response := msg.Ack()
				fmt.Println(response)
			} else {
				response := msg.Nack()
				fmt.Println(response)
			}
			<-time.After(time.Millisecond)
		case err, ok := <-errC:
			if !ok {
				fmt.Println("break on error chan")
				break out
			}
			fmt.Println("consume error", err)
		}
	}
	fmt.Println("count: ", count)
}
