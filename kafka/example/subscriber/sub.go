package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/soyacen/easypubsub"
	kafkasubscriber "github.com/soyacen/easypubsub/kafka/subscriber"
)

func main() {
	consumerGroup()
	//consumer()
}

func consumerGroup() {
	subscriber := kafkasubscriber.New(
		[]string{"localhost:9092"},
		kafkasubscriber.WithConsumerGroupConfig("awesome-2", kafkasubscriber.DefaultSubscriberConfig()),
		kafkasubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	go func(subscriber easypubsub.Subscriber) {
		<-time.After(10 * time.Second)
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}(subscriber)

	ctx, _ := context.WithTimeout(context.Background(), 25*time.Second)
	err := subscriber.Subscribe(ctx, "awesome")
	if err != nil {
		panic(err)
	}

	msgC := subscriber.Messages()
	errC := subscriber.Errors()
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
			fmt.Println(count)
			fmt.Println("===============", count, "===============")
			fmt.Println(msg.Header())
			fmt.Println(string(msg.Body()))
			response := msg.Ack()
			fmt.Println(response)
		case err, ok := <-errC:
			if !ok {
				fmt.Println("break on error chan")
				break out
			}
			fmt.Println(err)
		}
	}
	fmt.Println("count: ", count)
}

func consumer() {
	subscriber := kafkasubscriber.New(
		[]string{"localhost:9092"},
		kafkasubscriber.WithConsumerConfig(kafkasubscriber.DefaultSubscriberConfig()),
		kafkasubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	defer func(subscriber easypubsub.Subscriber) {
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}(subscriber)

	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ctx := context.Background()
	err := subscriber.Subscribe(ctx, "awesome")
	if err != nil {
		panic(err)
	}

	msgC := subscriber.Messages()
	errC := subscriber.Errors()
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
			fmt.Println(msg.Header())
			fmt.Println(string(msg.Body()))
			response := msg.Ack()
			fmt.Println(response)
			//<-time.After(time.Millisecond+100)
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
