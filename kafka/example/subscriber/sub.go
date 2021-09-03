package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/soyacen/goutils/backoffutils"

	"github.com/soyacen/easypubsub"
	kafkasubscriber "github.com/soyacen/easypubsub/kafka/subscriber"
)

func main() {
	//consumerGroup()
	consumer()
}

func consumerGroup() {
	subscriber := kafkasubscriber.New(
		kafkasubscriber.ConsumerGroup([]string{"localhost:9092"}, "awesome-2", kafkasubscriber.DefaultSubscriberConfig()),
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
		kafkasubscriber.Consumer([]string{"localhost:9092"}, kafkasubscriber.DefaultSubscriberConfig()),
		kafkasubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
		kafkasubscriber.WithNackResend(3, backoffutils.Linear(3*time.Second)),
		kafkasubscriber.WithReconnectBackoff(backoffutils.Constant(5*time.Second)),
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
			fmt.Println(msg.Header())
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
