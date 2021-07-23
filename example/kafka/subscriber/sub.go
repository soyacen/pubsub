package main

import (
	"context"
	"fmt"
	"os"
	"time"

	easypubsub "github.com/soyacen/pubsub"
	kafkasubscriber "github.com/soyacen/pubsub/kafka/subscriber"
)

func main() {
	a := func(topic string, msg *easypubsub.Message, handler easypubsub.InterceptHandler) error {
		msg.Header().Set("time", time.Now().Format(time.RFC3339))
		return handler(topic, msg)
	}

	subscriber, err := kafkasubscriber.New(
		[]string{"localhost:9092"},
		kafkasubscriber.WithConsumerConfig(kafkasubscriber.DefaultSubscriberConfig()),
		kafkasubscriber.WithInterceptor(a),
		kafkasubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	if err != nil {
		panic(err)
	}
	defer func(subscriber easypubsub.Subscriber) {
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}(subscriber)

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err = subscriber.Subscribe(ctx, "awesome")
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
			fmt.Println("57")
			response := msg.Ack()
			fmt.Println("59")
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
