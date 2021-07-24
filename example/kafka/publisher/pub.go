package main

import (
	"fmt"
	"os"

	easypubsub "github.com/soyacen/pubsub"
	kafkapublisher "github.com/soyacen/pubsub/kafka/publisher"
)

func main() {
	sync()
	//async()
}

func async() {
	var headerFunc = func(topic string, msg *easypubsub.Message, handler easypubsub.MsgHandler) error {
		msg.Header().Set("interceptor", "true")
		msg.SetBody(append(msg.Body(), " 屌炸天"...))
		return handler(topic, msg)
	}

	publisher, err := kafkapublisher.New(
		[]string{"localhost:9092"},
		kafkapublisher.WithInterceptor(headerFunc),
		kafkapublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
		kafkapublisher.WithAsyncProducerConfig(kafkapublisher.DefaultSaramaConfig()),
	)

	for i := 0; i < 10000; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼"))))
		if result.Err != nil {
			panic(err)
		}
		fmt.Println(result.Result)
	}
	err = publisher.Close()
	if err != nil {
		panic(err)
	}
}

func sync() {
	var headerFunc = func(topic string, msg *easypubsub.Message, handler easypubsub.MsgHandler) error {
		msg.Header().Set("interceptor", "true")
		msg.SetBody(append(msg.Body(), " 屌炸天"...))
		return handler(topic, msg)
	}

	publisher, err := kafkapublisher.New(
		[]string{"localhost:9092"},
		kafkapublisher.WithInterceptor(headerFunc),
		kafkapublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
		kafkapublisher.WithSyncProducerConfig(kafkapublisher.DefaultSaramaConfig()),
	)

	for i := 0; i < 10000; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼"))))
		if result.Err != nil {
			panic(err)
		}
		fmt.Println(result.Result)
	}
	err = publisher.Close()
	if err != nil {
		panic(err)
	}
}
