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
	publisher, err := kafkapublisher.New(
		[]string{"localhost:9092"},
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
	publisher, err := kafkapublisher.New(
		[]string{"localhost:9092"},
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
