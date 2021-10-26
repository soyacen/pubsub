package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"

	"github.com/soyacen/easypubsub"

	kafkapublisher "github.com/soyacen/easypubsub/kafka/publisher"
)

func main() {
	//sync()
	//async()
	saramaSync()
}

func async() {
	publisher, err := kafkapublisher.New(
		kafkapublisher.AsyncProducer([]string{"localhost:9092"}, kafkapublisher.DefaultSaramaConfig()),
		kafkapublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)

	for i := 0; i < 10000; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼"))))
		if result.Err != nil {
			panic(err)
		}
		fmt.Println(result.Result)
		time.Sleep(time.Millisecond * 10)
	}
	err = publisher.Close()
	if err != nil {
		panic(err)
	}
}

func sync() {
	publisher, err := kafkapublisher.New(
		kafkapublisher.SyncProducer([]string{"localhost:9092"}, kafkapublisher.DefaultSaramaConfig()),
		kafkapublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10000; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼"))))
		if result.Err != nil {
			panic(result.Err)
		}
		fmt.Println(result.Result)
		time.Sleep(time.Millisecond * 100)
	}
	err = publisher.Close()
	if err != nil {
		panic(err)
	}
}

func saramaSync() {
	config := sarama.NewConfig()
	//config.Admin.Timeout = 3*time.Second
	config.Net.DialTimeout = 3 * time.Second
	config.Metadata.Timeout = 3 * time.Second
	//config.Producer.Timeout = 3*time.Second
	config.Producer.Return.Successes = true
	_, err := sarama.NewSyncProducer([]string{
		"192.168.4.95:9092",
		"192.168.4.96:9092",
		"192.168.4.97:9092",
	}, config)
	if err != nil {
		panic(err)
	}
}
