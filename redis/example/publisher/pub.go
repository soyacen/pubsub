package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/soyacen/easypubsub"
	redispublisher "github.com/soyacen/easypubsub/redis/publisher"
)

func main() {

	go awesome()
	go startsome()
	go something()

	select {}
}

func awesome() {
	publisher, err := redispublisher.New(
		redispublisher.SampleClient(&redis.Options{Addr: "localhost:6379"}),
		redispublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10000; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼 awesome "+strconv.Itoa(i)))))
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

func startsome() {
	publisher, err := redispublisher.New(
		redispublisher.SampleClient(&redis.Options{Addr: "localhost:6379"}),
		redispublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10000; i++ {
		result := publisher.Publish("startsome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼 startsome "+strconv.Itoa(i)))))
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

func something() {
	publisher, err := redispublisher.New(
		redispublisher.SampleClient(&redis.Options{Addr: "localhost:6379"}),
		redispublisher.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10000; i++ {
		result := publisher.Publish("something", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼 something "+strconv.Itoa(i)))))
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
