package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/soyacen/easypubsub"
	iosubscriber "github.com/soyacen/easypubsub/io/subscriber"
)

var filepath = flag.String("filepath", "", "file path")

func main() {
	flag.Parse()
	if *filepath == "" {
		flag.Usage()
		return
	}

	f, err := os.OpenFile(*filepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}

	subscriber := iosubscriber.New(
		f,
		iosubscriber.WithDelimiter('\n'),
		iosubscriber.WithPollInterval(time.Second),
	)
	defer func(subscriber easypubsub.Subscriber) {
		err := subscriber.Close()
		if err != nil {
			panic(err)
		}
	}(subscriber)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
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
				break out
			}
			fmt.Println(msg.Header())
			fmt.Println(string(msg.Body()))
			response := msg.Ack()
			fmt.Println(response)
			count++
			fmt.Println(count)
		case err, ok := <-errC:
			if !ok {
				break out
			}
			fmt.Println(err)
		}
	}
	fmt.Println("count: ", count)
}
