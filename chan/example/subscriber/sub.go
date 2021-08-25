package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/soyacen/easypubsub"
	chansubscriber "github.com/soyacen/easypubsub/chan/subscriber"
)

func main() {
	originC := make(chan *easypubsub.Message)
	go func() {
		for i := 0; i < 10000; i++ {
			originC <- easypubsub.NewMessage(easypubsub.WithBody([]byte("this is s msg")))
		}
		close(originC)
	}()
	subscriber := chansubscriber.New(
		originC,
		chansubscriber.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
		chansubscriber.WithNackResendSleepDuration(5*time.Second))

	msgC, errC := subscriber.Subscribe(context.Background(), "awesome")
	count := 0
out:
	for {
		select {
		case msg, ok := <-msgC:
			if !ok {
				fmt.Println("msg channel exit")
				break out
			}
			fmt.Println(string(msg.Body()))
			response := msg.Ack()
			fmt.Println(response)
			count++
			fmt.Println(count)
		case err, ok := <-errC:
			if !ok {
				fmt.Println("err channel exit")
				break out
			}
			fmt.Println(err)
		}
	}
	fmt.Println("count: ", count)
}
