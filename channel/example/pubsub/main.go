package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/soyacen/easypubsub"
	"github.com/soyacen/easypubsub/channel"
)

func main() {
	pubsub := channel.NewPubSub()
	pubsub.Start()
	defer pubsub.Close()

	subscriber1 := pubsub.Subscriber(easypubsub.NewStdLogger(os.Stdout), 3*time.Second)
	msgC1, errC1 := subscriber1.Subscribe(context.Background(), "topic1")

	subscriber2 := pubsub.Subscriber(easypubsub.NewStdLogger(os.Stdout), 3*time.Second)
	msgC2, errC2 := subscriber2.Subscribe(context.Background(), "topic2")

	subscriber3 := pubsub.Subscriber(easypubsub.NewStdLogger(os.Stdout), 3*time.Second)
	msgC3, errC3 := subscriber3.Subscribe(context.Background(), "topic2")

	publisher1 := pubsub.Publisher()
	go func() {
		for i := 0; i < 1000; i++ {
			publisher1.Publish("topic1", easypubsub.NewMessage(easypubsub.WithBody([]byte("this is "+strconv.Itoa(i)+" "+time.Now().String()))))
		}
	}()

	publisher2 := pubsub.Publisher()
	go func() {
		for i := 0; i < 1000; i++ {
			publisher2.Publish("topic2", easypubsub.NewMessage(easypubsub.WithBody([]byte("this is 2 "+strconv.Itoa(i)+" "+time.Now().String()))))
		}
	}()

	for {
		select {
		case msg := <-msgC1:
			log.Println("c1 " + string(msg.Body()))
			ackResp := msg.Ack()
			log.Printf("c1 %v", ackResp)
		case msg := <-msgC2:
			log.Println("c2 " + string(msg.Body()))
			ackResp := msg.Ack()
			log.Printf("c2 %v", ackResp)
		case msg := <-msgC3:
			log.Println("c3 " + string(msg.Body()))
			ackResp := msg.Ack()
			log.Printf("c3 %v", ackResp)
		case err := <-errC1:
			log.Println("c1 " + err.Error())
		case err := <-errC2:
			log.Println("c2 " + err.Error())
		case err := <-errC3:
			log.Println("c3 " + err.Error())
		}
	}

}
