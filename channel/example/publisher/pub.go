package main

import (
	"fmt"

	"github.com/soyacen/easypubsub"
	channelpublisher "github.com/soyacen/easypubsub/channel/publisher"
)

func main() {
	msgC := make(chan *easypubsub.Message)
	exitC := make(chan struct{})
	go func() {
		i := 0
		for msg := range msgC {
			fmt.Println(string(msg.Body()))
			i++
		}
		fmt.Println(i)
		close(exitC)
	}()
	publisher := channelpublisher.New(msgC)
	for i := 0; i < 10000; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼"))))
		if result.Err != nil {
			panic(result.Err)
		}
		fmt.Println(result.Result)
	}
	close(msgC)
	<-exitC

}
