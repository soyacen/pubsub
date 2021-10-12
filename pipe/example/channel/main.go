package main

import (
	"log"
	"os"

	"github.com/soyacen/easypubsub"
	easypubsubpipe "github.com/soyacen/easypubsub/pipe"
	pipemiddleware "github.com/soyacen/easypubsub/pipe/middleware"
)

func main() {
	pipe := easypubsubpipe.New(
		easypubsubpipe.NewSource(sub.SubscribeTopic(), sub.Subscriber()),
		nil,
		func(msg *easypubsub.Message) error {
			log.Println("time is " + string(msg.Body()))
			ackResp := msg.Ack()
			log.Println(ackResp)
			return nil
		},
		easypubsubpipe.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
		easypubsubpipe.WithInterceptors(pipemiddleware.Recovery()),
	)

}
