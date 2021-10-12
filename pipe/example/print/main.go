package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/soyacen/easypubsub"
	iosubscriber "github.com/soyacen/easypubsub/io/subscriber"
	easypubsubpipe "github.com/soyacen/easypubsub/pipe"
	pipemiddleware "github.com/soyacen/easypubsub/pipe/middleware"
)

var source = flag.String("source", "", "source file path")

func main() {
	flag.Parse()
	if *source == "" {
		flag.Usage()
		return
	}

	sourceFile, err := os.OpenFile(*source, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}

	unmarshalMsgFunc := func(topic string, data []byte) (msg *easypubsub.Message, err error) {
		msg = easypubsub.NewMessage(
			easypubsub.WithBody(data),
			easypubsub.WithHeader(map[string][]string{"topic": {topic}}),
			easypubsub.WithContext(context.Background()),
		)
		return msg, nil
	}

	subscriber := iosubscriber.New(
		sourceFile,
		iosubscriber.WithDelimiter('\n'),
		iosubscriber.WithPollInterval(time.Second),
		iosubscriber.WithUnmarshalMsgFunc(unmarshalMsgFunc),
	)

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	channel := easypubsubpipe.New(
		easypubsubpipe.NewSource("", subscriber),
		nil,
		func(msg *easypubsub.Message) error {
			msg.Ack()
			fmt.Println(string(msg.Body()))
			return nil
		},
		easypubsubpipe.WithInterceptors(
			pipemiddleware.CustomRecovery(func(p interface{}) (err error) { return errors.New(fmt.Sprint(p)) }),
			func(msg *easypubsub.Message, handler easypubsubpipe.MessageHandler) error {
				randInt := rand.Intn(100)
				if randInt > 90 {
					panic(fmt.Errorf("%d > 90", randInt))
				}
				return handler(msg)
			}),
		easypubsubpipe.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 4000*time.Millisecond)
	defer cancelFunc()
	errC := channel.Flow(ctx)
	for err := range errC {
		fmt.Println("flow error:", err)
	}
	err = channel.Close()
	if err != nil {
		panic(err)
	}
}
