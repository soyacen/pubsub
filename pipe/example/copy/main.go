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
	iopublisher "github.com/soyacen/easypubsub/io/publisher"
	iosubscriber "github.com/soyacen/easypubsub/io/subscriber"
	easypubsubpipe "github.com/soyacen/easypubsub/pipe"
	pipemiddleware "github.com/soyacen/easypubsub/pipe/middleware"
)

var source = flag.String("source", "", "source file path")
var target = flag.String("target", "", "target file path")

func main() {
	flag.Parse()
	if *source == "" || *target == "" {
		flag.Usage()
		return
	}

	sourceFile, err := os.OpenFile(*source, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}

	targetFile, err := os.OpenFile(*target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
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

	marshalMsg := func(topic string, msg *easypubsub.Message) ([]byte, error) {
		return msg.Body(), nil
	}
	publisher := iopublisher.New(
		targetFile,
		iopublisher.WithMarshalMsgFunc(marshalMsg),
	)
	i := 0
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	channel := easypubsubpipe.New(
		easypubsubpipe.NewSource("", subscriber),
		easypubsubpipe.NewSink("", publisher),
		func(msg *easypubsub.Message) error {
			ackResp := msg.Ack()
			i++
			fmt.Println(ackResp, i)
			return nil
		},
		easypubsubpipe.WithInterceptors(pipemiddleware.CustomRecovery(func(p interface{}) (err error) {
			fmt.Println("Recovery: ", p)
			i++
			return errors.New(fmt.Sprint(p))
		}), func(msg *easypubsub.Message, handler easypubsubpipe.MessageHandler) error {
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
		fmt.Println(err)
	}
	err = channel.Close()
	if err != nil {
		panic(err)
	}
}
