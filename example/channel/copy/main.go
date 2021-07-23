package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	easypubsub "github.com/soyacen/pubsub"
	iopublisher "github.com/soyacen/pubsub/io/publisher"
	iosubscriber "github.com/soyacen/pubsub/io/subscriber"
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

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	channel := easypubsub.NewChannel(
		easypubsub.NewSource("", subscriber),
		easypubsub.NewSink("", publisher),
		easypubsub.WithFilter(func(inMsg *easypubsub.Message) (outMsg *easypubsub.Message, err error) {
			if rand.Int()%3 == 0 {
				inMsg.Nack()
				return nil, errors.New("this is zero error")
			}
			inMsg.Ack()
			return inMsg, nil
		}),
		easypubsub.WithFilter(func(inMsg *easypubsub.Message) (outMsg *easypubsub.Message, err error) {
			if rand.Int()%3 == 1 {
				inMsg.Nack()
				return nil, errors.New("this is one error")
			}
			inMsg.Ack()
			return inMsg, nil
		}),
		easypubsub.WithFilter(func(inMsg *easypubsub.Message) (outMsg *easypubsub.Message, err error) {
			if rand.Int()%3 == 1 {
				inMsg.Nack()
				return nil, errors.New("this is two error")
			}
			inMsg.Ack()
			return inMsg, nil
		}),
		easypubsub.WithLogger(easypubsub.NewStdLogger(os.Stdout)),
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 2000*time.Millisecond)
	defer cancelFunc()
	err = channel.Flow(ctx)
	if err != nil {
		panic(err)
	}
	for err := range channel.Errors() {
		fmt.Println(err)
	}
	channel.Close()
}
