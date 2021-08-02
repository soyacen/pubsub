package easypubsub

import (
	"fmt"
	"io"
)

type (
	PublishResult struct {
		Err    error
		Result interface{}
	}

	Publisher interface {
		Publish(topic string, msg *Message) (result *PublishResult)
		io.Closer
		fmt.Stringer
	}
)

type NopPublisher struct{}

func (pub *NopPublisher) Publish(topic string, msg *Message) (result *PublishResult) {
	return &PublishResult{}
}

func (pub *NopPublisher) Close() error {
	return nil
}

func (pub *NopPublisher) String() string {
	return "NopPublisher"
}
