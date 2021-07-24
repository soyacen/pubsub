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

type nopPublisher struct{}

func (n *nopPublisher) AddInterceptor(interceptors ...Interceptor) {}

func (n *nopPublisher) Publish(topic string, msg *Message) (result *PublishResult) {
	return &PublishResult{}
}

func (n *nopPublisher) Close() error {
	return nil
}

func (n *nopPublisher) String() string {
	return "nopPublisher"
}
