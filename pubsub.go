package easypubsub

import (
	"context"
	"fmt"
	"io"
)

type (
	Interceptor func(topic string, msg *Message) error

	PublishResult struct {
		Err    error
		Result interface{}
	}

	Publisher interface {
		AddInterceptor(interceptors ...Interceptor)
		Publish(topic string, msg *Message) (result PublishResult)
		io.Closer
		fmt.Stringer
	}

	Subscriber interface {
		AddInterceptor(interceptors ...Interceptor)
		Subscribe(ctx context.Context, topic string) (err error)
		Messages() (msgC <-chan *Message)
		Errors() (errC <-chan error)
		io.Closer
		fmt.Stringer
	}
)

type nopPublisher struct{}

func (n *nopPublisher) AddInterceptor(interceptors ...Interceptor) {}

func (n *nopPublisher) Publish(topic string, msg *Message) (result PublishResult) {
	return PublishResult{}
}

func (n *nopPublisher) Close() error {
	return nil
}

func (n *nopPublisher) String() string {
	return "nopPublisher"
}

type nopSubscriber struct{}

func (n *nopSubscriber) AddInterceptor(interceptors ...Interceptor) {}

func (n *nopSubscriber) Subscribe(ctx context.Context, topic string) (err error) {
	return nil
}

func (n *nopSubscriber) Messages() (msgC <-chan *Message) {
	return nil
}

func (n *nopSubscriber) Errors() (errC <-chan error) {
	return nil
}

func (n *nopSubscriber) Close() error {
	return nil
}

func (n *nopSubscriber) String() string {
	return "nopSubscriber"
}
