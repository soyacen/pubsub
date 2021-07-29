package easypubsub

import (
	"context"
	"fmt"
	"io"
)

type (
	Subscriber interface {
		Subscribe(ctx context.Context, topic string) (<-chan *Message, <-chan error)
		io.Closer
		fmt.Stringer
	}
)

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
