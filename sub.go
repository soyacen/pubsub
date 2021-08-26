package easypubsub

import (
	"context"
	"fmt"
	"io"
)

type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (msgC <-chan *Message, errC <-chan error)
	io.Closer
	fmt.Stringer
}

type NopSubscriber struct{}

func (sub *NopSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *Message, <-chan error) {
	return nil, nil
}

func (sub *NopSubscriber) Close() error {
	return nil
}

func (sub *NopSubscriber) String() string {
	return "NopSubscriber"
}
