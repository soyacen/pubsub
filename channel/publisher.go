package channel

import (
	"errors"
	"sync/atomic"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Publisher struct {
	holder *PubSub
	close  int32
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) (result *easypubsub.PublishResult) {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return &easypubsub.PublishResult{Err: errors.New("publisher is closed")}
	}
	msgC := make(chan *easypubsub.Message)
	pub.holder.SendMessageChannel(topic, msgC)
	msgC <- msg
	return &easypubsub.PublishResult{Result: "ok"}
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		return nil
	}
	return nil
}

func (pub *Publisher) String() string {
	return "ChannelPublisher"
}

func NewPublisher(holder *PubSub) easypubsub.Publisher {
	pub := &Publisher{holder: holder, close: NORMAL}
	return pub
}
