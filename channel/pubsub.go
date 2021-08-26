package channel

import (
	"fmt"
	"time"

	"github.com/soyacen/easypubsub"
)

type sendMsgCBinder struct {
	topic string
	msgC  <-chan *easypubsub.Message
}

type receiveMsgCBinder struct {
	topic string
	msgC  chan<- *easypubsub.Message
}

type PubSub struct {
	closeC      chan struct{}
	deregisterC chan *receiveMsgCBinder
	registerC   chan *receiveMsgCBinder
	sendC       chan *sendMsgCBinder
}

func NewPubSub() *PubSub {
	return &PubSub{
		closeC:      make(chan struct{}),
		deregisterC: make(chan *receiveMsgCBinder),
		registerC:   make(chan *receiveMsgCBinder),
		sendC:       make(chan *sendMsgCBinder),
	}
}

func (holder *PubSub) Start() {
	go holder.Run()
}

func (holder *PubSub) Run() {
	topicChannelMap := make(map[string][]chan<- *easypubsub.Message)
	for {
		select {
		case <-holder.closeC:
			return
		case binder := <-holder.registerC:
			fmt.Println("register1")
			topicChannelMap[binder.topic] = append(topicChannelMap[binder.topic], binder.msgC)
			fmt.Println("register")
		case binder := <-holder.deregisterC:
			topicChannelMap[binder.topic] = append(topicChannelMap[binder.topic], binder.msgC)
			fmt.Println("deregister")
		case binder := <-holder.sendC:
			msg := <-binder.msgC
			for _, msgC := range topicChannelMap[binder.topic] {
				msgC <- msg.Clone()
			}
		}
	}
	fmt.Println("exit...")
}

func (holder *PubSub) Close() {
	select {
	case <-holder.closeC:
	default:
		close(holder.closeC)
	}
}

func (holder *PubSub) SendMessageChannel(topic string, msgC <-chan *easypubsub.Message) {
	select {
	case <-holder.closeC:
	default:
		holder.sendC <- &sendMsgCBinder{msgC: msgC, topic: topic}
	}
}

func (holder *PubSub) RegisterMessageChannel(topic string, msgC chan<- *easypubsub.Message) {
	select {
	case <-holder.closeC:
	default:
		holder.registerC <- &receiveMsgCBinder{msgC: msgC, topic: topic}
		fmt.Println("RegisterMessageChannel")
	}
}

func (holder *PubSub) DeregisterMessageChannel(topic string, msgC chan<- *easypubsub.Message) {
	select {
	case <-holder.closeC:
	default:
		holder.deregisterC <- &receiveMsgCBinder{msgC: msgC, topic: topic}
	}
}

func (holder *PubSub) Subscriber(logger easypubsub.Logger, nackResendSleepDuration time.Duration) easypubsub.Subscriber {
	return NewSubscriber(holder, WithLogger(logger), WithNackResendSleepDuration(nackResendSleepDuration))
}

func (holder *PubSub) Publisher() easypubsub.Publisher {
	return NewPublisher(holder)
}
