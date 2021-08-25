package chansubscriber

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	o      *options
	closed int32
	closeC chan struct{}
	inputC <-chan *easypubsub.Message
	topic  string
	errC   chan error
	msgC   chan *easypubsub.Message
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *easypubsub.Message, <-chan error) {
	sub.errC = make(chan error)
	sub.msgC = make(chan *easypubsub.Message)
	if atomic.LoadInt32(&sub.closed) == CLOSED {
		go sub.closeErrCAndMsgC(errors.New("subscriber is closed"))
		return sub.msgC, sub.errC
	}
	sub.topic = topic
	go sub.subscribe(ctx)
	return sub.msgC, sub.errC
}

func (sub *Subscriber) Close() error {
	if atomic.CompareAndSwapInt32(&sub.closed, NORMAL, CLOSED) {
		close(sub.closeC)
		return nil
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "IOSubscriber"
}

func (sub *Subscriber) subscribe(ctx context.Context) {
	defer sub.closeErrCAndMsgC(nil)
	sub.o.logger.Logf("start subscribe")
	for {
		select {
		case <-sub.closeC:
			sub.o.logger.Log("subscriber is closing, stopping subscribe")
			return
		case <-ctx.Done():
			sub.o.logger.Log("context is Done, stopping subscribe")
			return
		default:
			msg, ok := <-sub.inputC
			if !ok {
				sub.o.logger.Log("go channel is closed, stopping subscribe")
				return
			}
		HandleMsg:
			msg.Responder = easypubsub.NewResponder()
			sub.msgC <- msg
			sub.o.logger.Logf("message %s sent, wait ack...", msg.Id())
			select {
			case <-msg.Acked():
				msg.AckResp() <- &easypubsub.Response{Result: "ok"}
				sub.o.logger.Logf("message %s acked", msg.Id())
			case <-msg.Nacked():
				msg.NackResp() <- &easypubsub.Response{Result: "ok"}
				sub.o.logger.Logf("message %s nacked", msg.Id())
				if sub.o.nackResendSleepDuration > 0 {
					time.Sleep(sub.o.nackResendSleepDuration)
					sub.o.logger.Logf("message %s will resend", msg.Id())
				}
				goto HandleMsg
			}
		}
	}
}

func (sub *Subscriber) closeErrCAndMsgC(err error) {
	if err != nil {
		sub.errC <- err
	}
	close(sub.errC)
	close(sub.msgC)
}

func New(inputC <-chan *easypubsub.Message, opts ...Option) easypubsub.Subscriber {
	o := defaultOptions()
	o.apply(opts...)
	sub := &Subscriber{o: o, inputC: inputC, closed: NORMAL, closeC: make(chan struct{})}
	return sub
}
