package channel

import (
	"context"
	"errors"
	"time"

	"github.com/soyacen/easypubsub"
)

type subOptions struct {
	logger                  easypubsub.Logger
	nackResendSleepDuration time.Duration
}

func (o *subOptions) apply(opts ...SubOption) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultSubOptions() *subOptions {
	return &subOptions{
		logger:                  easypubsub.DefaultLogger(),
		nackResendSleepDuration: 5 * time.Second,
	}
}

type SubOption func(o *subOptions)

func WithLogger(logger easypubsub.Logger) SubOption {
	return func(o *subOptions) {
		o.logger = logger
	}
}

func WithNackResendSleepDuration(interval time.Duration) SubOption {
	return func(o *subOptions) {
		o.nackResendSleepDuration = interval
	}
}

type Subscriber struct {
	o      *subOptions
	holder *PubSub
	inputC chan *easypubsub.Message
	closeC chan struct{}
	topic  string
	errC   chan error
	msgC   chan *easypubsub.Message
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *easypubsub.Message, <-chan error) {
	sub.errC = make(chan error)
	sub.msgC = make(chan *easypubsub.Message)
	select {
	case <-sub.closeC:
		go sub.closeErrCAndMsgC(errors.New("subscriber is closed"))
		return sub.msgC, sub.errC
	default:
	}
	sub.topic = topic
	go sub.subscribe(ctx)
	return sub.msgC, sub.errC
}

func (sub *Subscriber) Close() error {
	select {
	case <-sub.closeC:
	default:
		close(sub.closeC)
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "ChanSubscriber"
}

func (sub *Subscriber) subscribe(ctx context.Context) {
	sub.o.logger.Logf("start subscribe")
	defer sub.closeErrCAndMsgC(nil)
	sub.inputC = make(chan *easypubsub.Message)
	sub.holder.RegisterMessageChannel(sub.topic, sub.inputC)
	defer sub.holder.DeregisterMessageChannel(sub.topic, sub.inputC)
	for {
		select {
		case <-sub.closeC:
			close(sub.inputC)
			sub.o.logger.Log("subscriber is closing, stopping subscribe")
		case <-ctx.Done():
			close(sub.inputC)
			sub.o.logger.Log("context is Done, stopping subscribe")
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

func NewSubscriber(holder *PubSub, opts ...SubOption) easypubsub.Subscriber {
	o := defaultSubOptions()
	o.apply(opts...)
	sub := &Subscriber{o: o, holder: holder, closeC: make(chan struct{})}
	return sub
}
