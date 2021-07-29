package amqpsubscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/soyacen/goutils/errorutils"
	"github.com/streadway/amqp"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	o            *options
	url          string
	topic        string
	closed       int32
	closeC       chan struct{}
	msgC         chan *easypubsub.Message
	errC         chan error
	conn         *amqp.Connection
	channel      *amqp.Channel
	notifyCloseC chan *amqp.Error
	deliveryC    <-chan amqp.Delivery
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *easypubsub.Message, <-chan error) {
	sub.errC = make(chan error)
	sub.msgC = make(chan *easypubsub.Message)
	if atomic.LoadInt32(&sub.closed) == CLOSED {
		go sub.closeErrCAndMsgC(errors.New("subscriber is closed"))
		return sub.msgC, sub.errC
	}
	sub.topic = topic
	if err := sub.subscribe(ctx); err != nil {
		go sub.closeErrCAndMsgC(err)
	}
	return sub.msgC, sub.errC

}

func (sub *Subscriber) Messages() (msgC <-chan *easypubsub.Message) {
	return sub.msgC
}

func (sub *Subscriber) Errors() (errC <-chan error) {
	return sub.errC
}

func (sub *Subscriber) Close() error {
	if atomic.CompareAndSwapInt32(&sub.closed, NORMAL, CLOSED) {
		close(sub.closeC)
		err := sub.conn.Close()
		if err != nil {
			return fmt.Errorf("failed close amqp connection, %w", err)
		}
		return nil
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "AMQPSubscriber"
}

// ========================  consumer consume  ========================
func (sub *Subscriber) subscribe(ctx context.Context) error {
	if err := sub.openConnection(); err != nil {
		return err
	}
	if err := sub.prepareConsume(); err != nil {
		return err
	}
	exitC := sub.startConsume(ctx)
	go func(exitC <-chan struct{}) {
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping subscribe")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping subscribe")
				return
			case <-exitC:
				if err := sub.openConnection(); err != nil {
					sub.o.logger.Log(err)
					return
				}
				if err := sub.prepareConsume(); err != nil {
					sub.o.logger.Log(err)
					return
				}
				exitC = sub.startConsume(ctx)
			}
		}
	}(exitC)
	return nil
}

func (sub *Subscriber) openConnection() error {
	sub.o.logger.Logf("dial amqp broker %s", sub.url)
	if sub.o.amqpConfig != nil {
		conn, err := amqp.DialConfig(sub.url, *sub.o.amqpConfig)
		if err != nil {
			return fmt.Errorf("failed dial %s with amqpConfig %v, %w", sub.url, sub.o.amqpConfig, err)
		}
		sub.conn = conn
	} else if sub.o.tlsConfig != nil {
		conn, err := amqp.DialTLS(sub.url, sub.o.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed dial %s with tlsConfig %v, %w", sub.url, sub.o.tlsConfig, err)
		}
		sub.conn = conn
	} else {
		conn, err := amqp.Dial(sub.url)
		if err != nil {
			return fmt.Errorf("failed dial broker %s, %w", sub.url, err)
		}
		sub.conn = conn
	}
	sub.o.logger.Log("open amqp channel")
	channel, err := sub.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel, %w", err)
	}
	sub.channel = channel

	qosConfig := sub.o.qosConfig
	if qosConfig != nil {
		if err := sub.channel.Qos(qosConfig.PrefetchCount, qosConfig.PrefetchSize, qosConfig.Global); err != nil {
			return fmt.Errorf("failed to set QoS, %w", err)
		}
	}
	return nil
}

func (sub *Subscriber) prepareConsume() error {
	sub.o.logger.Log("declare exchange")
	exchangeName := sub.o.exchange.NameFunc(sub.topic)
	if err := sub.channel.ExchangeDeclare(
		exchangeName, sub.o.exchange.Kind, sub.o.exchange.Durable,
		sub.o.exchange.AutoDelete, sub.o.exchange.Internal, sub.o.exchange.NoWait,
		sub.o.exchange.Args); err != nil {
		return fmt.Errorf("failed to declare an exchange, %w", err)
	}

	sub.o.logger.Log("declare queue")
	queueName := sub.o.queue.NameFunc(sub.topic)
	queue, err := sub.channel.QueueDeclare(
		queueName,
		sub.o.queue.Durable,
		sub.o.queue.AutoDelete,
		sub.o.queue.Exclusive,
		sub.o.queue.NoWait,
		sub.o.queue.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare an queue, %w", err)
	}
	queueName = queue.Name

	for _, queueBind := range sub.o.queueBinds {
		sub.o.logger.Logf("bind queue %s to exchange %s with key %s", queueName, exchangeName, queueBind.Key)
		err = sub.channel.QueueBind(
			queueName,
			queueBind.Key,
			exchangeName,
			queueBind.NoWait,
			queueBind.Args,
		)
		if err != nil {
			return fmt.Errorf("failed to bind a queue, %w", err)
		}
	}
	sub.notifyCloseC = sub.channel.NotifyClose(make(chan *amqp.Error))

	sub.o.logger.Logf("prepare consume queue %s", queueName)
	deliveryC, err := sub.channel.Consume(
		queueName,
		sub.o.consume.Consumer,
		false,
		sub.o.consume.Exclusive,
		sub.o.consume.NoLocal,
		sub.o.consume.NoWait,
		sub.o.consume.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to consume queue %s, %w", queueName, err)
	}
	sub.deliveryC = deliveryC
	return nil
}

func (sub *Subscriber) startConsume(ctx context.Context) <-chan struct{} {
	sub.o.logger.Log("start consume")
	var wg sync.WaitGroup
	sub.msgC = make(chan *easypubsub.Message)
	sub.errC = make(chan error)
	wg.Add(1)
	go errorutils.WithRecover(sub.recoverHandler, sub.handleMessages(ctx, &wg))
	exit := make(chan struct{})
	go func() {
		wg.Wait()
		close(sub.msgC)
		close(sub.errC)
		sub.o.logger.Log("close message and error chan")
		close(exit)
	}()
	return exit
}

func (sub *Subscriber) handleMessages(ctx context.Context, wg *sync.WaitGroup) func() {
	return func() {
		defer wg.Done()
		defer func() {
			sub.o.logger.Log("close channel")
			if err := sub.channel.Close(); err != nil {
				sub.errC <- fmt.Errorf("failed close channel, %w", err)
			}
		}()

		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping handle message")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping handle message")
				return
			case <-sub.notifyCloseC:
				sub.o.logger.Logf("channel closed, stopping handle message")
				return
			case amqpMsg, ok := <-sub.deliveryC:
				if !ok {
					sub.o.logger.Log("partition consumer message's channel is closed, stopping partition consumer")
					return
				}
				sub.handlerMsg(ctx, &amqpMsg)
			}
		}

	}
}

func (sub *Subscriber) handlerMsg(ctx context.Context, amqpMsg *amqp.Delivery) {
	msg, err := sub.o.unmarshalMsgFunc(ctx, sub.topic, amqpMsg)
	if err != nil {
		sub.errC <- err
		return
	}

	msg.Responder = easypubsub.NewResponder()
	// send msg to msg chan
	sub.msgC <- msg
	sub.o.logger.Logf("message %s sent to channel, wait ack...", msg.Id())
	select {
	case <-msg.Acked():
		if err := amqpMsg.Ack(false); err != nil {
			msg.AckResp() <- &easypubsub.Response{Err: err}
		} else {
			msg.AckResp() <- &easypubsub.Response{Result: "ok"}
		}

		sub.o.logger.Logf("message %s acked", msg.Id())
		return
	case <-msg.Nacked():
		if err := amqpMsg.Nack(false, sub.o.requeueOnNack); err != nil {
			msg.NackResp() <- &easypubsub.Response{Err: err}
		} else {
			msg.NackResp() <- &easypubsub.Response{Result: "ok"}
		}
		sub.o.logger.Logf("message %s nacked", msg.Id())
	}
}

func (sub *Subscriber) recoverHandler(p interface{}) {
	sub.o.logger.Logf("recover panic %v", p)
}

func (sub *Subscriber) closeErrCAndMsgC(err error) {
	if err != nil {
		sub.errC <- err
	}
	close(sub.errC)
	close(sub.msgC)
}

func New(url string, opts ...Option) easypubsub.Subscriber {
	o := defaultOptions()
	o.apply(opts...)
	sub := &Subscriber{url: url, o: o, close: NORMAL, closeC: make(chan struct{})}
	return sub
}
