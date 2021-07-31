package amqpsubscriber

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/soyacen/goutils/errorutils"
	"github.com/streadway/amqp"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	o         *options
	url       string
	topic     string
	closed    int32
	closeC    chan struct{}
	msgC      chan *easypubsub.Message
	errC      chan error
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
	deliveryC <-chan amqp.Delivery
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

func (sub *Subscriber) Close() error {
	if atomic.CompareAndSwapInt32(&sub.closed, NORMAL, CLOSED) {
		close(sub.closeC)
		var err error
		if sub.channel != nil {
			if e := sub.channel.Close(); e != nil {
				err = multierror.Append(err, fmt.Errorf("failed close channel, %w", e))
			}
		}
		if sub.conn != nil {
			if e := sub.conn.Close(); e != nil {
				err = multierror.Append(err, fmt.Errorf("failed close channel, %w", e))
			}
		}
		return err
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "AMQPSubscriber"
}

func (sub *Subscriber) subscribe(ctx context.Context) error {
	if err := sub.openConnection(); err != nil {
		return err
	}
	if err := sub.openChannel(); err != nil {
		return err
	}
	if err := sub.consume(); err != nil {
		return err
	}
	go errorutils.WithRecover(sub.recoverHandler, sub.stream(ctx))
	go errorutils.WithRecover(sub.recoverHandler, sub.consumeDaemon(ctx))
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
	return nil
}

func (sub *Subscriber) openChannel() error {
	sub.o.logger.Log("open amqp channel")
	channel, err := sub.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel, %w", err)
	}
	sub.channel = channel

	qosConfig := sub.o.qosConfig
	if qosConfig != nil {
		err := sub.channel.Qos(qosConfig.PrefetchCount, qosConfig.PrefetchSize, qosConfig.Global)
		if err != nil {
			return fmt.Errorf("failed to set QoS, %w", err)
		}
	}

	exchangeName := sub.o.exchange.NameFunc(sub.topic)
	sub.o.logger.Logf("declare exchange %s, kind is %s", exchangeName, sub.o.exchange.Kind)
	err = sub.channel.ExchangeDeclare(
		exchangeName,
		sub.o.exchange.Kind,
		sub.o.exchange.Durable,
		sub.o.exchange.AutoDelete,
		sub.o.exchange.Internal,
		sub.o.exchange.NoWait,
		sub.o.exchange.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange, %w", err)
	}

	sub.queueName = sub.o.queue.NameFunc(sub.topic)
	sub.o.logger.Logf("declare queue %s", sub.queueName)
	queue, err := sub.channel.QueueDeclare(
		sub.queueName,
		sub.o.queue.Durable,
		sub.o.queue.AutoDelete,
		sub.o.queue.Exclusive,
		sub.o.queue.NoWait,
		sub.o.queue.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare an queue, %w", err)
	}
	sub.queueName = queue.Name
	sub.o.logger.Logf("actual queue name is %s", sub.queueName)

	for _, queueBind := range sub.o.queueBinds {
		sub.o.logger.Logf("bind queue %s to exchange %s with key %s", sub.queueName, exchangeName, queueBind.Key)
		err = sub.channel.QueueBind(
			sub.queueName,
			queueBind.Key,
			exchangeName,
			queueBind.NoWait,
			queueBind.Args,
		)
		if err != nil {
			return fmt.Errorf("failed to bind a queue, %w", err)
		}
	}
	return nil
}

func (sub *Subscriber) consume() error {
	sub.o.logger.Logf("consumer %s start consume queue %s", sub.o.consume.Consumer, sub.queueName)
	deliveryC, err := sub.channel.Consume(
		sub.queueName,
		sub.o.consume.Consumer,
		false,
		sub.o.consume.Exclusive,
		sub.o.consume.NoLocal,
		sub.o.consume.NoWait,
		sub.o.consume.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to consume queue %s, %w", sub.queueName, err)
	}
	sub.deliveryC = deliveryC
	return nil
}

func (sub *Subscriber) stream(ctx context.Context) func() {
	return func() {
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping handle message")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping handle message")
				return
			case amqpMsg, ok := <-sub.deliveryC:
				if !ok {
					sub.o.logger.Log("delivery channel is closed, stopping handle message")
					return
				}
				sub.handlerMsg(ctx, &amqpMsg)
			}
		}

	}
}

func (sub *Subscriber) consumeDaemon(ctx context.Context) func() {
	return func() {
		sub.o.logger.Log("consume daemon")
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping subscribe")
				go sub.closeErrCAndMsgC(nil)
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping subscribe")
				go sub.closeErrCAndMsgC(nil)
				return
			case err := <-sub.conn.NotifyClose(make(chan *amqp.Error, 1)):
				if err == nil {
					sub.o.logger.Log("shut down connection, stopping subscribe")
					go sub.closeErrCAndMsgC(nil)
					return
				}
				sub.errC <- fmt.Errorf("received a exception from amqp broker, %w", err)
				sub.o.logger.Logf("connection is closed with error %v", err)
				for i := 0; ; i++ {
					reconnectInterval := sub.o.reconnectBackoff(ctx, uint(i))
					sub.o.logger.Logf("wait %s to reconnect to amqp", reconnectInterval)
					time.Sleep(reconnectInterval)
					if err := sub.openConnection(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					if err := sub.openChannel(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					if err := sub.consume(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					go errorutils.WithRecover(sub.recoverHandler, sub.stream(ctx))
					break
				}
			case err := <-sub.channel.NotifyClose(make(chan *amqp.Error, 1)):
				if err == nil {
					sub.o.logger.Log("shut down channel, stopping subscribe")
					go sub.closeErrCAndMsgC(nil)
					return
				}
				sub.errC <- fmt.Errorf("received a exception from amqp broker, %w", err)
				sub.o.logger.Logf("channel is closed with error %v", err)
				for i := 0; ; i++ {
					reconnectInterval := sub.o.reconnectBackoff(ctx, uint(i))
					sub.o.logger.Logf("wait %s to reconnect to amqp", reconnectInterval)
					time.Sleep(reconnectInterval)
					if err := sub.openChannel(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					if err := sub.consume(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					go errorutils.WithRecover(sub.recoverHandler, sub.stream(ctx))
					break
				}
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
	sub.o.logger.Log("close msg and err chan")
	if err != nil {
		sub.errC <- err
	}
	close(sub.msgC)
	close(sub.errC)
}

func New(url string, opts ...Option) easypubsub.Subscriber {
	o := defaultOptions()
	o.apply(opts...)
	sub := &Subscriber{url: url, o: o, closed: NORMAL, closeC: make(chan struct{})}
	return sub
}
