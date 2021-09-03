package amqppublisher

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/streadway/amqp"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type PublishResult struct {
	Partition int32
	Offset    int64
}

type Publisher struct {
	connO   *connectionOptions
	o       *options
	close   int32
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) *easypubsub.PublishResult {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return &easypubsub.PublishResult{Err: errors.New("publisher is closed")}
	}
	amqpMsg, err := pub.o.marshalMsgFunc(topic, msg, pub.o.msgProps)
	if err != nil {
		return &easypubsub.PublishResult{Err: fmt.Errorf("failed marsharl msg, %w", err)}
	}

	pub.o.logger.Logf("send message %s", msg.Id())
	if pub.o.transactional {
		if err := pub.publishWithTransaction(topic, amqpMsg); err != nil {
			return &easypubsub.PublishResult{Err: err}
		}
		return &easypubsub.PublishResult{Result: "ok"}
	}

	if err := pub.channel.Publish(
		pub.o.exchange.Name, pub.o.publish.RoutingKeyFunc(topic),
		pub.o.publish.Mandatory, pub.o.publish.Immediate, *amqpMsg); err != nil {
		return &easypubsub.PublishResult{Err: fmt.Errorf("failed send message, %w", err)}
	}
	return &easypubsub.PublishResult{Result: "ok"}
}

func (pub *Publisher) publishWithTransaction(topic string, amqpMsg *amqp.Publishing) error {
	if err := pub.channel.Tx(); err != nil {
		return fmt.Errorf("failed start transaction, %w", err)
	}
	if err := pub.channel.Publish(pub.o.exchange.Name, topic,
		pub.o.publish.Mandatory, pub.o.publish.Immediate, *amqpMsg); err != nil {
		if e := pub.channel.TxRollback(); e != nil {
			return multierror.Append(nil,
				fmt.Errorf("failed send message, %w", err),
				fmt.Errorf("failed rollback transaction, %w", e))
		}
		return fmt.Errorf("failed send message, %w", err)
	}
	if err := pub.channel.TxCommit(); err != nil {
		return fmt.Errorf("cannot start transaction, %w", err)
	}
	return nil
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		var err error
		if e := pub.channel.Close(); e != nil {
			err = multierror.Append(err, e)
		}
		if e := pub.conn.Close(); e != nil {
			err = multierror.Append(err, e)
		}
		return err
	}
	return nil
}

func (pub *Publisher) String() string {
	return "amqpPublisher"
}

func (pub *Publisher) openConnection() error {

	return nil
}

func New(connOpt ConnectionOption, opts ...Option) (easypubsub.Publisher, error) {
	connO := &connectionOptions{}
	connOpt(connO)
	o := defaultOptions()
	o.apply(opts...)
	pub := &Publisher{o: o, connO: connO, close: NORMAL}

	pub.o.logger.Logf("dial amqp broker %s", pub.connO.url)
	switch pub.connO.connectionType {
	default:
		return nil, fmt.Errorf("unknown amqp connection type %d", pub.connO.connectionType)
	case normalConnectionType:
		conn, err := amqp.Dial(pub.connO.url)
		if err != nil {
			return nil, fmt.Errorf("failed dial %s, %w", pub.connO.url, err)
		}
		pub.conn = conn
	case tlsConnectionType:
		conn, err := amqp.DialTLS(pub.connO.url, pub.connO.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed dial %s with tls %v, %w", pub.connO.url, pub.connO.tlsConfig, err)
		}
		pub.conn = conn
	case amqpConnectionType:
		conn, err := amqp.DialConfig(pub.connO.url, *pub.connO.amqpConfig)
		if err != nil {
			return nil, fmt.Errorf("failed dial %s with config %v, %w", pub.connO.url, pub.connO.amqpConfig, err)
		}
		pub.conn = conn
	}

	pub.o.logger.Log("open amqp channel")
	channel, err := pub.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed open channel, %w", err)
	}
	pub.channel = channel

	pub.o.logger.Logf("declare exchange %s, kind is %s", pub.o.exchange.Name, pub.o.exchange.Kind)
	err = channel.ExchangeDeclare(
		pub.o.exchange.Name,       // name
		pub.o.exchange.Kind,       // type
		pub.o.exchange.Durable,    // durable
		pub.o.exchange.AutoDelete, // auto-deleted
		pub.o.exchange.Internal,   // internal
		pub.o.exchange.NoWait,     // no-wait
		pub.o.exchange.Args,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed declare exchange, %w", err)
	}
	return pub, nil
}
