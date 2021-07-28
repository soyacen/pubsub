package amqppublisher

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/streadway/amqp"

	"github.com/soyacen/goutils/stringutils"

	easypubsub "github.com/soyacen/pubsub"
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
	o       *options
	url     string
	close   int32
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) *easypubsub.PublishResult {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return &easypubsub.PublishResult{Err: errors.New("publisher is closed")}
	}
	amqpMsg, err := pub.o.marshalMsgFunc(topic, msg)
	if err != nil {
		return &easypubsub.PublishResult{Err: fmt.Errorf("failed marsharl msg, %w", err)}
	}
	if stringutils.IsNotBlank(pub.o.contentType) {
		amqpMsg.ContentType = pub.o.contentType
	}
	if stringutils.IsNotBlank(pub.o.contentEncoding) {
		amqpMsg.ContentEncoding = pub.o.contentEncoding
	}
	if pub.o.persistentDeliveryMode {
		amqpMsg.DeliveryMode = amqp.Persistent
	} else {
		amqpMsg.DeliveryMode = amqp.Transient
	}

	pub.o.logger.Logf("send message %s", msg.Id())
	if pub.o.transactional {
		if err := pub.publishWithTransaction(topic, amqpMsg); err != nil {
			return &easypubsub.PublishResult{Err: err}
		}
		return &easypubsub.PublishResult{Result: "ok"}
	}

	if err := pub.channel.Publish(
		pub.o.exchangeName, topic,
		pub.o.publishMandatory, pub.o.publishImmediate, *amqpMsg); err != nil {
		return &easypubsub.PublishResult{Err: fmt.Errorf("failed send message, %w", err)}
	}
	return &easypubsub.PublishResult{Result: "ok"}
}

func (pub *Publisher) publishWithTransaction(topic string, amqpMsg *amqp.Publishing) error {
	if err := pub.channel.Tx(); err != nil {
		return fmt.Errorf("failed start transaction, %w", err)
	}
	if err := pub.channel.Publish(pub.o.exchangeName, topic,
		pub.o.publishMandatory, pub.o.publishImmediate, *amqpMsg); err != nil {
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
	if pub.o.amqpConfig != nil {
		conn, err := amqp.DialConfig(pub.url, *pub.o.amqpConfig)
		if err != nil {
			return fmt.Errorf("failed dial %s config %v, %w", pub.url, pub.o.amqpConfig, err)
		}
		pub.conn = conn
	} else if pub.o.tlsConfig != nil {
		conn, err := amqp.DialTLS(pub.url, pub.o.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed dial %s tlsConfig %v, %w", pub.url, pub.o.amqpConfig, err)
		}
		pub.conn = conn
	} else {
		conn, err := amqp.Dial(pub.url)
		if err != nil {
			return fmt.Errorf("failed dial %s tlsConfig %v, %w", pub.url, pub.o.amqpConfig, err)
		}
		pub.conn = conn
	}
	return nil
}

func New(url string, opts ...Option) (easypubsub.Publisher, error) {
	o := defaultOptions()
	o.apply(opts...)
	pub := &Publisher{o: o, url: url, close: NORMAL}
	if err := pub.openConnection(); err != nil {
		return nil, err
	}
	channel, err := pub.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed open channel, %w", err)
	}
	pub.channel = channel

	err = channel.ExchangeDeclare(
		pub.o.exchangeName,       // name
		pub.o.exchangeKind,       // type
		pub.o.exchangeDurable,    // durable
		pub.o.exchangeAutoDelete, // auto-deleted
		pub.o.exchangeInternal,   // internal
		pub.o.exchangeNoWait,     // no-wait
		pub.o.exchangeArgs,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed declare exchange, %w", err)
	}
	return pub, nil
}
