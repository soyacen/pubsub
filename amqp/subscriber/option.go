package amqpsubscriber

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/streadway/amqp"

	"github.com/soyacen/easypubsub"
)

type UnmarshalMsgFunc func(ctx context.Context, topic string, amqpMsg *amqp.Delivery) (msg *easypubsub.Message, err error)

type QosConfig struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type Exchange struct {
	NameFunc   func(topic string) string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

type Queue struct {
	NameFunc   func(topic string) string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

type QueueBind struct {
	Key    string
	NoWait bool
	Args   map[string]interface{}
}

type Consume struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      map[string]interface{}
}

type options struct {
	logger           easypubsub.Logger
	unmarshalMsgFunc UnmarshalMsgFunc
	requeueOnNack    bool
	tlsConfig        *tls.Config
	amqpConfig       *amqp.Config
	qosConfig        *QosConfig
	exchange         *Exchange
	queue            *Queue
	queueBinds       []*QueueBind
	consume          *Consume
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger:           easypubsub.DefaultLogger(),
		unmarshalMsgFunc: DefaultUnmarshalMsgFunc,
		requeueOnNack:    true,
		exchange: &Exchange{
			NameFunc: func(topic string) string {
				return topic
			},
			Kind:       "topic",
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       nil,
		},
		queue: &Queue{
			NameFunc: func(topic string) string {
				return topic
			},
			Durable:    true,
			AutoDelete: false,
			Exclusive:  true,
			NoWait:     false,
			Args:       nil,
		},
		consume: &Consume{
			Consumer:  "",
			AutoAck:   false,
			Exclusive: true,
			NoLocal:   false,
			NoWait:    false,
			Args:      nil,
		},
	}
}

type Option func(o *options)

func WithUnmarshalMsgFunc(unmarshalMsgFunc UnmarshalMsgFunc) Option {
	return func(o *options) {
		o.unmarshalMsgFunc = unmarshalMsgFunc
	}
}

func WithLogger(logger easypubsub.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithRequeueOnNack(enable bool) Option {
	return func(o *options) {
		o.requeueOnNack = enable
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = config
	}
}

func WithAMQPConfig(config *amqp.Config) Option {
	return func(o *options) {
		o.amqpConfig = config
	}
}

func WithQosConfig(qosConfig *QosConfig) Option {
	return func(o *options) {
		o.qosConfig = qosConfig
	}
}

func WithExchange(exchange *Exchange) Option {
	return func(o *options) {
		o.exchange = exchange
	}
}

func WithQueue(queue *Queue) Option {
	return func(o *options) {
		o.queue = queue
	}
}

func WithQueueBinds(queueBinds ...*QueueBind) Option {
	return func(o *options) {
		o.queueBinds = append(o.queueBinds, queueBinds...)
	}
}

func WithConsume(consume *Consume) Option {
	return func(o *options) {
		o.consume = consume
	}
}

func DefaultUnmarshalMsgFunc(ctx context.Context, topic string, amqpMsg *amqp.Delivery) (msg *easypubsub.Message, err error) {
	header := easypubsub.Header(map[string][]string{
		"Topic":           {topic},
		"ContentType":     {amqpMsg.ContentType},
		"ContentEncoding": {amqpMsg.ContentEncoding},
		"Expiration":      {amqpMsg.Expiration},
		"MessageId":       {amqpMsg.MessageId},
		"Timestamp":       {amqpMsg.Timestamp.Format(time.RFC3339)},
		"Exchange":        {amqpMsg.Exchange},
		"RoutingKey":      {amqpMsg.RoutingKey},
	})

	for key, val := range amqpMsg.Headers {
		if key == easypubsub.DefaultMessageUUIDKey {
			continue
		}
		addHeader(header, key, val)
	}
	msgOpts := make([]easypubsub.MessageOption, 0, 4)
	msgOpts = append(msgOpts, easypubsub.WithHeader(header), easypubsub.WithBody(amqpMsg.Body), easypubsub.WithContext(ctx))
	if uuid, ok := amqpMsg.Headers[easypubsub.DefaultMessageUUIDKey]; ok {
		msgOpts = append(msgOpts, easypubsub.WithId(uuid.(string)))
	}
	msg = easypubsub.NewMessage(msgOpts...)
	return msg, nil
}

func addHeader(header easypubsub.Header, key string, f interface{}) error {
	switch fv := f.(type) {
	case nil:
		return nil
	case bool:
		header.Append(key, strconv.FormatBool(fv))
		return nil
	case byte:
		header.Append(key, strconv.Itoa(int(fv)))
		return nil
	case int:
		header.Append(key, strconv.Itoa(fv))
		return nil
	case int16:
		header.Append(key, strconv.Itoa(int(fv)))
		return nil
	case int32:
		header.Append(key, strconv.Itoa(int(fv)))
		return nil
	case int64:
		header.Append(key, strconv.Itoa(int(fv)))
		return nil
	case float32:
		header.Append(key, strconv.FormatFloat(float64(fv), 'g', 5, 64))
		return nil
	case float64:
		header.Append(key, strconv.FormatFloat(fv, 'g', 5, 64))
		return nil
	case string:
		header.Append(key, fv)
		return nil
	case []byte:
		header.Append(key, base64.StdEncoding.EncodeToString(fv))
		return nil
	case amqp.Decimal:
		header.Append(key, "Scale: "+strconv.Itoa(int(fv.Scale))+", Value: "+strconv.Itoa(int(fv.Value)))
		return nil
	case time.Time:
		header.Append(key, fv.Format(time.RFC3339))
		return nil
	case []interface{}:
		for _, v := range fv {
			if err := addHeader(header, key, v); err != nil {
				return fmt.Errorf("in array %s", err)
			}
		}
		return nil
	case amqp.Table:
		for k, v := range fv {
			if err := addHeader(header, key+"-"+k, v); err != nil {
				return fmt.Errorf("table field %q %s", k, err)
			}
		}
		return nil
	}

	return fmt.Errorf("value %T not supported", f)
}
