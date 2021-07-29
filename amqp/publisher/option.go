package amqppublisher

import (
	"crypto/tls"
	"time"

	"github.com/streadway/amqp"

	easypubsub "github.com/soyacen/pubsub"
)

type MarshalMsgFunc func(topic string, msg *easypubsub.Message, msgProps *MessageProperties) (*amqp.Publishing, error)

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

type MessageProperties struct {
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id
}

type Publish struct {
	Mandatory bool
	Immediate bool
}

type options struct {
	logger         easypubsub.Logger
	marshalMsgFunc MarshalMsgFunc
	tlsConfig      *tls.Config
	amqpConfig     *amqp.Config
	exchange       *Exchange
	msgProps       *MessageProperties
	publish        *Publish
	transactional  bool
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger:         easypubsub.DefaultLogger(),
		marshalMsgFunc: DefaultMarshalMsgFunc,
		msgProps: &MessageProperties{
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    2,
		},
		exchange: &Exchange{
			Name:       "",
			Kind:       "topic",
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       nil,
		},
		publish: &Publish{
			Mandatory: false,
			Immediate: false,
		},
		transactional: false,
	}
}

type Option func(o *options)

func WithMarshalMsgFunc(marshalMsgFunc MarshalMsgFunc) Option {
	return func(o *options) {
		o.marshalMsgFunc = marshalMsgFunc
	}
}

func WithLogger(logger easypubsub.Logger) Option {
	return func(o *options) {
		o.logger = logger
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

func WithExchange(exchange *Exchange) Option {
	return func(o *options) {
		o.exchange = exchange
	}
}

func WithMessageProperties(msgProps *MessageProperties) Option {
	return func(o *options) {
		o.msgProps = msgProps
	}
}

func WithPublish(publish *Publish) Option {
	return func(o *options) {
		o.publish = publish
	}
}

func WithTransactional(enabled bool) Option {
	return func(o *options) {
		o.transactional = enabled
	}
}

func DefaultMarshalMsgFunc(topic string, msg *easypubsub.Message, msgProps *MessageProperties) (*amqp.Publishing, error) {
	amqpHeader := make(amqp.Table, len(msg.Header())+1)
	amqpHeader["Easy-PubSub-Message-ID"] = msg.Id()
	msgHeader := msg.Header()
	for key, values := range msgHeader {
		amqpHeader[key] = append([]string{}, values...)
	}
	pMsg := &amqp.Publishing{
		Headers:         amqpHeader,
		ContentType:     msgProps.ContentType,
		ContentEncoding: msgProps.ContentEncoding,
		DeliveryMode:    msgProps.DeliveryMode,
		Priority:        msgProps.Priority,
		CorrelationId:   msgProps.CorrelationId,
		ReplyTo:         msgProps.ReplyTo,
		Expiration:      msgProps.Expiration,
		MessageId:       msgProps.MessageId,
		Timestamp:       msgProps.Timestamp,
		Type:            msgProps.Type,
		UserId:          msgProps.UserId,
		AppId:           msgProps.AppId,
		Body:            msg.Body(),
	}
	return pMsg, nil
}
