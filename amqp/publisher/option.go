package amqppublisher

import (
	"crypto/tls"
	"time"

	"github.com/streadway/amqp"

	"github.com/soyacen/easypubsub"
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
	RoutingKeyFunc func(topic string) string
	Mandatory      bool
	Immediate      bool
}

type options struct {
	logger         easypubsub.Logger
	marshalMsgFunc MarshalMsgFunc
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
		logger: easypubsub.DefaultLogger(),
		marshalMsgFunc: func(topic string, msg *easypubsub.Message, msgProps *MessageProperties) (*amqp.Publishing, error) {
			amqpHeader := make(amqp.Table, len(msg.Header())+1)
			amqpHeader[easypubsub.DefaultMessageUUIDKey] = msg.Id()
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
		},
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

func WithPublishOptions(publish *Publish) Option {
	return func(o *options) {
		o.publish = publish
	}
}

func WithTransactional(enabled bool) Option {
	return func(o *options) {
		o.transactional = enabled
	}
}

const (
	_ = iota
	normalConnectionType
	tlsConnectionType
	amqpConnectionType
)

type connectionOptions struct {
	connectionType int
	url            string
	tlsConfig      *tls.Config
	amqpConfig     *amqp.Config
}

type ConnectionOption func(o *connectionOptions)

func Connection(url string) ConnectionOption {
	return func(o *connectionOptions) {
		o.url = url
		o.connectionType = normalConnectionType
	}
}

func ConnectionWithTLS(url string, tlsConfig *tls.Config) ConnectionOption {
	return func(o *connectionOptions) {
		o.url = url
		o.tlsConfig = tlsConfig
		o.connectionType = tlsConnectionType
	}
}

func ConnectionWithConfig(url string, amqpConfig *amqp.Config) ConnectionOption {
	return func(o *connectionOptions) {
		o.url = url
		o.amqpConfig = amqpConfig
		o.connectionType = amqpConnectionType
	}
}
