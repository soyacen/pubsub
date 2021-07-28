package amqppublisher

import (
	"crypto/tls"

	"github.com/streadway/amqp"

	easypubsub "github.com/soyacen/pubsub"
)

type MarshalMsgFunc func(topic string, msg *easypubsub.Message) (*amqp.Publishing, error)

type options struct {
	logger                 easypubsub.Logger
	marshalMsgFunc         MarshalMsgFunc
	tlsConfig              *tls.Config
	amqpConfig             *amqp.Config
	contentType            string
	contentEncoding        string
	persistentDeliveryMode bool
	exchangeName           string
	exchangeKind           string
	exchangeDurable        bool
	exchangeAutoDelete     bool
	exchangeInternal       bool
	exchangeNoWait         bool
	exchangeArgs           map[string]interface{}
	publishMandatory       bool
	publishImmediate       bool
	transactional          bool
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger:                 easypubsub.DefaultLogger(),
		marshalMsgFunc:         DefaultMarshalMsgFunc,
		contentType:            "text/plain",
		contentEncoding:        "",
		persistentDeliveryMode: true,
		exchangeName:           "",
		exchangeKind:           "topic",
		exchangeDurable:        true,
		exchangeAutoDelete:     false,
		exchangeInternal:       false,
		exchangeNoWait:         false,
		exchangeArgs:           nil,
		publishMandatory:       false,
		publishImmediate:       false,
		transactional:          false,
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

func WithExchangeName(name string) Option {
	return func(o *options) {
		o.exchangeName = name
	}
}
func WithExchangeKind(kind string) Option {
	return func(o *options) {
		o.exchangeKind = kind
	}
}
func WithExchangeDurable(durable bool) Option {
	return func(o *options) {
		o.exchangeDurable = durable
	}
}
func WithExchangeAutoDelete(autoDelete bool) Option {
	return func(o *options) {
		o.exchangeAutoDelete = autoDelete
	}
}
func WithExchangeInternal(internal bool) Option {
	return func(o *options) {
		o.exchangeInternal = internal
	}
}
func WithExchangeNoWait(noWait bool) Option {
	return func(o *options) {
		o.exchangeNoWait = noWait
	}
}
func WithExchangeArgs(args map[string]interface{}) Option {
	return func(o *options) {
		o.exchangeArgs = args
	}
}

func WithPublishMandatory(mandatory bool) Option {
	return func(o *options) {
		o.publishMandatory = mandatory
	}
}

func WithPublishImmediate(immediate bool) Option {
	return func(o *options) {
		o.publishImmediate = immediate
	}
}

func WithContentEncoding(contentEncoding string) Option {
	return func(o *options) {
		o.contentEncoding = contentEncoding
	}
}

func WithContentType(contentType string) Option {
	return func(o *options) {
		o.contentType = contentType
	}
}

func WithPersistentDeliveryMode(enabled bool) Option {
	return func(o *options) {
		o.persistentDeliveryMode = enabled
	}
}

func WithTransactional(enabled bool) Option {
	return func(o *options) {
		o.transactional = enabled
	}
}

func DefaultMarshalMsgFunc(topic string, msg *easypubsub.Message) (*amqp.Publishing, error) {
	amqpHeader := make(amqp.Table, len(msg.Header())+1)
	amqpHeader["Easy-PubSub-Message-ID"] = msg.Id()
	msgHeader := msg.Header()
	for key, values := range msgHeader {
		amqpHeader[key] = append([]string{}, values...)
	}
	pMsg := &amqp.Publishing{
		Headers: amqpHeader,
		Body:    msg.Body(),
	}
	return pMsg, nil
}
