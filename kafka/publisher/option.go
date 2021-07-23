package kafkapublisher

import (
	"time"

	"github.com/Shopify/sarama"

	easypubsub "github.com/soyacen/pubsub"
)

type MarshalMsgFunc func(topic string, msg *easypubsub.Message) (*sarama.ProducerMessage, error)

type options struct {
	logger          easypubsub.Logger
	marshalMsgFunc  MarshalMsgFunc
	interceptors    []easypubsub.Interceptor
	interceptor     easypubsub.Interceptor
	asyncEnabled    bool
	publisherConfig *sarama.Config
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger:          easypubsub.DefaultLogger(),
		marshalMsgFunc:  DefaultMarshalMsgFunc,
		asyncEnabled:    false,
		publisherConfig: DefaultPublisherConfig(),
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

func WithInterceptor(interceptors ...easypubsub.Interceptor) Option {
	return func(o *options) {
		o.interceptors = append(o.interceptors, interceptors...)
		o.interceptor = easypubsub.ChainInterceptor(o.interceptors...)
	}
}

func WithSyncPublisherConfig(config *sarama.Config) Option {
	return func(o *options) {
		o.publisherConfig = config
		o.publisherConfig.Producer.Return.Errors = true
		o.publisherConfig.Producer.Return.Successes = true
		o.asyncEnabled = false
	}
}

func WithAsyncPublisherConfig(config *sarama.Config) Option {
	return func(o *options) {
		o.publisherConfig = config
		o.publisherConfig.Producer.Return.Errors = true
		o.publisherConfig.Producer.Return.Successes = true
		o.asyncEnabled = true
	}
}

func DefaultMarshalMsgFunc(topic string, msg *easypubsub.Message) (*sarama.ProducerMessage, error) {
	kafkaHeaders := []sarama.RecordHeader{
		{Key: []byte("Easy-Message-ID"), Value: []byte(msg.Id())},
	}
	msgHeader := msg.Header()
	for key, values := range msgHeader {
		for _, value := range values {
			header := sarama.RecordHeader{Key: []byte(key), Value: []byte(value)}
			kafkaHeaders = append(kafkaHeaders, header)
		}
	}
	pMsg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(msg.Body()),
		Headers: kafkaHeaders,
	}
	return pMsg, nil
}

func DefaultPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V1_0_0_0
	config.Metadata.Retry.Backoff = time.Second * 2
	config.ClientID = "easypubsub"
	return config
}
