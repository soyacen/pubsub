package kafkapublisher

import (
	"time"

	"github.com/Shopify/sarama"

	"github.com/soyacen/easypubsub"
)

type MarshalMsgFunc func(topic string, msg *easypubsub.Message) (*sarama.ProducerMessage, error)

type producerType = int

const (
	producerTypeSync  producerType = 0
	producerTypeAsync producerType = 1
)

type options struct {
	logger         easypubsub.Logger
	marshalMsgFunc MarshalMsgFunc
	producerType   producerType
	producerConfig *sarama.Config
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
		producerType:   producerTypeSync,
		producerConfig: DefaultSaramaConfig(),
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

func WithSyncProducerConfig(config *sarama.Config) Option {
	return func(o *options) {
		o.producerType = producerTypeSync
		o.producerConfig = config
		o.producerConfig.Producer.Return.Errors = true
		o.producerConfig.Producer.Return.Successes = true
	}
}

func WithAsyncProducerConfig(config *sarama.Config) Option {
	return func(o *options) {
		o.producerType = producerTypeAsync
		o.producerConfig = config
		o.producerConfig.Producer.Return.Errors = true
		o.producerConfig.Producer.Return.Successes = true
	}
}

func DefaultMarshalMsgFunc(topic string, msg *easypubsub.Message) (*sarama.ProducerMessage, error) {
	kafkaHeaders := []sarama.RecordHeader{
		{Key: []byte(easypubsub.DefaultMessageUUIDKey), Value: []byte(msg.Id())},
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

func DefaultSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Metadata.Retry.Backoff = time.Second * 2
	config.ClientID = "easypubsub"
	return config
}
