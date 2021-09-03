package kafkasubscriber

import (
	"context"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/soyacen/goutils/backoffutils"

	"github.com/soyacen/easypubsub"
)

type UnmarshalMsgFunc func(ctx context.Context, topic string, kafkaMsg *sarama.ConsumerMessage) (msg *easypubsub.Message, err error)

type options struct {
	logger           easypubsub.Logger
	unmarshalMsgFunc UnmarshalMsgFunc

	nackResendMaxAttempt uint
	nackResendBackoff    backoffutils.BackoffFunc
	reconnectBackoff     backoffutils.BackoffFunc
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger: easypubsub.DefaultLogger(),
		unmarshalMsgFunc: func(ctx context.Context, topic string, kafkaMsg *sarama.ConsumerMessage) (msg *easypubsub.Message, err error) {
			header := map[string][]string{
				"Topic":          {topic},
				"Partition":      {strconv.FormatInt(int64(kafkaMsg.Partition), 10)},
				"Offset":         {strconv.FormatInt(kafkaMsg.Offset, 10)},
				"Timestamp":      {kafkaMsg.Timestamp.Format(time.RFC3339)},
				"BlockTimestamp": {kafkaMsg.BlockTimestamp.Format(time.RFC3339)},
				"Key":            {string(kafkaMsg.Key)},
			}
			for _, recordHeader := range kafkaMsg.Headers {
				header[string(recordHeader.Key)] = append(header[string(recordHeader.Key)], string(recordHeader.Value))
			}
			msg = easypubsub.NewMessage(
				easypubsub.WithHeader(header),
				easypubsub.WithBody(kafkaMsg.Value),
				easypubsub.WithContext(ctx),
			)
			return msg, nil
		},
		nackResendMaxAttempt: 2,
		nackResendBackoff:    backoffutils.Linear(100 * time.Millisecond),
		reconnectBackoff:     backoffutils.Constant(time.Minute),
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

func WithNackResend(maxAttempt uint, backoff backoffutils.BackoffFunc) Option {
	return func(o *options) {
		o.nackResendMaxAttempt = maxAttempt
		o.nackResendBackoff = backoff
	}
}

func WithReconnectBackoff(reconnectBackoff backoffutils.BackoffFunc) Option {
	return func(o *options) {
		o.reconnectBackoff = reconnectBackoff
	}
}

const (
	_ = iota
	consumerConsumerType
	consumerGroupConsumerType
)

type consumerOptions struct {
	brokers        []string
	consumerType   int
	consumerConfig *sarama.Config
	groupID        string
}

type ConsumerOption func(o *consumerOptions)

func defaultConsumerOptions() *consumerOptions {
	return &consumerOptions{
		brokers:        nil,
		consumerType:   consumerConsumerType,
		consumerConfig: DefaultSubscriberConfig(),
		groupID:        "",
	}
}

func Consumer(brokers []string, config *sarama.Config) ConsumerOption {
	return func(o *consumerOptions) {
		o.consumerType = consumerConsumerType
		o.brokers = brokers
		o.consumerConfig = config
	}
}

func ConsumerGroup(brokers []string, groupID string, config *sarama.Config) ConsumerOption {
	return func(o *consumerOptions) {
		o.consumerType = consumerGroupConsumerType
		o.brokers = brokers
		o.groupID = groupID
		o.consumerConfig = config
	}
}

func DefaultSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = "easypubsub"
	return config
}
