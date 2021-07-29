package kafkasubscriber

import (
	"context"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/soyacen/easypubsub"
)

type consumerType = int

const (
	consumerTypeConsumer      consumerType = 0
	consumerTypeConsumerGroup consumerType = 1
)

type UnmarshalMsgFunc func(ctx context.Context, topic string, kafkaMsg *sarama.ConsumerMessage) (msg *easypubsub.Message, err error)

type options struct {
	logger                  easypubsub.Logger
	unmarshalMsgFunc        UnmarshalMsgFunc
	consumerType            consumerType
	consumerConfig          *sarama.Config
	groupID                 string
	nackResendSleepDuration time.Duration
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
		consumerType:     consumerTypeConsumer,
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

func WithConsumerConfig(config *sarama.Config) Option {
	return func(o *options) {
		o.consumerType = consumerTypeConsumer
		o.consumerConfig = config
	}
}

func WithConsumerGroupConfig(groupID string, config *sarama.Config) Option {
	return func(o *options) {
		o.consumerType = consumerTypeConsumerGroup
		o.consumerConfig = config
		o.groupID = groupID
	}
}

func WithNackResendSleepDuration(duration time.Duration) Option {
	return func(o *options) {
		o.nackResendSleepDuration = duration
	}
}

func DefaultUnmarshalMsgFunc(ctx context.Context, topic string, kafkaMsg *sarama.ConsumerMessage) (msg *easypubsub.Message, err error) {
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
}

func DefaultSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.ClientID = "easypubsub"
	return config
}
