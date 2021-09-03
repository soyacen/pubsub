package redissubscriber

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/soyacen/goutils/backoffutils"

	"github.com/soyacen/easypubsub"
)

type MsgUnmarshaler func(ctx context.Context, topic string, redisMsg *redis.Message) (msg *easypubsub.Message, err error)
type ChannelGenerator func(topic string) (channel string)
type PatternGenerator func(topic string) (channel string)

type options struct {
	logger                  easypubsub.Logger
	unmarshalMsg            MsgUnmarshaler
	patternSubscribeEnabled bool
	generateChannel         ChannelGenerator
	generatePattern         PatternGenerator
	nackResendMaxAttempt    uint
	nackResendBackoff       backoffutils.BackoffFunc
	reSubMaxAttempt         uint
	resubBackoff            backoffutils.BackoffFunc
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger: easypubsub.DefaultLogger(),
		unmarshalMsg: func(ctx context.Context, topic string, redisMsg *redis.Message) (msg *easypubsub.Message, err error) {
			header := map[string][]string{
				"Topic":   {topic},
				"Channel": {redisMsg.Channel},
				"Pattern": {redisMsg.Pattern},
			}
			msg = easypubsub.NewMessage(
				easypubsub.WithHeader(header),
				easypubsub.WithBody([]byte(redisMsg.Payload)),
				easypubsub.WithContext(ctx),
			)
			return msg, nil
		},
		patternSubscribeEnabled: false,
		generateChannel: func(topic string) (channel string) {
			return topic
		},
		generatePattern: func(topic string) (channel string) {
			return topic
		},
		nackResendMaxAttempt: 2,
		nackResendBackoff:    backoffutils.Linear(100 * time.Millisecond),
		reSubMaxAttempt:      5,
		resubBackoff:         backoffutils.Constant(time.Minute),
	}
}

type Option func(o *options)

func WithLogger(logger easypubsub.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithMsgUnmarshaler(unmarshalMsg MsgUnmarshaler) Option {
	return func(o *options) {
		o.unmarshalMsg = unmarshalMsg
	}
}

func WithEnablePatternSubscribe() Option {
	return func(o *options) {
		o.patternSubscribeEnabled = true
	}
}

func WithChannelGenerator(generateChannel ChannelGenerator) Option {
	return func(o *options) {
		o.generateChannel = generateChannel
	}
}

func WithPatternGenerator(generatePattern PatternGenerator) Option {
	return func(o *options) {
		o.generatePattern = generatePattern
	}
}

func WithNackResendMaxAttempt(nackResendMaxAttempt uint) Option {
	return func(o *options) {
		o.nackResendMaxAttempt = nackResendMaxAttempt
	}
}

func WithNackResendBackoff(nackResendBackoff backoffutils.BackoffFunc) Option {
	return func(o *options) {
		o.nackResendBackoff = nackResendBackoff
	}
}

func WithReSubMaxAttempt(reSubMaxAttempt uint) Option {
	return func(o *options) {
		o.reSubMaxAttempt = reSubMaxAttempt
	}
}

func WithReSubBackoff(reSubBackoff backoffutils.BackoffFunc) Option {
	return func(o *options) {
		o.resubBackoff = reSubBackoff
	}
}
