package redispublisher

import (
	"github.com/soyacen/easypubsub"
)

type MsgMarshaler func(topic string, msg *easypubsub.Message) ([]byte, error)
type ChannelGenerator func(topic string) (channel string)

type options struct {
	logger          easypubsub.Logger
	marshalMsg      MsgMarshaler
	generateChannel ChannelGenerator
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger: easypubsub.DefaultLogger(),
		marshalMsg: func(topic string, msg *easypubsub.Message) ([]byte, error) {
			return msg.Body(), nil
		},
		generateChannel: func(topic string) (channel string) {
			return topic
		},
	}
}

type Option func(o *options)

func WithMsgMarshaler(marshalMsg MsgMarshaler) Option {
	return func(o *options) {
		o.marshalMsg = marshalMsg
	}
}

func WithLogger(logger easypubsub.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithChannelGenerator(generateChannel ChannelGenerator) Option {
	return func(o *options) {
		o.generateChannel = generateChannel
	}
}
