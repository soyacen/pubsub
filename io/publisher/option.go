package iopublisher

import (
	"github.com/soyacen/easypubsub"
)

type MarshalMsgFunc func(topic string, msg *easypubsub.Message) ([]byte, error)

type options struct {
	logger         easypubsub.Logger
	marshalMsgFunc MarshalMsgFunc
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger: easypubsub.DefaultLogger(),
		marshalMsgFunc: func(topic string, msg *easypubsub.Message) ([]byte, error) {
			return msg.Body(), nil
		},
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
