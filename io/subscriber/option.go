package iosubscriber

import (
	"context"
	"time"

	"github.com/soyacen/easypubsub"
)

type SplitType = int

const (
	splitTypeDelimiter SplitType = 0
	splitTypeBlock     SplitType = 1
)

type UnmarshalMsgFunc func(topic string, data []byte) (msg *easypubsub.Message, err error)

type options struct {
	logger                  easypubsub.Logger
	unmarshalMsgFunc        UnmarshalMsgFunc
	splitType               SplitType
	blockSize               int
	delimiter               byte
	pollInterval            time.Duration
	nackResendSleepDuration time.Duration
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func defaultOptions() *options {
	return &options{
		logger: easypubsub.DefaultLogger(),
		unmarshalMsgFunc: func(topic string, data []byte) (msg *easypubsub.Message, err error) {
			msg = easypubsub.NewMessage(
				easypubsub.WithBody(data),
				easypubsub.WithHeader(map[string][]string{"topic": {topic}}),
				easypubsub.WithContext(context.Background()),
			)
			return msg, nil
		},
		splitType:               splitTypeDelimiter,
		delimiter:               '\n',
		pollInterval:            time.Second,
		nackResendSleepDuration: time.Millisecond * 100,
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

func WithBlockSize(size int) Option {
	return func(o *options) {
		o.blockSize = size
		o.splitType = splitTypeBlock
	}
}

func WithDelimiter(delimiter byte) Option {
	return func(o *options) {
		o.delimiter = delimiter
		o.splitType = splitTypeDelimiter
	}
}

func WithPollInterval(pollInterval time.Duration) Option {
	return func(o *options) {
		o.pollInterval = pollInterval
	}
}

func WithNackResendSleepDuration(interval time.Duration) Option {
	return func(o *options) {
		o.nackResendSleepDuration = interval
	}
}
