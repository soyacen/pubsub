package chansubscriber

import (
	"time"

	"github.com/soyacen/easypubsub"
)

type options struct {
	logger                  easypubsub.Logger
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
	}
}

type Option func(o *options)

func WithLogger(logger easypubsub.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithNackResendSleepDuration(interval time.Duration) Option {
	return func(o *options) {
		o.nackResendSleepDuration = interval
	}
}
