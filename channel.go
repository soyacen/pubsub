package easypubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type (
	Filter func(inMsg *Message) (outMsg *Message, err error)

	Source struct {
		topic      string
		subscriber Subscriber
	}

	Sink struct {
		topic     string
		publisher Publisher
	}

	channelOptions struct {
		filters []Filter
		filter  Filter
		logger  Logger
	}

	Channel struct {
		o       *channelOptions
		source  *Source
		sink    *Sink
		errC    chan error
		outMsgC chan *Message
		close   int32
		closeC  chan struct{}
		wg      *sync.WaitGroup
	}
)

func (c *Channel) Flow(ctx context.Context) error {
	if atomic.LoadInt32(&c.close) == CLOSED {
		return errors.New("channel is closed")
	}
	c.o.logger.Logf("start subscribe %s", c.source.topic)
	err := c.source.subscriber.Subscribe(ctx, c.source.topic)
	if err != nil {
		return err
	}

	c.outMsgC = make(chan *Message)
	c.errC = make(chan error)
	c.wg = &sync.WaitGroup{}
	c.wg.Add(1)
	go c.flowInto()
	c.wg.Add(1)
	go c.outFlow()
	go c.waiteError()
	return nil
}

func (c *Channel) Errors() <-chan error {
	return c.errC
}

func (c *Channel) Close() error {
	if atomic.CompareAndSwapInt32(&c.close, NORMAL, CLOSED) {
		close(c.closeC)
		var err error
		if e := c.source.subscriber.Close(); e != nil {
			err = multierror.Append(err, e)
		}
		if e := c.sink.publisher.Close(); e != nil {
			err = multierror.Append(err, e)
		}
		return err
	}
	return nil
}

func (c *Channel) flowInto() {
	defer c.wg.Done()
	defer close(c.outMsgC)
	inMsgC := c.source.subscriber.Messages()
	errC := c.source.subscriber.Errors()
	for {
		select {
		case <-c.closeC:
			return
		case msg, ok := <-inMsgC:
			if !ok {
				return
			}
			c.o.logger.Logf("received message %s", msg.Id())
			outMsg, err := c.o.filter(msg)
			if err != nil {
				c.errC <- err
			} else {
				c.outMsgC <- outMsg
			}
		case err, ok := <-errC:
			if !ok {
				return
			}
			c.errC <- err
		}
	}
}

func (c *Channel) outFlow() {
	defer c.wg.Done()
	for msg := range c.outMsgC {
		c.o.logger.Logf("send message %s", msg.Id())
		publishResult := c.sink.publisher.Publish(c.sink.topic, msg)
		if publishResult.Err != nil {
			c.errC <- publishResult.Err
		} else {
			c.o.logger.Logf("successful publish message, detail: %v", publishResult.Result)
		}
	}
}

func (c *Channel) waiteError() {
	c.wg.Wait()
	close(c.errC)
}

func NewSource(topic string, subscriber Subscriber) *Source {
	return &Source{topic: topic, subscriber: subscriber}
}

func NewSink(topic string, publisher Publisher) *Sink {
	return &Sink{topic: topic, publisher: publisher}
}

func defaultChannelOptions() *channelOptions {
	return &channelOptions{
		filters: []Filter{DefaultFilter},
		logger:  DefaultLogger(),
	}
}

type ChannelOption func(o *channelOptions)

func WithLogger(logger Logger) ChannelOption {
	return func(o *channelOptions) {
		o.logger = logger
	}
}

func WithFilter(filters ...Filter) ChannelOption {
	return func(o *channelOptions) {
		o.filters = append(o.filters, filters...)
	}
}

func NewChannel(source *Source, sink *Sink, opts ...ChannelOption) *Channel {
	o := defaultChannelOptions()
	for _, opt := range opts {
		opt(o)
	}
	o.filter = func(inMsg *Message) (outMsg *Message, err error) {
		for _, filter := range o.filters {
			if outMsg, err := filter(inMsg); err != nil {
				return nil, err
			} else {
				inMsg = outMsg
			}
		}
		return inMsg, err
	}
	channel := &Channel{
		o:      o,
		source: source,
		sink:   sink,
		close:  NORMAL,
		closeC: make(chan struct{}),
	}
	return channel
}

func DefaultFilter(inMsg *Message) (outMsg *Message, err error) {
	inMsg.Ack()
	return inMsg, nil
}
