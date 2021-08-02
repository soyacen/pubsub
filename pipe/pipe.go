package easypubsubpipe

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type (
	options struct {
		interceptors []Interceptor
		logger       easypubsub.Logger
	}

	Pipe struct {
		o           *options
		source      *Source
		sink        *Sink
		errC        chan error
		outMsgC     chan *easypubsub.Message
		closed      int32
		wg          sync.WaitGroup
		closeC      chan struct{}
		interceptor Interceptor
		handler     MessageHandler
	}
)

func (c *Pipe) Flow(ctx context.Context) <-chan error {
	c.errC = make(chan error)
	if atomic.LoadInt32(&c.closed) == CLOSED {
		go c.closeErrC(errors.New("pipe is closed"))
		return c.errC
	}
	go c.flow(ctx)
	return c.errC
}

func (c *Pipe) Close() error {
	if atomic.CompareAndSwapInt32(&c.closed, NORMAL, CLOSED) {
		var err error
		if e := c.source.subscriber.Close(); e != nil {
			err = multierror.Append(err, e)
		}
		c.wg.Wait()
		if e := c.sink.publisher.Close(); e != nil {
			err = multierror.Append(err, e)
		}
		close(c.closeC)
		return err
	}
	return nil
}

func (c *Pipe) flow(ctx context.Context) {
	c.o.logger.Log("start flow")
	msgC, errC := c.source.subscriber.Subscribe(ctx, c.source.topic)
	for {
		select {
		case <-c.closeC:
			c.o.logger.Log("pipe is closing, stopping flow")
			return
		case <-ctx.Done():
			c.o.logger.Log("context is done, stopping flow")
			return
		case err := <-errC:
			if err != nil {
				c.o.logger.Log(err.Error())
				c.errC <- err
			}
		case msg := <-msgC:
			if msg != nil {
				c.handleMsg(msg)
			}
		}
	}
}

func (c *Pipe) handleMsg(msg *easypubsub.Message) {
	c.wg.Add(1)
	defer c.wg.Done()
	if err := c.interceptor(msg, c.handler); err != nil {
		msg.Ack()
		c.o.logger.Logf("msg %d was intercepted and auto acked, error: %v", msg.Id(), err)
		c.errC <- err
		return
	}
	if c.sink == nil {
		return
	}
	c.o.logger.Logf("send message %s", msg.Id())
	publishResult := c.sink.publisher.Publish(c.sink.topic, msg)
	if publishResult.Err != nil {
		c.errC <- publishResult.Err
	} else {
		c.o.logger.Logf("successful publish message, result: %v", publishResult.Result)
	}
}

func (c *Pipe) closeErrC(err error) {
	if err != nil {
		c.errC <- err
	}
	close(c.errC)
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

func WithInterceptors(interceptors ...Interceptor) Option {
	return func(o *options) {
		o.interceptors = append(o.interceptors, interceptors...)
	}
}

func New(source *Source, sink *Sink, handler MessageHandler, opts ...Option) *Pipe {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	pipe := &Pipe{
		o:       o,
		source:  source,
		sink:    sink,
		handler: handler,
		closed:  NORMAL,
		closeC:  make(chan struct{}),
	}
	pipe.interceptor = ChainInterceptor(pipe.o.interceptors...)
	return pipe
}
