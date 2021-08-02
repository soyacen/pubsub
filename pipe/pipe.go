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

func (pipe *Pipe) Flow(ctx context.Context) <-chan error {
	pipe.errC = make(chan error)
	if atomic.LoadInt32(&pipe.closed) == CLOSED {
		go pipe.closeErrC(errors.New("pipe is closed"))
		return pipe.errC
	}
	if pipe.source == nil {
		go pipe.closeErrC(errors.New("source is nil"))
		return pipe.errC
	}
	go pipe.flow(ctx)
	return pipe.errC
}

func (pipe *Pipe) Close() error {
	if atomic.CompareAndSwapInt32(&pipe.closed, NORMAL, CLOSED) {
		var err error
		if pipe.source != nil {
			if e := pipe.source.subscriber.Close(); e != nil {
				err = multierror.Append(err, e)
			}
		}
		pipe.wg.Wait()
		if pipe.sink != nil {
			if e := pipe.sink.publisher.Close(); e != nil {
				err = multierror.Append(err, e)
			}
		}
		close(pipe.closeC)
		return err
	}
	return nil
}

func (pipe *Pipe) flow(ctx context.Context) {
	pipe.o.logger.Log("start flow")
	msgC, errC := pipe.source.subscriber.Subscribe(ctx, pipe.source.topic)
	for {
		select {
		case <-pipe.closeC:
			pipe.o.logger.Log("pipe is closing, stopping flow")
			pipe.closeErrC(nil)
			return
		case <-ctx.Done():
			pipe.o.logger.Log("context is done, stopping flow")
			pipe.closeErrC(nil)
			return
		case err := <-errC:
			if err != nil {
				pipe.o.logger.Log(err.Error())
				pipe.errC <- err
			}
		case msg := <-msgC:
			if msg != nil {
				pipe.handleMsg(msg)
			}
		}
	}
}

func (pipe *Pipe) handleMsg(msg *easypubsub.Message) {
	pipe.wg.Add(1)
	defer pipe.wg.Done()
	if err := pipe.interceptor(msg, pipe.handler); err != nil {
		msg.Ack()
		pipe.o.logger.Logf("msg %s was intercepted and auto acked, error: %v", msg.Id(), err)
		pipe.errC <- err
		return
	}
	if pipe.sink == nil {
		return
	}
	pipe.o.logger.Logf("send message %s", msg.Id())
	publishResult := pipe.sink.publisher.Publish(pipe.sink.topic, msg)
	if publishResult.Err != nil {
		pipe.errC <- publishResult.Err
	} else {
		pipe.o.logger.Logf("successful publish message, result: %v", publishResult.Result)
	}
}

func (pipe *Pipe) closeErrC(err error) {
	if err != nil {
		pipe.errC <- err
	}
	close(pipe.errC)
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
