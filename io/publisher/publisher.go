package iopublisher

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	easypubsub "github.com/soyacen/pubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Publisher struct {
	o           *options
	writeCloser io.WriteCloser
	close       int32
}

func (pub *Publisher) AddInterceptor(interceptors ...easypubsub.Interceptor) {
	pub.o.interceptors = append(pub.o.interceptors, interceptors...)
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) (result easypubsub.PublishResult) {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return easypubsub.PublishResult{Err: errors.New("publisher is closed")}
	}
	for _, interceptor := range pub.o.interceptors {
		if err := interceptor(topic, msg); err != nil {
			return easypubsub.PublishResult{Err: fmt.Errorf("msg is intercepted, %w", err)}
		}
	}
	data, err := pub.o.marshalMsgFunc(topic, msg)
	if err != nil {
		return easypubsub.PublishResult{Err: fmt.Errorf("failed marsharl msg, %w", err)}
	}
	n, err := pub.writeCloser.Write(data)
	if err != nil {
		return easypubsub.PublishResult{Err: fmt.Errorf("failed write msg, %w", err)}
	}
	return easypubsub.PublishResult{Result: n}
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		return pub.writeCloser.Close()
	}
	return nil
}

func (pub *Publisher) String() string {
	return "IOPublisher"
}

func New(writer io.Writer, opts ...Option) easypubsub.Publisher {
	o := defaultOptions()
	o.apply(opts...)
	pub := &Publisher{o: o, close: NORMAL}
	if writeCloser, ok := writer.(io.WriteCloser); ok {
		pub.writeCloser = writeCloser
	} else {
		pub.writeCloser = NopCloser(writer)
	}
	return pub
}
