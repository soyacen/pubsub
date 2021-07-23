package iosubscriber

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"sync/atomic"
	"time"

	easypubsub "github.com/soyacen/pubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	o          *options
	readCloser io.ReadCloser
	close      int32
	closeC     chan struct{}
	msgC       chan *easypubsub.Message
	errC       chan error
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (err error) {
	if atomic.LoadInt32(&sub.close) == CLOSED {
		return errors.New("subscriber is closed")
	}
	sub.msgC = make(chan *easypubsub.Message)
	sub.errC = make(chan error)
	go sub.consume(ctx, topic)
	return nil
}

func (sub *Subscriber) Messages() (msgC <-chan *easypubsub.Message) {
	return sub.msgC
}

func (sub *Subscriber) Errors() (errC <-chan error) {
	return sub.errC
}

func (sub *Subscriber) Close() error {
	if atomic.CompareAndSwapInt32(&sub.close, NORMAL, CLOSED) {
		close(sub.closeC)
		err := sub.readCloser.Close()
		return err
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "IOSubscriber"
}

func (sub *Subscriber) consume(ctx context.Context, topic string) {
	defer close(sub.errC)
	defer close(sub.msgC)
	var reader *bufio.Reader
	if sub.o.splitType == splitTypeBlock {
		reader = bufio.NewReaderSize(sub.readCloser, sub.o.blockSize)
	} else {
		reader = bufio.NewReader(sub.readCloser)
	}
FirstLoop:
	for {
		if atomic.LoadInt32(&sub.close) == CLOSED {
			return
		}
		select {
		case <-sub.closeC:
			return
		case <-ctx.Done():
			return
		default:
			block, err := sub.read(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					time.Sleep(sub.o.pollInterval)
					continue FirstLoop
				} else {
					sub.errC <- err
					return
				}
			}
			if len(block) == 0 {
				time.Sleep(sub.o.pollInterval)
				continue FirstLoop
			}

			msg, err := sub.o.unmarshalMsgFunc(topic, block)
			if err != nil {
				sub.errC <- err
				continue FirstLoop
			}
			if sub.o.interceptor != nil {
				err := sub.o.interceptor(topic, msg, easypubsub.DefaultInterceptHandler)
				if err != nil {
					sub.errC <- err
					continue FirstLoop
				}
			}
		HandleMsg:
			msg.Responder = easypubsub.NewResponder()
			// send msg to msg chan
			sub.msgC <- msg
			sub.o.logger.Logf("message %s sent to channel, wait ack...", msg.Id())
			select {
			case <-msg.Acked():
				msg.AckResp() <- &easypubsub.Response{Result: "ok"}
				sub.o.logger.Logf("message %s acked", msg.Id())
				return
			case <-msg.Nacked():
				msg.NackResp() <- &easypubsub.Response{Result: "ok"}
				sub.o.logger.Logf("message %s nacked", msg.Id())
				if sub.o.nackResendSleepDuration > 0 {
					time.Sleep(sub.o.nackResendSleepDuration)
				}
				sub.o.logger.Logf("message %s resend", msg.Id())
				goto HandleMsg
			}
		}
	}
}

func (sub *Subscriber) read(reader *bufio.Reader) ([]byte, error) {
	var block []byte
	var err error
	switch sub.o.splitType {
	case splitTypeDelimiter:
		block, err = reader.ReadBytes(sub.o.delimiter)
	case splitTypeBlock:
		block = make([]byte, sub.o.blockSize)
		var n int
		n, err = reader.Read(block)
		block = block[:n]
	}
	return block, err
}

func New(reader io.Reader, opts ...Option) easypubsub.Subscriber {
	o := defaultOptions()
	o.apply(opts...)
	sub := &Subscriber{o: o, close: NORMAL, closeC: make(chan struct{})}
	if readCloser, ok := reader.(io.ReadCloser); ok {
		sub.readCloser = readCloser
	} else {
		sub.readCloser = ioutil.NopCloser(reader)
	}
	return sub
}
