package kafkasubscriber

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
		return sub.readCloser.Close()
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "KafkaSubscriber"
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
			sub.errC <- ctx.Err()
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
		HandleMsg:
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

			ackC := make(chan struct{})
			nackC := make(chan struct{})
			ackRespC := make(chan easypubsub.Response)
			nackRespC := make(chan easypubsub.Response)
			msg.Responder = easypubsub.NewResponder(ackC, nackC, ackRespC, nackRespC)
			sub.msgC <- msg
			select {
			case <-ackC:
				ackRespC <- easypubsub.Response{}
				close(ackRespC)
				close(nackC)
				close(nackRespC)
			case <-nackC:
				nackRespC <- easypubsub.Response{}
				close(nackRespC)
				close(ackC)
				close(ackRespC)
				goto HandleMsg
			case <-sub.closeC:
				return
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
	pub := &Subscriber{o: o, close: NORMAL, closeC: make(chan struct{})}
	if readCloser, ok := reader.(io.ReadCloser); ok {
		pub.readCloser = readCloser
	} else {
		pub.readCloser = ioutil.NopCloser(reader)
	}
	return pub
}
