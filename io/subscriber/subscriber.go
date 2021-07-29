package iosubscriber

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync/atomic"
	"time"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	o          *options
	readCloser io.ReadCloser
	closed     int32
	closeC     chan struct{}
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *easypubsub.Message, <-chan error) {
	errC := make(chan error)
	msgC := make(chan *easypubsub.Message)
	if atomic.LoadInt32(&sub.closed) == CLOSED {
		go func() { defer close(errC); defer close(msgC); errC <- errors.New("subscriber is closed") }()
		return msgC, errC
	}
	go sub.subscribe(ctx, topic, msgC, errC)
	return msgC, errC
}

func (sub *Subscriber) Close() error {
	if atomic.CompareAndSwapInt32(&sub.closed, NORMAL, CLOSED) {
		close(sub.closeC)
		err := sub.readCloser.Close()
		if err != nil {
			return fmt.Errorf("failed to close reader")
		}
		return nil
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "IOSubscriber"
}

func (sub *Subscriber) subscribe(ctx context.Context, topic string, msgC chan *easypubsub.Message, errC chan error) {
	defer close(errC)
	defer close(msgC)
	sub.o.logger.Logf("start subscribe")
	var reader *bufio.Reader
	if sub.o.splitType == splitTypeBlock {
		sub.o.logger.Logf("new bufio reader with size %d", sub.o.blockSize)
		reader = bufio.NewReaderSize(sub.readCloser, sub.o.blockSize)
	} else {
		sub.o.logger.Log("new bufio reader")
		reader = bufio.NewReader(sub.readCloser)
	}
FirstLoop:
	for {
		select {
		case <-sub.closeC:
			sub.o.logger.Log("subscriber is closing, stopping subscribe")
			return
		case <-ctx.Done():
			sub.o.logger.Log("context is Done, stopping subscribe")
			return
		default:
			block, err := sub.read(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					sub.o.logger.Logf("read end of file, sleep %s", sub.o.pollInterval)
					time.Sleep(sub.o.pollInterval)
					continue FirstLoop
				} else {
					errC <- err
					return
				}
			}
			if len(block) == 0 {
				sub.o.logger.Logf("block size is 0, sleep %s", sub.o.pollInterval)
				time.Sleep(sub.o.pollInterval)
				continue FirstLoop
			}

			msg, err := sub.o.unmarshalMsgFunc(topic, block)
			if err != nil {
				errC <- err
				continue FirstLoop
			}

		HandleMsg:
			msg.Responder = easypubsub.NewResponder()
			msgC <- msg
			sub.o.logger.Logf("message %s sent, wait ack...", msg.Id())
			select {
			case <-msg.Acked():
				msg.AckResp() <- &easypubsub.Response{Result: "ok"}
				sub.o.logger.Logf("message %s acked", msg.Id())
			case <-msg.Nacked():
				msg.NackResp() <- &easypubsub.Response{Result: "ok"}
				sub.o.logger.Logf("message %s nacked", msg.Id())
				if sub.o.nackResendSleepDuration > 0 {
					time.Sleep(sub.o.nackResendSleepDuration)
					sub.o.logger.Logf("message %s will resend", msg.Id())
				}
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
	sub := &Subscriber{o: o, closed: NORMAL, closeC: make(chan struct{})}
	if readCloser, ok := reader.(io.ReadCloser); ok {
		sub.readCloser = readCloser
	} else {
		sub.readCloser = ioutil.NopCloser(reader)
	}
	return sub
}
