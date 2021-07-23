package kafkapublisher

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/soyacen/goutils/errorutils"
	"github.com/soyacen/goutils/stringutils"

	easypubsub "github.com/soyacen/pubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type PublishResult struct {
	Partition int32
	Offset    int64
}

type Publisher struct {
	o             *options
	brokers       []string
	close         int32
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	wg            sync.WaitGroup
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) (result *easypubsub.PublishResult) {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return &easypubsub.PublishResult{Err: errors.New("publisher is closed")}
	}
	if pub.o.interceptor != nil {
		err := pub.o.interceptor(topic, msg, easypubsub.DefaultInterceptHandler)
		if err != nil {
			return &easypubsub.PublishResult{Err: fmt.Errorf("msg is intercepted, %w", err)}
		}
	}
	producerMsg, err := pub.o.marshalMsgFunc(topic, msg)
	if err != nil {
		return &easypubsub.PublishResult{Err: fmt.Errorf("failed marsharl msg, %w", err)}
	}

	id := msg.Id()
	if stringutils.IsBlank(id) {
		id = uuid.NewString()
	}
	producerMsg.Metadata = id

	pub.o.logger.Logf("send message %s", id)
	if !pub.o.asyncEnabled {
		partition, offset, err := pub.syncProducer.SendMessage(producerMsg)
		if err != nil {
			return &easypubsub.PublishResult{Err: fmt.Errorf("failed send message, %w", err)}
		}
		return &easypubsub.PublishResult{Result: &PublishResult{Partition: partition, Offset: offset}}
	}
	pub.asyncProducer.Input() <- producerMsg
	return &easypubsub.PublishResult{Result: id}
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		if pub.o.asyncEnabled {
			pub.asyncProducer.AsyncClose()
			pub.wg.Wait()
			return nil
		}
		return pub.syncProducer.Close()
	}
	return nil
}

func (pub *Publisher) String() string {
	if pub.o.asyncEnabled {
		return "KafkaAsyncPublisher"
	}
	return "KafkaSyncPublisher"
}

func (pub *Publisher) handleSuccesses() {
	defer pub.wg.Done()
	for msg := range pub.asyncProducer.Successes() {
		pub.o.logger.Logf("message %s sent successfully", msg.Metadata)
	}
}

func (pub *Publisher) handleErrors() {
	defer pub.wg.Done()
	for err := range pub.asyncProducer.Errors() {
		pub.o.logger.Logf("failed to send message %s, %w", err.Msg.Metadata, err.Err)
	}
}

func New(brokers []string, opts ...Option) (easypubsub.Publisher, error) {
	o := defaultOptions()
	o.apply(opts...)
	pub := &Publisher{o: o, brokers: brokers, close: NORMAL, wg: sync.WaitGroup{}}
	if !o.asyncEnabled {
		producer, err := sarama.NewSyncProducer(pub.brokers, pub.o.publisherConfig)
		if err != nil {
			return nil, fmt.Errorf("failed new sync producer, %w", err)
		}
		pub.syncProducer = producer

	} else {
		producer, err := sarama.NewAsyncProducer(pub.brokers, pub.o.publisherConfig)
		if err != nil {
			return nil, fmt.Errorf("failed new sync producer, %w", err)
		}
		pub.asyncProducer = producer
		pub.wg.Add(2)
		go errorutils.WithRecover(nil, pub.handleSuccesses)
		go errorutils.WithRecover(nil, pub.handleErrors)
	}
	return pub, nil
}
