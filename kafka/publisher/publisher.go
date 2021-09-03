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

	"github.com/soyacen/easypubsub"
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
	producerO     *producerOptions
	o             *options
	close         int32
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	wg            sync.WaitGroup
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) (result *easypubsub.PublishResult) {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return &easypubsub.PublishResult{Err: errors.New("publisher is closed")}
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
	switch pub.producerO.producerType {
	case syncProducerType:
		partition, offset, err := pub.syncProducer.SendMessage(producerMsg)
		if err != nil {
			return &easypubsub.PublishResult{Err: fmt.Errorf("failed send message, %w", err)}
		}
		return &easypubsub.PublishResult{Result: &PublishResult{Partition: partition, Offset: offset}}
	case asyncProducerType:
		pub.asyncProducer.Input() <- producerMsg
		return &easypubsub.PublishResult{Result: id}
	default:
		return &easypubsub.PublishResult{Err: fmt.Errorf("unknown publisher type %d", pub.producerO.producerType)}
	}
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		switch pub.producerO.producerType {
		case syncProducerType:
			return pub.syncProducer.Close()
		case asyncProducerType:
			pub.asyncProducer.AsyncClose()
			pub.wg.Wait()
			return nil
		default:
			return nil
		}
	}
	return nil
}

func (pub *Publisher) String() string {
	return "KafkaPublisher"
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

func (pub *Publisher) recoverHandler(p interface{}) {
	pub.o.logger.Logf("recover panic %v", p)
}

func New(producerOpt ProducerOption, opts ...Option) (easypubsub.Publisher, error) {
	producerO := defaultProducerOptions()
	producerOpt(producerO)
	o := defaultOptions()
	o.apply(opts...)
	pub := &Publisher{producerO: producerO, o: o, close: NORMAL, wg: sync.WaitGroup{}}
	switch pub.producerO.producerType {
	default:
		return nil, fmt.Errorf("unknown publisher type %d", pub.producerO.producerType)
	case syncProducerType:
		producer, err := sarama.NewSyncProducer(pub.producerO.brokers, pub.producerO.producerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed new kafka sync producer, %w", err)
		}
		pub.syncProducer = producer
	case asyncProducerType:
		producer, err := sarama.NewAsyncProducer(pub.producerO.brokers, pub.producerO.producerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed new kafka async producer, %w", err)
		}
		pub.asyncProducer = producer
		pub.wg.Add(2)
		go errorutils.WithRecover(pub.recoverHandler, pub.handleSuccesses)
		go errorutils.WithRecover(pub.recoverHandler, pub.handleErrors)
	}
	return pub, nil
}
