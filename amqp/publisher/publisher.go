package amqppublisher

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
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
	url           string
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
	switch pub.o.producerType {
	case producerTypeSync:
		partition, offset, err := pub.syncProducer.SendMessage(producerMsg)
		if err != nil {
			return &easypubsub.PublishResult{Err: fmt.Errorf("failed send message, %w", err)}
		}
		return &easypubsub.PublishResult{Result: &PublishResult{Partition: partition, Offset: offset}}
	case producerTypeAsync:
		pub.asyncProducer.Input() <- producerMsg
		return &easypubsub.PublishResult{Result: id}
	default:
		return &easypubsub.PublishResult{Err: fmt.Errorf("unknown publisher type %d", pub.o.producerType)}
	}
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		switch pub.o.producerType {
		case producerTypeSync:
			return pub.syncProducer.Close()
		case producerTypeAsync:
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
	switch pub.o.producerType {
	case producerTypeSync:
		return "KafkaSyncPublisher"
	case producerTypeAsync:
		return "KafkaAsyncPublisher"
	default:
		return ""
	}
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

func New(url string, opts ...Option) (easypubsub.Publisher, error) {
	o := defaultOptions()
	o.apply(opts...)
	pub := &Publisher{o: o, url: url, close: NORMAL, wg: sync.WaitGroup{}}
	amqp.Dial()
	return pub, nil
}
