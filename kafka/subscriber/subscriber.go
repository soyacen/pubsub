package kafkasubscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/soyacen/goutils/errorutils"

	easypubsub "github.com/soyacen/pubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	o             *options
	close         int32
	closeC        chan struct{}
	msgC          chan *easypubsub.Message
	errC          chan error
	consumerGroup sarama.ConsumerGroup
	consumer      sarama.Consumer

	topic string
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (err error) {
	if atomic.LoadInt32(&sub.close) == CLOSED {
		return errors.New("subscriber is closed")
	}
	sub.topic = topic
	switch sub.o.consumerType {
	case consumerTypeConsumerGroup:
		sub.consumerGroupConsume(ctx)
		return nil
	case consumerTypeConsumer:
		if err := sub.consumerConsume(ctx); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown consumer type %d", sub.o.consumerType)
	}
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
		switch sub.o.consumerType {
		case consumerTypeConsumerGroup:
			err := sub.consumerGroup.Close()
			if err != nil {
				return fmt.Errorf("failed close consumer group, %w", err)
			}
			return nil
		case consumerTypeConsumer:
			err := sub.consumer.Close()
			if err != nil {
				return fmt.Errorf("failed close consumer, %w", err)
			}
			return nil
		default:
			return fmt.Errorf("unknown consumer type %d", sub.o.consumerType)
		}
	}
	return nil
}

func (sub *Subscriber) String() string {
	switch sub.o.consumerType {
	case consumerTypeConsumerGroup:
		return "KafkaConsumerGroupSubscriber"
	case consumerTypeConsumer:
		return "KafkaConsumerSubscriber"
	default:
		return ""
	}
}

func (sub *Subscriber) consume(ctx context.Context, topic string) {

}

func (sub *Subscriber) consumerConsume(ctx context.Context) error {
	partitions, err := sub.consumer.Partitions(sub.topic)
	if err != nil {
		return fmt.Errorf("failed get partitions, %w", err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, 0, len(partitions))
	for _, partition := range partitions {
		offset := sub.o.consumerConfig.Consumer.Offsets.Initial
		partitionConsumer, err := sub.consumer.ConsumePartition(sub.topic, partition, offset)
		if err != nil {
			return fmt.Errorf("failed consume partition, topic: %s, partition: %d, offset: %d, %w", sub.topic, partition, offset, err)
		}
		partitionConsumers = append(partitionConsumers, partitionConsumer)
	}

	var wg sync.WaitGroup
	sub.msgC = make(chan *easypubsub.Message)
	sub.errC = make(chan error)
	for _, partitionConsumer := range partitionConsumers {
		wg.Add(1)
		go errorutils.WithRecover(sub.recoverHandler, sub.partitionConsumerConsume(ctx, &wg, partitionConsumer))
	}
	go func() {
		wg.Wait()
		close(sub.msgC)
		close(sub.errC)
		sub.o.logger.Log("close message and error chan")
	}()
	return nil
}

func (sub *Subscriber) partitionConsumerConsume(ctx context.Context, wg *sync.WaitGroup, partitionConsumer sarama.PartitionConsumer) func() {
	return func() {
		defer wg.Done()
		defer func() {
			sub.o.logger.Log("stop partition consumer")
			if err := partitionConsumer.Close(); err != nil {
				sub.errC <- fmt.Errorf("failed close partition consumer, %w", err)
			}
		}()

		messages := partitionConsumer.Messages()
		errors := partitionConsumer.Errors()
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping partition consumer")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping partition consumer")
				return
			case err, ok := <-errors:
				if !ok {
					sub.o.logger.Log("partition consumer error's channel is closed, stopping partition consumer")
					return
				}
				sub.errC <- err
			case kafkaMsg, ok := <-messages:
				if !ok {
					sub.o.logger.Log("partition consumer message's channel is closed, stopping partition consumer")
					return
				}
				sub.handlerMsg(ctx, kafkaMsg)
			}
		}

	}
}

func (sub *Subscriber) recoverHandler(p interface{}) {
	sub.o.logger.Logf("recover panic %v", p)
}

func (sub *Subscriber) handlerMsg(ctx context.Context, kafkaMsg *sarama.ConsumerMessage) {
	msg, err := sub.o.unmarshalMsgFunc(ctx, sub.topic, kafkaMsg)
	if err != nil {
		sub.errC <- err
		return
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

func (sub *Subscriber) consumerGroupConsume(ctx context.Context) {

}

func New(brokers []string, opts ...Option) (easypubsub.Subscriber, error) {
	o := defaultOptions()
	o.apply(opts...)
	sub := &Subscriber{o: o, close: NORMAL, closeC: make(chan struct{})}
	switch sub.o.consumerType {
	case consumerTypeConsumerGroup:
		consumerGroup, err := sarama.NewConsumerGroup(brokers, o.groupID, sub.o.consumerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed new kafka %v consumer group %s, %w", brokers, o.groupID, err)
		}
		sub.consumerGroup = consumerGroup
		return sub, nil
	case consumerTypeConsumer:
		consumer, err := sarama.NewConsumer(brokers, sub.o.consumerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed new kafka %v consumer, %w", brokers, err)
		}
		sub.consumer = consumer
		return sub, nil
	default:
		return nil, fmt.Errorf("unknown consumer type %d", sub.o.consumerType)
	}
}
