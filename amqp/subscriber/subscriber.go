package amqpsubscriber

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
	brokers       []string
	topic         string
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (err error) {
	if atomic.LoadInt32(&sub.close) == CLOSED {
		return errors.New("subscriber is closed")
	}
	sub.topic = topic
	switch sub.o.consumerType {
	case consumerTypeConsumerGroup:
		return sub.consumerGroupSubscribe(ctx)
	case consumerTypeConsumer:
		return sub.consumerSubscribe(ctx)
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
			if sub.consumerGroup != nil {
				return nil
			}
			err := sub.consumerGroup.Close()
			if err != nil {
				return fmt.Errorf("failed close consumer group, %w", err)
			}
			return nil
		case consumerTypeConsumer:
			if sub.consumer != nil {
				return nil
			}
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

// ========================  consumer consume  ========================
func (sub *Subscriber) consumerSubscribe(ctx context.Context) error {
	sub.o.logger.Log("create consumer")
	partitionConsumers, err := sub.createPartitionConsumer()
	if err != nil {
		return err
	}
	sub.o.logger.Log("prepare partition consumer consume")
	exitC := sub.preparePartitionConsumerConsume(ctx, partitionConsumers)
	go func(exitC <-chan struct{}) {
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping consumer subscribe")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping consumer subscribe")
				return
			case <-exitC:
				sub.o.logger.Log("create consumer")
				partitionConsumers, err := sub.createPartitionConsumer()
				if err != nil {
					sub.o.logger.Log(err.Error())
					return
				}
				sub.o.logger.Log("prepare partition consumer consume")
				exitC = sub.preparePartitionConsumerConsume(ctx, partitionConsumers)
			}
		}
	}(exitC)
	return nil
}

func (sub *Subscriber) createPartitionConsumer() ([]sarama.PartitionConsumer, error) {
	consumer, err := sarama.NewConsumer(sub.brokers, sub.o.consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed new kafka %v consumer, %w", sub.brokers, err)
	}
	sub.consumer = consumer
	partitions, err := sub.consumer.Partitions(sub.topic)
	if err != nil {
		return nil, fmt.Errorf("failed get partitions, %w", err)
	}
	partitionConsumers := make([]sarama.PartitionConsumer, 0, len(partitions))
	for _, partition := range partitions {
		offset := sub.o.consumerConfig.Consumer.Offsets.Initial
		partitionConsumer, err := sub.consumer.ConsumePartition(sub.topic, partition, offset)
		if err != nil {
			return nil, fmt.Errorf("failed consume partition, topic: %s, partition: %d, offset: %d, %w", sub.topic, partition, offset, err)
		}
		partitionConsumers = append(partitionConsumers, partitionConsumer)
	}
	return partitionConsumers, nil
}

func (sub *Subscriber) preparePartitionConsumerConsume(ctx context.Context, partitionConsumers []sarama.PartitionConsumer) <-chan struct{} {
	var wg sync.WaitGroup
	sub.msgC = make(chan *easypubsub.Message)
	sub.errC = make(chan error)
	for _, partitionConsumer := range partitionConsumers {
		wg.Add(1)
		go errorutils.WithRecover(sub.recoverHandler, sub.partitionConsumerConsume(ctx, &wg, partitionConsumer))
	}
	exit := make(chan struct{})
	go func() {
		wg.Wait()
		close(sub.msgC)
		close(sub.errC)
		sub.o.logger.Log("close message and error chan")
		close(exit)
	}()
	return exit
}

func (sub *Subscriber) partitionConsumerConsume(ctx context.Context, wg *sync.WaitGroup, partitionConsumer sarama.PartitionConsumer) func() {
	return func() {
		defer wg.Done()
		defer func() {
			sub.o.logger.Log("close partition consumer")
			if err := partitionConsumer.Close(); err != nil {
				sub.errC <- fmt.Errorf("failed close partition consumer, %w", err)
			}
		}()

		kafkaMsgC := partitionConsumer.Messages()
		kafkaErrC := partitionConsumer.Errors()
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping partition consumer")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping partition consumer")
				return
			case err, ok := <-kafkaErrC:
				if !ok {
					sub.o.logger.Log("partition consumer error's channel is closed, stopping partition consumer")
					return
				}
				sub.errC <- err
			case kafkaMsg, ok := <-kafkaMsgC:
				if !ok {
					sub.o.logger.Log("partition consumer message's channel is closed, stopping partition consumer")
					return
				}
				sub.handlerMsg(ctx, kafkaMsg, nil)
			}
		}

	}
}

// ========================  consumerGroup consume  ========================
func (sub *Subscriber) consumerGroupSubscribe(ctx context.Context) error {
	consumerGroup, err := sarama.NewConsumerGroup(sub.brokers, sub.o.groupID, sub.o.consumerConfig)
	if err != nil {
		return fmt.Errorf("failed new kafka %v consumer group %s, %w", sub.brokers, sub.o.groupID, err)
	}
	sub.consumerGroup = consumerGroup

	var wg sync.WaitGroup
	sub.msgC = make(chan *easypubsub.Message)
	sub.errC = make(chan error)
	consumerGroupHandler := consumerGroupHandler{sub: sub, ctx: ctx}
	wg.Add(1)
	go errorutils.WithRecover(sub.recoverHandler, sub.doConsumerGroupConsume(ctx, &wg, &consumerGroupHandler))
	go func() {
		wg.Wait()
		close(sub.msgC)
		close(sub.errC)
		sub.o.logger.Log("close message and error chan")
	}()
	return nil
}

func (sub *Subscriber) doConsumerGroupConsume(ctx context.Context, wg *sync.WaitGroup, handler *consumerGroupHandler) func() {
	return func() {
		defer wg.Done()
		defer func() {
			sub.o.logger.Log("close consumer group")
			if err := sub.consumerGroup.Close(); err != nil {
				sub.errC <- fmt.Errorf("failed close partition consumer, %w", err)
			}
		}()

		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping consumer group")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is done, stopping partition consumer")
				return
			default:
				err := sub.consumerGroup.Consume(ctx, []string{sub.topic}, handler)
				if err != nil {
					sub.errC <- fmt.Errorf("failed consume")
					return
				}
			}
		}

	}
}

type consumerGroupHandler struct {
	sub *Subscriber
	ctx context.Context
}

func (c *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	c.sub.o.logger.Log("consumer group session setup")
	return nil
}

func (c *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	c.sub.o.logger.Log("consumer group session cleanup")
	return nil
}

func (c *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kafkaMsgC := claim.Messages()
	for {
		select {
		case <-c.sub.closeC:
			c.sub.o.logger.Log("subscriber is closing, ConsumeClaim exit")
			return nil
		case <-c.ctx.Done():
			c.sub.o.logger.Log("context was done, ConsumeClaim exit")
			return nil
		case kafkaMsgC, ok := <-kafkaMsgC:
			if !ok {
				c.sub.o.logger.Log("kafka message chan was close, ConsumeClaim exit")
				return nil
			}
			c.sub.handlerMsg(c.ctx, kafkaMsgC, sess)
		}
	}

}

func (sub *Subscriber) handlerMsg(ctx context.Context, kafkaMsg *sarama.ConsumerMessage, sess sarama.ConsumerGroupSession) {
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
		if sess != nil {
			sess.MarkMessage(kafkaMsg, msg.Id())
		}
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

func (sub *Subscriber) recoverHandler(p interface{}) {
	sub.o.logger.Logf("recover panic %v", p)
}

func New(brokers []string, opts ...Option) easypubsub.Subscriber {
	o := defaultOptions()
	o.apply(opts...)
	sub := &Subscriber{brokers: brokers, o: o, close: NORMAL, closeC: make(chan struct{})}
	return sub
}
