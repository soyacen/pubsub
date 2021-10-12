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

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	consumerO          *consumerOptions
	o                  *options
	topic              string
	closed             int32
	closeC             chan struct{}
	consumerGroup      sarama.ConsumerGroup
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	errC               chan error
	msgC               chan *easypubsub.Message
}

func (sub *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *easypubsub.Message, <-chan error) {
	sub.errC = make(chan error)
	sub.msgC = make(chan *easypubsub.Message)
	if atomic.LoadInt32(&sub.closed) == CLOSED {
		go sub.closeErrCAndMsgC(errors.New("subscriber is closed"))
		return sub.msgC, sub.errC
	}
	sub.topic = topic
	err := sub.subscribe(ctx)
	if err != nil {
		go sub.closeErrCAndMsgC(err)
		return sub.msgC, sub.errC
	}
	return sub.msgC, sub.errC
}

func (sub *Subscriber) Close() error {
	if atomic.CompareAndSwapInt32(&sub.closed, NORMAL, CLOSED) {
		close(sub.closeC)
	}
	return nil
}

func (sub *Subscriber) String() string {
	return "KafkaSubscriber"
}

func (sub *Subscriber) subscribe(ctx context.Context) error {
	switch sub.consumerO.consumerType {
	case consumerConsumerType:
		return sub.consumerSubscribe(ctx)
	case consumerGroupConsumerType:
		return sub.consumerGroupSubscribe(ctx)
	default:
		return fmt.Errorf("unknown consumer type %d", sub.consumerO.consumerType)
	}
}

// ========================  consumer consume  ========================
func (sub *Subscriber) consumerSubscribe(ctx context.Context) error {
	sub.o.logger.Log("start consumer subscribe")
	if err := sub.createConsumer(); err != nil {
		return err
	}
	sub.o.logger.Log("prepare partition consumer consume")
	exitC := sub.waitConsumerConsume(ctx)
	go errorutils.WithRecover(sub.recoverHandler, sub.consumerConsumeDaemon(ctx, exitC))
	return nil
}

func (sub *Subscriber) createConsumer() error {
	sub.o.logger.Logf("create broker %v consumer", sub.consumerO.brokers)
	consumer, err := sarama.NewConsumer(sub.consumerO.brokers, sub.consumerO.consumerConfig)
	if err != nil {
		return fmt.Errorf("failed new kafka %v consumer, %w", sub.consumerO.brokers, err)
	}
	sub.consumer = consumer

	partitions, err := sub.consumer.Partitions(sub.topic)
	if err != nil {
		return fmt.Errorf("failed get partitions, %w", err)
	}
	sub.o.logger.Logf("get topic %s partitions %v  ", sub.topic, partitions)

	partitionConsumers := make([]sarama.PartitionConsumer, 0, len(partitions))
	for _, partition := range partitions {
		offset := sub.consumerO.consumerConfig.Consumer.Offsets.Initial
		sub.o.logger.Logf("create partition %d consumer, offset %d", partition, offset)
		partitionConsumer, err := sub.consumer.ConsumePartition(sub.topic, partition, offset)
		if err != nil {
			return fmt.Errorf("failed consume partition, topic: %s, partition: %d, offset: %d, %w", sub.topic, partition, offset, err)
		}
		partitionConsumers = append(partitionConsumers, partitionConsumer)
	}
	sub.partitionConsumers = partitionConsumers
	return nil
}

func (sub *Subscriber) waitConsumerConsume(ctx context.Context) <-chan struct{} {
	var wg sync.WaitGroup
	for _, partitionConsumer := range sub.partitionConsumers {
		wg.Add(1)
		go errorutils.WithRecover(sub.recoverHandler, sub.consumerConsume(ctx, &wg, partitionConsumer))
	}
	exitC := make(chan struct{})
	go func() {
		wg.Wait()
		sub.o.logger.Log("closing kafka consumer")
		err := sub.consumer.Close()
		if err != nil {
			sub.o.logger.Logf("failed close consumer, %v", err)
			sub.errC <- fmt.Errorf("failed close consumer, %w", err)
		}
		close(exitC)
	}()
	return exitC
}

func (sub *Subscriber) consumerConsumeDaemon(ctx context.Context, exitC <-chan struct{}) func() {
	return func() {
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping subscribe")
				go sub.closeErrCAndMsgC(nil)
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping subscribe")
				go sub.closeErrCAndMsgC(nil)
				return
			case <-exitC:
				sub.o.logger.Logf("consumer consume is exited")
				for i := 0; ; i++ {
					reconnectInterval := sub.o.reconnectBackoff(ctx, uint(i))
					sub.o.logger.Logf("wait %s to reconnect to kafka", reconnectInterval)
					time.Sleep(reconnectInterval)
					if err := sub.createConsumer(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					sub.o.logger.Log("prepare partition consumer consume")
					exitC = sub.waitConsumerConsume(ctx)
					break
				}

			}
		}
	}
}

func (sub *Subscriber) consumerConsume(ctx context.Context, wg *sync.WaitGroup, partitionConsumer sarama.PartitionConsumer) func() {
	return func() {
		sub.o.logger.Log("start partition consumer consume")
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
				sub.o.logger.Log("subscriber is closing, stopping consumer consume")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping consumer consume")
				return
			case err, ok := <-kafkaErrC:
				if !ok {
					sub.o.logger.Log("consumer error's channel is closed, stopping consumer consume")
					return
				}
				sub.o.logger.Log(err.Error())
				sub.errC <- err
				return
			case kafkaMsg, ok := <-kafkaMsgC:
				if !ok {
					sub.o.logger.Log("partition consumer message's channel is closed, stopping consumer consume")
					return
				}
				sub.handlerMsg(ctx, kafkaMsg, nil)
			}
		}

	}
}

// ========================  consumerGroup consume  ========================

func (sub *Subscriber) consumerGroupSubscribe(ctx context.Context) error {
	sub.o.logger.Log("start consumer group subscribe")
	if err := sub.createConsumerGroup(); err != nil {
		return err
	}
	exitC := sub.waitConsumerGroupConsume(ctx)
	go errorutils.WithRecover(sub.recoverHandler, sub.consumerGroupConsumeDaemon(ctx, exitC))
	return nil
}

func (sub *Subscriber) createConsumerGroup() error {
	sub.o.logger.Logf("create broker %v consumer group %s", sub.consumerO.brokers, sub.consumerO.groupID)
	consumerGroup, err := sarama.NewConsumerGroup(sub.consumerO.brokers, sub.consumerO.groupID, sub.consumerO.consumerConfig)
	if err != nil {
		return fmt.Errorf("failed new kafka %v consumer group %s, %w", sub.consumerO.brokers, sub.consumerO.groupID, err)
	}
	sub.consumerGroup = consumerGroup
	return nil
}

func (sub *Subscriber) waitConsumerGroupConsume(ctx context.Context) <-chan struct{} {
	var wg sync.WaitGroup
	consumerGroupHandler := consumerGroupHandler{sub: sub, ctx: ctx}
	wg.Add(1)
	go errorutils.WithRecover(sub.recoverHandler, sub.consumerGroupConsume(ctx, &wg, &consumerGroupHandler))
	exitC := make(chan struct{})
	go func() {
		wg.Wait()
		sub.o.logger.Log("closing kafka consumer group")
		err := sub.consumerGroup.Close()
		if err != nil {
			sub.o.logger.Logf("failed close consumer group, %v", err)
			sub.errC <- fmt.Errorf("failed close consumer group, %w", err)
		}
		close(exitC)
	}()
	return exitC
}

func (sub *Subscriber) consumerGroupConsumeDaemon(ctx context.Context, exitC <-chan struct{}) func() {
	return func() {
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping subscribe")
				go sub.closeErrCAndMsgC(nil)
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping subscribe")
				go sub.closeErrCAndMsgC(nil)
				return
			case <-exitC:
				sub.o.logger.Logf("consumerGroup consume is exited")
				for i := 0; ; i++ {
					reconnectInterval := sub.o.reconnectBackoff(ctx, uint(i))
					sub.o.logger.Logf("wait %s to reconnect to kafka", reconnectInterval)
					time.Sleep(reconnectInterval)
					if err := sub.createConsumerGroup(); err != nil {
						sub.o.logger.Log(err.Error())
						sub.errC <- err
						continue
					}
					exitC = sub.waitConsumerGroupConsume(ctx)
				}
			}
		}
	}
}

func (sub *Subscriber) consumerGroupConsume(ctx context.Context, wg *sync.WaitGroup, handler *consumerGroupHandler) func() {
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
				sub.o.logger.Log("start consumer group consume")
				err := sub.consumerGroup.Consume(ctx, []string{sub.topic}, handler)
				if err != nil {
					sub.o.logger.Logf("failed consume, %v", err)
					sub.errC <- fmt.Errorf("failed consume, %w", err)
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
	defer sess.Commit()
	kafkaMsgC := claim.Messages()
	for {
		select {
		case <-c.sub.closeC:
			c.sub.o.logger.Log("subscriber is closing, stopping ConsumeClaim")
			return nil
		case <-c.ctx.Done():
			c.sub.o.logger.Log("context was done, stopping ConsumeClaim")
			return nil
		case kafkaMsgC, ok := <-kafkaMsgC:
			if !ok {
				c.sub.o.logger.Log("kafka message chan was close, stopping ConsumeClaim")
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

	var attempt uint
HandleMsg:
	msg.Responder = easypubsub.NewResponder()
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
		if attempt >= sub.o.nackResendMaxAttempt {
			sub.o.logger.Logf("had resent %dth and failed handle message@%s/%d/%d, skip now", sub.o.nackResendMaxAttempt, kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset)
			return
		}
		attempt++
		interval := sub.o.nackResendBackoff(ctx, attempt)
		sub.o.logger.Logf("this is %dth resend, wait %s", attempt, interval)
		time.Sleep(interval)
		sub.o.logger.Logf("message %s resend", msg.Id())
		goto HandleMsg
	}
}

func (sub *Subscriber) recoverHandler(p interface{}) {
	sub.o.logger.Logf("recover panic %v", p)
}

func (sub *Subscriber) closeErrCAndMsgC(err error) {
	if err != nil {
		sub.errC <- err
	}
	close(sub.errC)
	close(sub.msgC)
}

func New(consumerOpt ConsumerOption, opts ...Option) easypubsub.Subscriber {
	consumerO := defaultConsumerOptions()
	consumerOpt(consumerO)

	o := defaultOptions()
	o.apply(opts...)

	sub := &Subscriber{
		consumerO: consumerO,
		o:         o,
		closed:    NORMAL,
		closeC:    make(chan struct{}),
	}
	return sub
}
