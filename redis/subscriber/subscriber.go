package redissubscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/soyacen/goutils/errorutils"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Subscriber struct {
	clientO     *clientOptions
	o           *options
	topic       string
	closed      int32
	closeC      chan struct{}
	errC        chan error
	msgC        chan *easypubsub.Message
	redisCli    redis.UniversalClient
	redisPubSub *redis.PubSub
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
	if sub.o.patternSubscribeEnabled {
		return sub.patternSubscribe(ctx)
	} else {
		return sub.sampleSubscribe(ctx)
	}
}

func (sub *Subscriber) createRedisClient(ctx context.Context) error {
	switch sub.clientO.clientType {
	default:
		return fmt.Errorf("unknown redis client type %d", sub.clientO.clientType)
	case sampleClientType:
		sub.o.logger.Logf("create sample redis client")
		sub.redisCli = redis.NewClient(sub.clientO.sampleClientOptions)
	case failoverClientType:
		sub.o.logger.Logf("create failover redis client")
		sub.redisCli = redis.NewFailoverClient(sub.clientO.failoverClientOptions)
	case clusterClientType:
		sub.o.logger.Logf("create cluster redis client")
		sub.redisCli = redis.NewClusterClient(sub.clientO.clusterClientOptions)
	}
	ctx, _ = context.WithTimeout(ctx, time.Second)
	_, err := sub.redisCli.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to ping redis, %w", err)
	}
	return nil
}

func (sub *Subscriber) sampleSubscribe(ctx context.Context) error {
	sub.o.logger.Log("start sample subscribe")
	if err := sub.createSampleSubscriber(ctx); err != nil {
		return err
	}
	sub.o.logger.Log("prepare consume")
	exitC := sub.waitConsume(ctx)
	go errorutils.WithRecover(sub.recoverHandler, sub.consumeDaemon(ctx, exitC))
	return nil
}

func (sub *Subscriber) createSampleSubscriber(ctx context.Context) error {
	if err := sub.createRedisClient(ctx); err != nil {
		return err
	}
	sub.o.logger.Logf("create sample subscriber")
	sub.redisPubSub = sub.redisCli.Subscribe(ctx)
	channel := sub.o.generateChannel(sub.topic)
	err := sub.redisPubSub.Subscribe(ctx, channel)
	if err != nil {
		return fmt.Errorf("failed to subscribe channel %s, %w", channel, err)
	}
	return nil
}

func (sub *Subscriber) patternSubscribe(ctx context.Context) error {
	sub.o.logger.Log("start pattern subscribe")
	if err := sub.createPatternSubscriber(ctx); err != nil {
		return err
	}
	sub.o.logger.Log("prepare consume")
	exitC := sub.waitConsume(ctx)
	go errorutils.WithRecover(sub.recoverHandler, sub.consumeDaemon(ctx, exitC))
	return nil
}

func (sub *Subscriber) createPatternSubscriber(ctx context.Context) error {
	if err := sub.createRedisClient(ctx); err != nil {
		return err
	}
	sub.o.logger.Logf("create pattern subscriber")
	sub.redisPubSub = sub.redisCli.PSubscribe(ctx)
	pattern := sub.o.generatePattern(sub.topic)
	err := sub.redisPubSub.PSubscribe(ctx, pattern)
	if err != nil {
		return fmt.Errorf("failed to psubscribe pattern %s, %w", pattern, err)
	}
	return nil
}

func (sub *Subscriber) waitConsume(ctx context.Context) <-chan struct{} {
	var wg sync.WaitGroup
	wg.Add(1)
	go errorutils.WithRecover(sub.recoverHandler, sub.consume(ctx, &wg))
	exitC := make(chan struct{})
	go func() {
		wg.Wait()
		sub.o.logger.Log("closing redis pubsub")
		if err := sub.redisPubSub.Close(); err != nil {
			sub.errC <- fmt.Errorf("failed to close redis pubsub, %w", err)
		}
		if err := sub.redisCli.Close(); err != nil {
			sub.errC <- fmt.Errorf("failed to close redis client, %w", err)
		}
		close(exitC)
	}()
	return exitC
}

func (sub *Subscriber) consumeDaemon(ctx context.Context, exitC <-chan struct{}) func() {
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
				attempts := 0
				for {
					if attempts >= int(sub.o.reSubMaxAttempt) {
						go sub.closeErrCAndMsgC(errors.New("failed to create redis pubsub"))
						return
					}
					attempts++
					rePubSubInterval := sub.o.resubBackoff(ctx, uint(attempts))
					sub.o.logger.Logf("wait %s to resub", rePubSubInterval)
					time.Sleep(rePubSubInterval)
					if sub.o.patternSubscribeEnabled {
						if err := sub.createPatternSubscriber(ctx); err != nil {
							sub.errC <- err
							continue
						}
					} else {
						if err := sub.createSampleSubscriber(ctx); err != nil {
							sub.errC <- err
							continue
						}
					}
					sub.o.logger.Log("prepare consume")
					exitC = sub.waitConsume(ctx)
					break
				}

			}
		}
	}
}

func (sub *Subscriber) consume(ctx context.Context, wg *sync.WaitGroup) func() {
	return func() {
		sub.o.logger.Log("start consume")
		defer wg.Done()
		defer func() {
			if sub.o.patternSubscribeEnabled {
				if err := sub.redisPubSub.PUnsubscribe(ctx, sub.o.generatePattern(sub.topic)); err != nil {
					sub.errC <- fmt.Errorf("failed to PUnsubscribe, %w", err)
				}
				return
			}
			if err := sub.redisPubSub.Unsubscribe(ctx, sub.o.generateChannel(sub.topic)); err != nil {
				sub.errC <- fmt.Errorf("failed to Unsubscribe, %w", err)
			}
			return
		}()
		redisMsgC := sub.redisPubSub.Channel()
		for {
			select {
			case <-sub.closeC:
				sub.o.logger.Log("subscriber is closing, stopping consume")
				return
			case <-ctx.Done():
				sub.o.logger.Log("context is Done, stopping consume")
				return
			case redisMsg, ok := <-redisMsgC:
				if !ok {
					sub.o.logger.Log("redis message's channel is closed, stopping consume")
					return
				}
				sub.handlerMsg(ctx, redisMsg)
			}
		}

	}
}

func (sub *Subscriber) handlerMsg(ctx context.Context, redisMsg *redis.Message) {
	msg, err := sub.o.unmarshalMsg(ctx, sub.topic, redisMsg)
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
		sub.o.logger.Logf("message %s acked", msg.Id())
		return
	case <-msg.Nacked():
		msg.NackResp() <- &easypubsub.Response{Result: "ok"}
		sub.o.logger.Logf("message %s nacked", msg.Id())
		if attempt >= sub.o.nackResendMaxAttempt {
			sub.o.logger.Logf("had resent %dth and failed handle message@%s/%s, skip now", sub.o.nackResendMaxAttempt, redisMsg.Channel, redisMsg.Pattern)
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

func New(clientOpt ClientOption, opts ...Option) easypubsub.Subscriber {
	o := defaultOptions()
	o.apply(opts...)
	clientO := &clientOptions{}
	clientOpt(clientO)
	sub := &Subscriber{
		clientO: clientO,
		o:       o,
		closed:  NORMAL,
		closeC:  make(chan struct{}),
	}
	return sub
}
