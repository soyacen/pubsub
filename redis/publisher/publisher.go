package redispublisher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Publisher struct {
	clientO  *clientOptions
	o        *options
	redisCli redis.UniversalClient
	close    int32
}

func (pub *Publisher) Publish(topic string, msg *easypubsub.Message) (result *easypubsub.PublishResult) {
	if atomic.LoadInt32(&pub.close) == CLOSED {
		return &easypubsub.PublishResult{Err: errors.New("publisher is closed")}
	}
	redisMsg, err := pub.o.marshalMsg(topic, msg)
	if err != nil {
		return &easypubsub.PublishResult{Err: fmt.Errorf("failed marsharl msg, %w", err)}
	}
	pub.o.logger.Logf("send message %s", msg.Id())
	redisResult, err := pub.redisCli.Publish(msg.Context(), pub.o.generateChannel(topic), redisMsg).Result()
	if err != nil {
		return &easypubsub.PublishResult{Err: err}
	}
	return &easypubsub.PublishResult{Result: redisResult}
}

func (pub *Publisher) Close() error {
	if atomic.CompareAndSwapInt32(&pub.close, NORMAL, CLOSED) {
		return pub.redisCli.Close()
	}
	return nil
}

func (pub *Publisher) String() string {
	return "RedisPublisher"
}

func New(clientOpt ClientOption, opts ...Option) (easypubsub.Publisher, error) {
	o := defaultOptions()
	o.apply(opts...)
	clientO := &clientOptions{}
	clientOpt(clientO)
	var redisCli redis.UniversalClient
	switch clientO.clientType {
	case sampleClientType:
		redisCli = redis.NewClient(clientO.sampleClientOptions)
	case failoverClientType:
		redisCli = redis.NewFailoverClient(clientO.failoverClientOptions)
	case clusterClientType:
		redisCli = redis.NewClusterClient(clientO.clusterClientOptions)
	default:
		return nil, fmt.Errorf("unknown redis client type %d", clientO.clientType)
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	_, err := redisCli.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to ping redis, %w", err)
	}
	pub := &Publisher{clientO: clientO, o: o, redisCli: redisCli}
	return pub, nil
}
