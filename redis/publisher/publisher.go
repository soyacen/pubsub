package redispublisher

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/go-redis/redis/v8"

	"github.com/soyacen/easypubsub"
)

const (
	NORMAL = 0
	CLOSED = 1
)

type Publisher struct {
	o        *options
	redisCli redis.UniversalClient
	close    int32
	clientO  *clientOptions
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
	pub := &Publisher{o: o, clientO: clientO, redisCli: redisCli}
	return pub, nil
}
