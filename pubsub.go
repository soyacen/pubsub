package easypubsub

import "context"

type (
	Interceptor func(topic string, msg *Msg) error

	PublishResult struct {
		Err    error
		Result interface{}
	}

	Publisher interface {
		AddInterceptor(interceptors ...Interceptor)
		Publish(topic string, msg *Msg) (result PublishResult)
		Close() error
	}

	Subscriber interface {
		AddInterceptor(interceptors ...Interceptor)
		Subscribe(ctx context.Context, topic string) (err error)
		Messages() (msgC <-chan *Msg)
		Errors() (errC <-chan error)
		Close() error
	}
)
