# Easypubsub
Easypubsub implements the Pub/Sub message flow for Golang.
Pub/Sub allows services to communicate asynchronously.
Easypubsub also can build event driven applications.

# Why Pub/Sub
* Pub/Sub allows services to communicate asynchronously, with latencies on the order of 100 milliseconds.

* Pub/Sub is used for streaming analytics and data integration pipelines to ingest and distribute data. It is equally effective as messaging-oriented middleware for service integration or as a queue to parallelize tasks.

* Pub/Sub enables you to create systems of event producers and consumers, called publishers and subscribers. Publishers communicate with subscribers asynchronously by broadcasting events, rather than by synchronous remote procedure calls (RPCs).

* Publishers send events to the Pub/Sub service, without regard to how or when these events will be processed. Pub/Sub then delivers events to all services that need to react to them. Compared to systems communicating through RPCs, where publishers must wait for subscribers to receive the data, such asynchronous integration increases the flexibility and robustness of the system overall.


# Pub/Subs
Easypubsub provides universal interfaces for publisher and subscriber
```go
type (
    PublishResult struct {
        Err    error
        Result interface{}
    }
    
    Publisher interface {
        Publish(topic string, msg *Message) (result *PublishResult)
        io.Closer
        fmt.Stringer
    }
)

type Subscriber interface {
    Subscribe(ctx context.Context, topic string) (<-chan *Message, <-chan error)
    io.Closer
    fmt.Stringer
}
```

# Pipe
Pipe combines subscriber and Publisher, abstracts source and sink to process data streams,
* [copy](pipe/example/copy)

# Examples
## kafka
* [Kafka publisher](kafka/example/publisher)
* [Kafka subscriber](kafka/example/subscriber)

## rabbitmq
* [rabbitmq publisher](rabbitmq/example/publisher)
* [rabbitmq subscriber](rabbitmq/example/subscriber)

## io
* [io publisher](io/example/publisher)
* [io subscriber](io/example/subscriber)

...


