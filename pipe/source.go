package easypubsubpipe

import "github.com/soyacen/easypubsub"

type Source struct {
	topic      string
	subscriber easypubsub.Subscriber
}

func NewSource(topic string, subscriber easypubsub.Subscriber) *Source {
	return &Source{topic: topic, subscriber: subscriber}
}
