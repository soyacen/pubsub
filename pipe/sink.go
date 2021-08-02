package easypubsubpipe

import "github.com/soyacen/easypubsub"

type Sink struct {
	topic     string
	publisher easypubsub.Publisher
}

func NewSink(topic string, publisher easypubsub.Publisher) *Sink {
	return &Sink{topic: topic, publisher: publisher}
}
