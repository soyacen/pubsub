package easypubsub

import (
	"context"

	"github.com/google/uuid"
)

type Message struct {
	id     string
	ctx    context.Context
	header Header
	body   []byte
	*Responder
}

func (msg *Message) Id() string {
	return msg.id
}

func (msg *Message) Context() context.Context {
	if msg.ctx != nil {
		return msg.ctx
	}
	return context.Background()
}

func (msg *Message) SetContext(ctx context.Context) {
	msg.ctx = ctx
}

func (msg *Message) Header() Header {
	return msg.header
}

func (msg *Message) SetHeader(header Header) {
	msg.header = header
}

func (msg *Message) Body() []byte {
	return msg.body
}

func (msg *Message) SetBody(body []byte) {
	msg.body = body
}

func (msg *Message) Clone() *Message {
	body := make([]byte, len(msg.body))
	copy(body, msg.Body())
	out := &Message{
		id:     uuid.New().String(),
		header: msg.header.Clone(),
		body:   body,
	}
	return out
}

type MessageOption func(msg *Message)

func WithContext(ctx context.Context) MessageOption {
	return func(msg *Message) {
		msg.ctx = ctx
	}
}

func WithHeader(header map[string]string) MessageOption {
	return func(msg *Message) {
		for key, val := range header {
			msg.header.Set(key, val)
		}
	}
}

func WithBody(body []byte) MessageOption {
	return func(msg *Message) {
		msg.body = body
	}
}

func NewMessage(opts ...MessageOption) *Message {
	msg := &Message{
		header: make(Header),
		id:     uuid.New().String(),
	}
	for _, opt := range opts {
		opt(msg)
	}
	return msg
}
