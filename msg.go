package easypubsub

import (
	"context"
)

type Msg struct {
	ctx       context.Context
	header    Header
	body      []byte
	responder *Responder
}

func (m *Msg) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *Msg) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *Msg) Header() Header {
	return m.header
}

func (m *Msg) SetHeader(header Header) {
	m.header = header
}

func (m *Msg) Body() []byte {
	return m.body
}

func (m *Msg) SetBody(body []byte) {
	m.body = body
}

func (m *Msg) Responder() *Responder {
	return m.responder
}

func (m *Msg) SetResponder(responder *Responder) {
	m.responder = responder
}

func (m *Msg) Clone() *Msg {
	msg := &Msg{
		header: m.header.Clone(),
		body:   append([]byte{}, m.body...),
	}
	return msg
}

type MessageOption func(msg *Msg)

func WithContext(ctx context.Context) MessageOption {
	return func(msg *Msg) {
		msg.ctx = ctx
	}
}

func WithHeader(header map[string]string) MessageOption {
	return func(msg *Msg) {
		for key, val := range header {
			msg.header.Set(key, val)
		}
	}
}

func WithBody(body []byte) MessageOption {
	return func(msg *Msg) {
		msg.body = body
	}
}

//func WithResponder(responder *Responder) MessageOption {
//	return func(msg *Msg) {
//		msg.responder = responder
//	}
//}

func NewMsg(opts ...MessageOption) *Msg {
	msg := &Msg{
		header: make(Header),
	}
	for _, opt := range opts {
		opt(msg)
	}
	return msg
}
