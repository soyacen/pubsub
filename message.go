package pubsub

import (
	"context"
	"io"
)

type Header interface {
	// Get adds the key, value pair to the header.
	Get(key string) string
	Set(key, value string)
	// Del deletes the value associated with key.
	Del(key string)
	// Clone returns a copy of header or nil if header is nil.
	Clone() Header
}

type AckReply interface {
}

type NackReply interface {
}

type Acker interface {
	Ack() AckReply
	Nack() NackReply
}

type Message struct {
	ctx    context.Context
	Header Header
	Body   io.ReadCloser
	Acker  Acker
}

// Context returns the message's context. To change the context, use
// SetContext.
//
// The returned context is always non-nil; it defaults to the
// background context.
func (m *Message) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

// SetContext sets provided context to the message.
func (m *Message) SetContext(ctx context.Context) {
	m.ctx = ctx
}

// Clone returns a shallow clone of c.
func (m *Message) Clone() *Message {
	msg := NewMessage(m.UUID, m.Payload)
	for k, v := range m.Metadata {
		msg.Metadata.Set(k, v)
	}
	return msg
}
