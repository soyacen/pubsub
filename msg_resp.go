package easypubsub

import (
	"sync"
	"sync/atomic"
)

const (
	NoAck int32 = iota
	AckedState
	NackedState
)

type (
	Response struct {
		Err    error
		Result interface{}
	}

	Responder struct {
		ackC      chan struct{}
		nackC     chan struct{}
		ackRespC  chan *Response
		nackRespC chan *Response
		respOnce  sync.Once
		ackState  int32
	}
)

func NewResponder() *Responder {
	responder := &Responder{
		ackC:      make(chan struct{}),
		nackC:     make(chan struct{}),
		ackRespC:  make(chan *Response),
		nackRespC: make(chan *Response),
		ackState:  NoAck,
	}
	return responder
}

func (r *Responder) Ack() *Response {
	if r == nil {
		return nil
	}
	var resp *Response
	r.respOnce.Do(func() {
		close(r.ackC)
		r.ackState = AckedState
		resp = <-r.ackRespC
		close(r.ackRespC)
		close(r.nackC)
		close(r.nackRespC)
	})
	return resp
}

func (r *Responder) Nack() *Response {
	if r == nil {
		return nil
	}
	var resp *Response
	r.respOnce.Do(func() {
		close(r.nackC)
		r.ackState = NackedState
		resp = <-r.nackRespC
		close(r.nackRespC)
		close(r.ackC)
		close(r.ackRespC)
	})
	return resp
}

func (r *Responder) Acked() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.ackC
}

func (r *Responder) AckResp() chan<- *Response {
	if r == nil {
		return nil
	}
	return r.ackRespC
}

func (r *Responder) Nacked() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.nackC
}

func (r *Responder) NackResp() chan<- *Response {
	if r == nil {
		return nil
	}
	return r.nackRespC
}

func (r *Responder) IsAcked() bool {
	if r == nil {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.ackState, AckedState, AckedState)
}

func (r *Responder) IsNacked() bool {
	if r == nil {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.ackState, NackedState, NackedState)
}
