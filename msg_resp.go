package easypubsub

import "sync"

type (
	Response struct {
		Err    error
		Result interface{}
	}

	Responder struct {
		ackC      chan struct{}
		nackC     chan struct{}
		ackRespC  chan Response
		nackRespC chan Response
		respOnce  sync.Once
	}
)

func NewResponder(ackC chan struct{}, nackC chan struct{}, ackRespC chan Response, nackRespC chan Response) *Responder {
	return &Responder{ackC: ackC, nackC: nackC, ackRespC: ackRespC, nackRespC: nackRespC}
}

func (r *Responder) Ack() Response {
	var resp Response
	r.respOnce.Do(func() {
		close(r.ackC)
		resp = <-r.ackRespC
	})
	return resp
}

func (r *Responder) Nack() Response {
	var resp Response
	r.respOnce.Do(func() {
		close(r.nackC)
		resp = <-r.nackRespC
	})
	return resp
}
