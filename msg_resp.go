package easypubsub

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
	}
)

func NewResponder(ackC chan struct{}, nackC chan struct{}, ackRespC chan Response, nackRespC chan Response) *Responder {
	return &Responder{ackC: ackC, nackC: nackC, ackRespC: ackRespC, nackRespC: nackRespC}
}

func (r *Responder) Ack() Response {
	close(r.ackC)
	return <-r.ackRespC
}

func (r *Responder) Nack() Response {
	close(r.nackC)
	return <-r.nackRespC
}
