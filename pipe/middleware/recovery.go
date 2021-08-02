package pipemiddleware

import (
	"fmt"

	"github.com/soyacen/easypubsub"
	easypubsubpipe "github.com/soyacen/easypubsub/pipe"
)

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(p interface{}) (err error)

// Recovery returns a middleware that recovers from any panics.
func Recovery() easypubsubpipe.Interceptor {
	return CustomRecovery(DefaultRecoveryHandlerFunc)
}

// CustomRecovery returns a middleware that recovers from any panics and calls the provided handle func to handle it.
func CustomRecovery(recoveryHandler RecoveryHandlerFunc) easypubsubpipe.Interceptor {
	return func(msg *easypubsub.Message, handler easypubsubpipe.MessageHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoveryHandler(r)
			}
		}()
		err = handler(msg)
		return err
	}
}

func DefaultRecoveryHandlerFunc(p interface{}) (err error) {
	err, ok := p.(error)
	if ok {
		return err
	}
	return fmt.Errorf("%v", p)
}
