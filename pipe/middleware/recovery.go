package pipemiddleware

import (
	"fmt"

	"github.com/soyacen/easypubsub"
	easypubsubpipe "github.com/soyacen/easypubsub/pipe"
)

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(p interface{}) (err error)

// Recovery returns a middleware that recovers from any panics and calls the provided handle func to handle it.
func Recovery(recoveryHandler RecoveryHandlerFunc) easypubsubpipe.Interceptor {
	return func(msg *easypubsub.Message, handler easypubsubpipe.MessageHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoveryHandler(r)
				fmt.Println(msg.Id(), err)
			}
		}()
		err = handler(msg)
		return err
	}
}
