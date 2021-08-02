package pipemiddleware

import (
	"context"
	"time"

	"github.com/soyacen/easypubsub"
	easypubsubpipe "github.com/soyacen/easypubsub/pipe"
)

// Timeout add a timeout context to msg. when handle msg, should call context.Context.Done() to know when timeout.
func Timeout(timeout time.Duration) easypubsubpipe.Interceptor {
	return func(msg *easypubsub.Message, handler easypubsubpipe.MessageHandler) error {
		ctx, cancel := context.WithTimeout(msg.Context(), timeout)
		msg.SetContext(ctx)
		defer func() { cancel() }()
		return handler(msg)
	}
}
