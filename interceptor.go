package easypubsub

type (
	InterceptHandler func(topic string, msg *Message) error
	Interceptor      func(topic string, msg *Message, handler InterceptHandler) error
)

func DefaultInterceptHandler(topic string, msg *Message) error {

	return nil
}

// ChainInterceptor creates a single interceptor out of a chain of many interceptors.
// Execution is done in left-to-right order
func ChainInterceptor(interceptors ...Interceptor) Interceptor {
	n := len(interceptors)

	// Dummy interceptor maintained for backward compatibility to avoid returning nil.
	if n == 0 {
		return func(topic string, msg *Message, handler InterceptHandler) error {
			return handler(topic, msg)
		}
	}
	// The degenerate case, just return the single wrapped interceptor directly.
	if n == 1 {
		return interceptors[0]
	}
	// Return a function which satisfies the interceptor interface, and which is
	// a closure over the given list of interceptors to be chained.
	return func(topic string, msg *Message, handler InterceptHandler) error {
		currHandler := handler
		// Iterate backwards through all interceptors except the first (outermost).
		// Wrap each one in a function which satisfies the handler interface, but
		// is also a closure over the `handler` parameters. Then pass
		// each pseudo-handler to the next outer interceptor as the handler to be called.
		for i := n - 1; i > 0; i-- {
			// Rebind to loop-local vars so they can be closed over.
			innerHandler, i := currHandler, i
			currHandler = func(topic string, msg *Message) error {
				return interceptors[i](topic, msg, innerHandler)
			}
		}
		// Finally return the result of calling the outermost interceptor with the
		// outermost pseudo-handler created above as its handler.
		return interceptors[0](topic, msg, currHandler)
	}
}
