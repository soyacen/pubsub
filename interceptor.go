package easypubsub

type (
	InterceptHandler func(topic string, msg *Message) error
	Interceptor      func(topic string, msg *Message, handler InterceptHandler) error
)

func DefaultInterceptHandler(topic string, msg *Message) error { return nil }

// ChainInterceptor creates a single interceptor out of a chain of many interceptors.
// Execution is done in left-to-right order
func ChainInterceptor(interceptors ...Interceptor) Interceptor {
	n := len(interceptors)
	if n == 0 {
		return func(topic string, msg *Message, handler InterceptHandler) error {
			return handler(topic, msg)
		}
	}
	if n == 1 {
		return interceptors[0]
	}
	return func(topic string, msg *Message, handler InterceptHandler) error {
		currHandler := handler

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
