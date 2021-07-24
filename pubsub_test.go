package easypubsub_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	easypubsub "github.com/soyacen/pubsub"
)

func TestChainInterceptor(t *testing.T) {
	first := func(topic string, msg *easypubsub.Message, handler easypubsub.MsgHandler) error {
		require.Equal(t, "interceptor", topic)
		require.Equal(t, []byte("interceptor"), msg.Body())
		requireContextValue(t, msg.Context(), "origin")

		newTopic := topic + ":first"
		msg.SetContext(context.WithValue(msg.Context(), "first", "true"))
		msg.Header().Set("first", "true")
		msg.SetBody(append(msg.Body(), ",first"...))
		return handler(newTopic, msg)
	}
	second := func(topic string, msg *easypubsub.Message, handler easypubsub.MsgHandler) error {
		require.Equal(t, "interceptor:first", topic)
		require.Equal(t, []byte("interceptor,first"), msg.Body())
		pairs, _ := easypubsub.NewHeaderWithPairs("first", "true")
		require.Equal(t, pairs, msg.Header())
		requireContextValue(t, msg.Context(), "origin")
		requireContextValue(t, msg.Context(), "first")

		newTopic := topic + ":second"
		msg.SetContext(context.WithValue(msg.Context(), "second", "true"))
		msg.Header().Set("second", "true")
		msg.SetBody(append(msg.Body(), ",second"...))
		return handler(newTopic, msg)
	}
	handler := func(topic string, msg *easypubsub.Message) error {
		require.Equal(t, "interceptor:first:second", topic)
		require.Equal(t, []byte("interceptor,first,second"), msg.Body())
		pairs, _ := easypubsub.NewHeaderWithPairs("first", "true", "second", "true")
		require.Equal(t, pairs, msg.Header())
		requireContextValue(t, msg.Context(), "origin")
		requireContextValue(t, msg.Context(), "first")
		requireContextValue(t, msg.Context(), "second")
		return nil
	}
	interceptor := easypubsub.ChainInterceptor(first, second)
	message := easypubsub.NewMessage(
		easypubsub.WithBody([]byte("interceptor")),
		easypubsub.WithContext(context.WithValue(context.Background(), "origin", "true")),
	)
	err := interceptor("interceptor", message, handler)
	require.Nil(t, err)
}

func requireContextValue(t *testing.T, ctx context.Context, key string, msg ...interface{}) {
	val := ctx.Value(key)
	require.NotNil(t, val, msg...)
	require.Equal(t, "true", val, msg...)
}
