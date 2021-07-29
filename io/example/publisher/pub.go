package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/soyacen/easypubsub"
	iopublisher "github.com/soyacen/easypubsub/io/publisher"
)

var filepath = flag.String("filepath", "", "file path")

func main() {
	flag.Parse()
	if *filepath == "" {
		flag.Usage()
		return
	}

	f, err := os.OpenFile(*filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		panic(err)
	}

	var marshalMsg = func(topic string, msg *easypubsub.Message) ([]byte, error) {
		buf := bytes.NewBufferString(topic + "\n")
		header, _ := json.Marshal(msg.Header())
		buf.Write(header)
		buf.WriteString("\n")
		buf.Write(msg.Body())
		buf.WriteString("\n")
		return buf.Bytes(), nil
	}
	publisher := iopublisher.New(
		f,
		iopublisher.WithMarshalMsgFunc(marshalMsg),
	)

	for i := 0; i < 100; i++ {
		result := publisher.Publish("awesome", easypubsub.NewMessage(easypubsub.WithBody([]byte("easypubsub 牛逼"))))
		if result.Err != nil {
			panic(err)
		}
		fmt.Println(result.Result)
	}
}
