package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "example-123",
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion: func(messages []kafka.Message, err error) {

			if err != nil {
				fmt.Println(err)
				return
			}

			for _, val := range messages {
				fmt.Printf("Messages sent to Kafka Server with key=%s and value=%s. Offset=%d \n", val.Key, val.Value, val.Offset)
			}
		},
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("FirstKey"),
			Value: []byte("FirstValue"),
		},
		kafka.Message{
			Key:   []byte("SecondKey"),
			Value: []byte("SecondValue"),
		},
		kafka.Message{
			Key:   []byte("ThirdKey"),
			Value: []byte("ThirdValue"),
		},
	)

	if err != nil {
		log.Fatal("Can't write messages: Error=", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("Can't close writer: Error=", err)
	}
}
