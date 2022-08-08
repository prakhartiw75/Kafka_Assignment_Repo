package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	topic := "testTopic"

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "First-Group",
		Topic:   topic,
	})

	for {
		message, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}

		fmt.Printf("Recieved Message!! Offset is=%d: Key=%s Value is= %s\n", message.Offset, string(message.Key), string(message.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
