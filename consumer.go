package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func consumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:40977",
		"group.id":          "kafka-go-getting-started",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
	}

	topic := "purchases"
	c.SubscribeTopics([]string{topic}, nil)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}
			fmt.Printf("Consumed event from the topic %s: key= %-10s value=%-10s\n", *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
		}
	}
	c.Close()
}
