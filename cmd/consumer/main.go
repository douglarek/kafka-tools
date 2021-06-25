package main

import (
	"flag"
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers = flag.String("bootstrap.servers", "", "kafka bootstrap servers")
	topic            = flag.String("topic", "", "kafka topics to consumer, multiple divided by the comma")
)

func main() {

	flag.Parse()

	if *bootstrapServers == "" {
		panic("bootstrap.servers must not be empty")
	}
	if *topic == "" {
		panic("topic must not be empty")
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *bootstrapServers,
		"group.id":          "bigdata-replay-consumer",
		"auto.offset.reset": "latest",
	})

	if err != nil {
		panic(err)
	}
	defer func() { log.Println(c.Close()) }()

	topics := strings.Split(*topic, ",")
	_ = c.SubscribeTopics(topics, func(consumer *kafka.Consumer, event kafka.Event) error {
		log.Println(event)
		return nil
	})

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}
