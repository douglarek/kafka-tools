package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers = flag.String("bootstrap.servers", "", "kafka bootstrap servers")
	topic            = flag.String("topic", "", "kafka topics to consumer, multiple divided by the comma")
	offset           = flag.String("offset", "latest", "auto.offset.reset")
	timeout          = flag.Duration("timeout", -1, "consumer timeout")
)

func init() {
	log.SetOutput(os.Stdout)
}

func main() {

	flag.Parse()

	if *bootstrapServers == "" || *topic == "" || *offset == "" {
		panic("both of bootstrap.servers, topic and offset should be not empty")
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  *bootstrapServers,
		"group.id":           "a-kafka-consumer",
		"auto.offset.reset":  *offset,
		"enable.auto.commit": false, // no need to commit
	})

	if err != nil {
		panic(err)
	}

	defer func() { _ = c.Close() }()

	topics := strings.Split(*topic, ",")
	if err := c.SubscribeTopics(topics, func(_ *kafka.Consumer, _ kafka.Event) error { return nil }); err != nil {
		panic(fmt.Sprintf("failed to subscribe topics: %v", topics))
	}

	log.Printf("%v have been subscribed and start to consume ...", topics)

	for {
		m, err := c.ReadMessage(*timeout)
		if err == nil {
			log.Printf("topic: %v\tpartition: %d\tkey: %s\tvalue: %s", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.Key, m.Value)
		} else {
			log.Printf("consumer error: %v\n", err)
		}
	}
}
