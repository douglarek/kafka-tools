package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers = flag.String("bootstrap.servers", "", "kafka bootstrap servers")
	topic            = flag.String("topic", "", "kafka topics to consumer, multiple divided by the comma")
	groupID          = flag.String("group.id", "a-kafka-consumer", "kafka consumer group id")
	offset           = flag.String("offset", "latest", "auto.offset.reset")
	timeout          = flag.Duration("timeout", -1, "consumer timeout")
	saslProtocol     = flag.String("security.protocol", "sasl_plaintext", "security.protocol")
	saslMechanism    = flag.String("sasl.mechanism", "PLAIN", "sasl.mechanism")
	saslUser         = flag.String("sasl.username", "", "sasl.username")
	saslPass         = flag.String("sasl.password", "", "sasl.password")
)

func init() {
	log.SetOutput(os.Stdout)
}

func main() {

	flag.Parse()

	if *bootstrapServers == "" || *topic == "" || *groupID == "" || *offset == "" {
		panic("both of bootstrap.servers, topic, group.id and offset should be not empty")
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        *bootstrapServers,
		"group.id":                 *groupID,
		"auto.offset.reset":        *offset,
		"enable.auto.commit":       false, // no need to commit for stream consuming
		"enable.auto.offset.store": false,
	}
	if *saslUser != "" && *saslPass != "" {
		_ = cfg.SetKey("security.protocol", *saslProtocol)
		_ = cfg.SetKey("sasl.mechanism", *saslMechanism)
		_ = cfg.SetKey("sasl.username", *saslUser)
		_ = cfg.SetKey("sasl.password", *saslPass)
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		panic(err)
	}

	defer func() { _ = c.Close() }()

	topics := strings.Split(*topic, ",")
	if err := c.SubscribeTopics(topics, func(_ *kafka.Consumer, _ kafka.Event) error { return nil }); err != nil {
		panic(fmt.Sprintf("failed to subscribe topics: %v", topics))
	}

	log.Printf("%v have been subscribed and start to consume ...", topics)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	/*for {
		m, err := c.ReadMessage(*timeout)
		if err == nil {
			log.Printf("topic: %v\tpartition: %d\tkey: %s\tvalue: %s", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.Key, m.Value)
		} else {
			log.Printf("consumer error: %v\n", err)
		}
	}*/

	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(int(*timeout / time.Millisecond))
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("%% Message on %s:\nk: %s\nv: %s\n", e.TopicPartition, string(e.Key), string(e.Value))
				if e.Headers != nil {
					log.Printf("%% Headers: %v\n", e.Headers)
				}
				/*_, err := c.StoreMessage(e)
				if err != nil {
					log.Printf("%% Error storing offset after message %s:\n", e.TopicPartition)
				}*/
			case kafka.Error:
				log.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}
	log.Printf("Closing consumer\n")
}
