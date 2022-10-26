package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers = flag.String("bootstrap.servers", "", "kafka bootstrap servers")
	topic            = flag.String("topic", "", "kafka topics to consumer, multiple divided by the comma")
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

	if *bootstrapServers == "" || *topic == "" {
		panic("both of bootstrap.servers, topic and offset should be not empty")
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers": *bootstrapServers,
		// "enable.idempotence": true, // idempotent producer which provides strict ordering and and exactly-once producer guarantees. see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .
	}
	if *saslUser != "" && *saslPass != "" {
		_ = cfg.SetKey("security.protocol", *saslProtocol)
		_ = cfg.SetKey("sasl.mechanism", *saslMechanism)
		_ = cfg.SetKey("sasl.username", *saslUser)
		_ = cfg.SetKey("sasl.password", *saslPass)
	}

	p, err := kafka.NewProducer(cfg)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				log.Printf("Error: %v\n", ev)
			default:
				log.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var msgcnt int
	run := true
	for run {

		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			value := fmt.Sprintf("Producer example, message #%d", msgcnt)

			m := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
				Value:          []byte(value),
				Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
			}
			if err = p.Produce(m, nil); err != nil {
				if err.(kafka.Error).Code() == kafka.ErrQueueFull {
					time.Sleep(time.Second)
					continue
				}
				log.Printf("Failed to produce message: %v\n", err)
			}
			msgcnt++
		}
	}

	for p.Flush(10000) > 0 {
		log.Print("Still waiting to flush outstanding messages\n", err)
	}
}
