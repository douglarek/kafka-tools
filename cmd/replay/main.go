package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	kafkaTool "github.com/douglarek/kafka-tools"
)

var (
	bootstrapServers = flag.String("bootstrap.servers", "", "kafka bootstrap servers")
	topic            = flag.String("topic", "", "kafka topics to consumer, multiple divided by the comma")
	startTime        = flag.String("start", "", "start consumer time, format: 20060102150405")
	endTime          = flag.String("end", "", "end consumer time, format: 20060102150405")
	saslProtocol     = flag.String("security.protocol", "sasl_plaintext", "security.protocol")
	saslMechanism    = flag.String("sasl.mechanism", "PLAIN", "sasl.mechanism")
	saslUser         = flag.String("sasl.username", "", "sasl.username")
	saslPass         = flag.String("sasl.password", "", "sasl.password")
)

const dateTimeFormat = "20060102150405"

func main() {
	flag.Parse()

	if *bootstrapServers == "" {
		panic("bootstrap.servers must not be empty")
	}

	if *topic == "" {
		panic("topic must not be empty")
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	start, err := time.ParseInLocation(dateTimeFormat, *startTime, loc)
	if err != nil {
		panic(fmt.Errorf("start time is valid: %v", start))
	}

	end, err := time.ParseInLocation(dateTimeFormat, *endTime, loc)
	if err != nil {
		panic(fmt.Errorf("start time is valid: %v", end))
	}

	if end.Before(start) {
		panic("end time should be greater than start time")
	}

	var c *kafkaTool.ReplayConsumer

	if *saslUser != "" || *saslPass != "" {
		c = kafkaTool.NewSASLReplayConsumer(*bootstrapServers, *topic, start, end, *saslProtocol, *saslMechanism, *saslUser, *saslPass)
	} else {
		c = kafkaTool.NewReplayConsumer(*bootstrapServers, *topic, start, end)
	}

	defer c.Close()

	var count int64
	for m := range c.Read() {
		log.Printf("topic: %v\tpartition: %d\tmessage: %v", *m.TopicPartition.Topic, m.TopicPartition.Partition, string(m.Value))
		count++
	}
	log.Println("total count:", count)
}
