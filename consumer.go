package kafka_tools

import (
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ReplayConsumer struct {
	c            *kafka.Consumer
	topic        string
	start, end   time.Time
	partitionNum int
}

func newReplayConsumer(topic string, start, end time.Time, config *kafka.ConfigMap) *ReplayConsumer {
	c, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	if err := c.Subscribe(topic, nil); err != nil {
		panic(err)
	}

	return &ReplayConsumer{
		c:     c,
		topic: topic,
		start: start,
		end:   end,
	}
}

func NewReplayConsumer(brokers, topic string, start, end time.Time) *ReplayConsumer {
	config := &kafka.ConfigMap{
		"metadata.broker.list":            brokers,
		"group.id":                        "bigdata-replay-consumer",
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        true,
		"enable.partition.eof":            true,
	}

	return newReplayConsumer(topic, start, end, config)
}

func NewSASLReplayConsumer(brokers, topic string, start, end time.Time, saslProtocol, saslMechanism, saslUser, saslPass string) *ReplayConsumer {
	config := &kafka.ConfigMap{
		"metadata.broker.list":            brokers,
		"group.id":                        "bigdata-replay-consumer",
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        true,
		"enable.partition.eof":            true,
		"security.protocol":               saslProtocol,
		"sasl.mechanism":                  saslMechanism,
		"sasl.username":                   saslUser,
		"sasl.password":                   saslPass,
	}

	return newReplayConsumer(topic, start, end, config)
}

func (consumer *ReplayConsumer) offsetsForTimes(partitions []kafka.TopicPartition, timestamp int64) ([]kafka.TopicPartition, error) {
	var times []kafka.TopicPartition
	for _, p := range partitions {
		times = append(times, kafka.TopicPartition{Topic: p.Topic, Partition: p.Partition, Offset: kafka.Offset(timestamp)})
	}

	offsets, err := consumer.c.OffsetsForTimes(times, 5000)
	if err != nil {
		log.Printf("Failed to reset offsets to supplied timestamp due to error: %v\n", err)
		return partitions, err
	}

	return offsets, nil
}

func (consumer *ReplayConsumer) partitionNumbers(pars []kafka.TopicPartition) string {
	var pNums string
	for i, par := range pars {
		if i == len(pars)-1 {
			pNums = pNums + strconv.Itoa(int(par.Partition))
		} else {
			pNums = pNums + strconv.Itoa(int(par.Partition)) + ", "
		}
	}

	return pNums
}

func (consumer *ReplayConsumer) Read() <-chan *kafka.Message {
	var ends int
	ch := make(chan *kafka.Message, 100)

	go func() {
		defer close(ch)
	LOOP:
		for ev := range consumer.c.Events() {
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				p := e.Partitions
				if len(p) == 0 {
					log.Println("No partitions assigned")
					break LOOP
				}
				consumer.partitionNum = len(e.Partitions)

				log.Printf("Assigned/Re-assigned Partitions: %s\n", consumer.partitionNumbers(p))
				p, err := consumer.offsetsForTimes(e.Partitions, consumer.start.UnixNano()/int64(time.Millisecond))
				if err != nil {
					log.Printf("Trying to reset offsets to timestamp error: %v\n", err)
					break LOOP
				}

				consumer.c.Assign(p)
			case kafka.RevokedPartitions:
				consumer.c.Unassign()
			case *kafka.Message:
				if e.Timestamp.After(consumer.end) {
					continue
				}
				ch <- e
			case kafka.PartitionEOF: // no more messages
				ends++
				if ends < consumer.partitionNum {
					continue
				} else {
					break LOOP
				}
			case kafka.Error:
				log.Printf("Kafka error: %v\n", e)
				break LOOP
			}
		}
	}()

	return ch
}

func (consumer *ReplayConsumer) Close() error {
	return consumer.c.Close()
}
