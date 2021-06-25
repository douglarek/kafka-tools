package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"time"

	kafkaTool "github.com/douglarek/kafka-tools"
)

var (
	dtsBrokers     = flag.String("dts.bootstrap.servers", "", "dts kafka bootstrap servers")
	maxwellBrokers = flag.String("maxwell.bootstrap.servers", "", "maxwell kafka bootstrap servers")
	topic          = flag.String("topic", "", "kafka topic to consumer")
	startTime      = flag.String("start", "", "start consumer time, format: 20060102150405")
	endTime        = flag.String("end", "", "end consumer time, format: 20060102150405")
)

const (
	dateTimeFormat = "20060102150405"
)

type MessageData map[string]interface{}

func (d MessageData) String() string {
	var keys []string
	for k := range d {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteRune('=')
		if reflect.TypeOf(d[k]).Kind() == reflect.Float64 {
			sb.WriteString(fmt.Sprintf("%.2f", d[k]))
		} else {
			sb.WriteString(fmt.Sprintf("%v", d[k]))
		}
		sb.WriteRune('&')
	}

	return sb.String()
}

func (d MessageData) ID() int64 {
	return int64(d["id"].(float64))
}

type Message struct {
	Database string      `json:"database"`
	Table    string      `json:"table"`
	Type     string      `json:"type"`
	Data     MessageData `json:"data"`
}

func (m Message) K() string {
	k := fmt.Sprintf("%s_%s_%s_%d_%s", m.Database, m.Table, m.Type, m.Data.ID(), m.Data)
	return k
}

func (m Message) String() string {
	return "database: " + m.Database + ", table: " + m.Table + ", type: " + m.Type + ", data: " + m.Data.String()
}

func main() {
	flag.Parse()

	if *dtsBrokers == "" {
		panic("dts.bootstrap.servers must not be empty")
	}
	if *maxwellBrokers == "" {
		panic("maxwell.bootstrap.servers must not be empty")
	}
	if *dtsBrokers == *maxwellBrokers {
		panic("dts.bootstrap.servers must not be equal to maxwell.bootstrap.servers")
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

	c := kafkaTool.NewReplayConsumer(*dtsBrokers, *topic, start.Add(-5*time.Second), end.Add(5*time.Second))
	defer c.Close()

	data := make(map[string]map[string]int)
	var dtsCount int
	for m := range c.Read() {
		var msg Message
		json.Unmarshal(m.Value, &msg)
		k := msg.K()
		tp := *m.TopicPartition.Topic
		_, ok := data[tp]
		if !ok {
			data[tp] = make(map[string]int)
		}
		if data[tp][k] == 0 {
			dtsCount++
		}
		data[tp][k]++
	}

	c1 := kafkaTool.NewReplayConsumer(*maxwellBrokers, *topic, start, end)
	defer c1.Close()

	var diffCount, maxwellCount int
	for m := range c1.Read() {
		var msg Message
		json.Unmarshal(m.Value, &msg)
		k := msg.K()
		tp := *m.TopicPartition.Topic
		if data[tp] == nil || data[tp][k] == 0 {
			log.Printf("message: [%s] not exists in dst, topic: %s, partition: %d", msg, tp, m.TopicPartition.Partition)
			diffCount++
		}
		maxwellCount++
	}

	log.Printf("maxwell count: %d, dts count: %d, diff count: %d", maxwellCount, dtsCount, diffCount)
}
