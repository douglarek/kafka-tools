# kafka-tools [![Go](https://github.com/douglarek/kafka-tools/actions/workflows/go.yml/badge.svg)](https://github.com/douglarek/kafka-tools/actions/workflows/go.yml)

## Consume kafka topic(s)

```
$ go run cmd/consumer/main.go -topic '^basiclog-.*'

2021/04/09 18:11:26 AssignedPartitions: [basiclog-1[0]@unset basiclog-1[1]@unset basiclog-1[2]@unset]

2021/04/09 18:11:45 Message on basiclog-1[1]@7952731: {"@timestamp":"2021-04-09T10:11:45.316Z","@metadata":{"beat":"filebeat","type":"doc","version":"6.4.0","topic":"basiclog-1"},"offset":440,"message":"{\"logsev_ip\":\"127.0.0.1\",\"logsev_h\":\"test-0\",\"logsev_t\":1617963103674,\"a\":\"你好\"}","file_name":"1","prospector":{"type":"log"},"input":{"type":"log"},"beat":{"name":"test-0","hostname":"test-0","version":"6.4.0"},"source":"/home/logs/loglib/1_3.log","log_type":"basiclog-1","host":{"name":"test-0"}}
...
```
