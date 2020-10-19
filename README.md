# kafka-tools


```
cd ./kafka/cmd/consumer
go build -o consumer

consumer -addrs 127.0.0.1:9092 -topics mytopic
```

```
cd ./kafka/cmd/producer
go build -o producer

producer -addrs 127.0.0.1:9092 -topic mytopic
```

