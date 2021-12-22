# kafka-tools


```
cd ./kafka/cmd/consumer
go build -o consumer

# connect and subscribe
consumer -addrs 127.0.0.1:9092 -topics mytopic
```

```
cd ./kafka/cmd/producer
go build -o producer

# connect and set the default topic to publish
producer -addrs 127.0.0.1:9092 -topic defaultTopic
# then set the actual topic, key, headers and value to publish. topic is optional if default topic is set
-topic mytopic -key mykey -value myvalue -headers hk1:hv1,hk2:hv2
```

