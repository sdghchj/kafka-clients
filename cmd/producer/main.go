package main

import (
	"flag"
	"fmt"
	"github.com/sdghchj/kafka-tools/kafka"
	"strings"
)

func main() {
	var addrs, topic string
	flag.StringVar(&addrs, "addrs", "", "Addresses of kafka servers,format: 127.0.0.1:9002,127.0.0.2:9002")
	flag.StringVar(&topic, "topic", "", "Topic to produce data into")
	flag.Parse()
	if len(addrs) == 0 {
		flag.Usage()
		return
	}
	producer, err := kafka.OpenProducer(strings.Split(addrs, ","))
	if err != nil {
		fmt.Printf("failed to connect kafka: %s", err)
		return
	}
	defer producer.Close()
	fieldsCount := 2
	if len(topic) == 0 {
		fieldsCount = 3
	}
	var key, value string
	for {
		if fieldsCount == 2 {
			fmt.Println("input format: key value")
			_, err = fmt.Scanln(&key, &value)
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(producer.Send(topic, []byte(key), []byte(value)))
		} else if fieldsCount == 3 {
			fmt.Println("input format: topic key value")
			_, err = fmt.Scanln(&topic, &key, &value)
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(producer.Send(topic, []byte(key), []byte(value)))
		}
	}
}
