package main

import (
	"flag"
	"fmt"
	"strings"
	"unicode"

	"github.com/sdghchj/kafka-tools/kafka"
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
		fmt.Println("input format: topic key value")
	} else {
		fmt.Println("input format: key value")
	}
	var input string
	for {
		_, err = fmt.Scanln(&input)
		if err != nil {
			fmt.Println(err)
			break
		} else if strings.ToLower(input) == "exit" {
			break
		}
		n := 0
		fields := strings.FieldsFunc(input, func(r rune) bool {
			if unicode.IsSpace(r) {
				n++
				if n < fieldsCount {
					return true
				}
			}
			return false
		})
		if len(fields) != fieldsCount {
			continue
		} else if len(fields) == 2 {
			fmt.Println(producer.Send(topic, []byte(fields[0]), []byte(fields[1])))
		} else if len(fields) == 3 {
			fmt.Println(producer.Send(fields[0], []byte(fields[1]), []byte(fields[2])))
		}
	}
}
