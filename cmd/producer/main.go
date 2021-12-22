package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/sdghchj/kafka-tools/kafka"
	"os"
	"strings"
	"unicode"
)

func ExtractFieldsOfCmdLine(line string, predict func(r rune) bool) []string {
	doubleQuotation, quotation := 0, 0
	return strings.FieldsFunc(line, func(r rune) bool {
		if r == '"' {
			doubleQuotation ^= 1
		} else if r == '\'' {
			quotation ^= 1
		}
		return predict(r) && doubleQuotation == 0 && quotation == 0
	})
}

func main() {
	var addrs, defaultTopic string
	flag.StringVar(&addrs, "addrs", "", "Addresses of kafka servers,format: 127.0.0.1:9002,127.0.0.2:9002")
	flag.StringVar(&defaultTopic, "topic", "", "Topic to produce data into")
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
	var topic, key, headers, value string
	fs := flag.NewFlagSet("producer", flag.ExitOnError)
	fs.StringVar(&topic, "topic", "", "topic of the message")
	fs.StringVar(&key, "key", "", "key of the message")
	fs.StringVar(&headers, "headers", "", `headers of the message,format: "key1:value1,key2:value2..."`)
	fs.StringVar(&value, "value", "", "value of the message")
	fs.Usage()

	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		line := input.Text()
		if len(line) == 0 {
			continue
		}
		err := fs.Parse(ExtractFieldsOfCmdLine(line, unicode.IsSpace))
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		if topic == "" {
			topic = defaultTopic
			if topic == "" {
				fmt.Println("topic is empty")
				continue
			}
		}

		var hdrs map[string]string
		if len(headers) > 0 {
			hdrs = make(map[string]string)
			flds := strings.Split(headers, ",")
			for _, fld := range flds {
				kv := strings.SplitN(fld, ":", 2)
				if len(kv) >= 2 {
					hdrs[kv[0]] = kv[1]
				}
			}
		}

		fmt.Println(producer.Send(topic, hdrs, []byte(key), []byte(value)))
	}
}
