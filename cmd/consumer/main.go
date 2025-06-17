package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/sdghchj/kafka-tools/kafka"
)

type handler struct {
	n int
}

func (*handler) Setup(s sarama.ConsumerGroupSession) error {
	fmt.Println("Setup", s.Claims())
	return nil
}
func (*handler) Cleanup(s sarama.ConsumerGroupSession) error {
	fmt.Println("Cleanup")
	return nil
}
func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.n++
		fmt.Println("---------------------------------------------")
		fmt.Printf("time: %s\n", time.Now().Format("2006-01-02 15:04:05.000"))
		fmt.Printf("topic: %s\npartition: %d\noffset: %d\n", msg.Topic, msg.Partition, msg.Offset)
		for _, header := range msg.Headers {
			fmt.Printf("header: %s = %s\n", header.Key, header.Value)
		}
		fmt.Printf("key: %s\n", string(msg.Key))
		fmt.Printf("value: %s\n", string(msg.Value))
		fmt.Println("---------------------------------------------")
		fmt.Println("")
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (*handler) HandleError(err error) {
	fmt.Println(err)
}

func main() {
	var addrs, topics, group string
	var offset int64 = -1
	flag.StringVar(&addrs, "addrs", "", "Addresses of kafka servers,format: 127.0.0.1:9002,127.0.0.2:9002")
	flag.StringVar(&topics, "topics", "", "Topics to subscribe,format: topic1,topic2")
	flag.Int64Var(&offset, "offset", -1, "Consuming offset")
	flag.StringVar(&group, "group", "group", "Group name of consumers")
	flag.Parse()
	var hd handler
	cs, err := kafka.OpenGroupConsumer(strings.Split(addrs, ","),
		strings.Split(topics, ","),
		offset,
		group,
		&hd)
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
		os.Exit(1)
		return
	}
	defer cs.Close()
	waitForShutdownSig()
}

func waitForShutdownSig() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-ch
}
