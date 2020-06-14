package kafka

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
	"time"
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
		fmt.Printf("Message topic:%q partition:%d offset:%d\n value:%s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (*handler) HandleError(err error) {
	fmt.Println(err)
}

func TestKafkaGroupConsumer(t *testing.T) {
	var wg handler
	addrs := []string{"10.12.4.59:9092", "10.12.4.60:9092", "10.12.4.61:9092"}
	topic := "edgetest"
	consumer, err := OpenGroupConsumer(addrs, []string{topic}, -1, "group2", &wg)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("kafka consumer group started")

	defer consumer.Close()

	//producer, err := OpenProducer(addrs)
	//if err != nil {
	//	t.Error(err)
	//	return
	//}
	//defer producer.Close()
	//
	//for i := 0; i < 10; i++ {
	//	_, _, err = producer.Send(topic, []byte("hello"), []byte("hello world"))
	//	if err != nil {
	//		t.Error(err)
	//		return
	//	}
	//}
	time.Sleep(time.Second * 3000)
	if wg.n == 0 {
		t.Error(errors.New("no value consumed"))
	}
}
