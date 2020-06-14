package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type TopicHandler func(key, value []byte) error

type ConsumerGroupTopicHandler interface {
	ConsumerGroupHandler

	//注册topic处理方法
	RegisterTopicHandler(topic string, handler TopicHandler)

	//已订阅的所有topic
	Topics() []string
}

type groupTopicHandler struct {
	handlers map[string]TopicHandler
}

func NewTopicHandler() ConsumerGroupTopicHandler {
	return &groupTopicHandler{handlers: make(map[string]TopicHandler)}
}

func (*groupTopicHandler) Setup(s sarama.ConsumerGroupSession) error {
	fmt.Printf("%s: kafka consumer setup:%+v", time.Now(), s.Claims())
	return nil
}
func (*groupTopicHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	fmt.Printf("%s: kafka consumer cleanup:%+v", time.Now(), s.Claims())
	return nil
}
func (h *groupTopicHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var err error
	for msg := range claim.Messages() {
		if h, ok := h.handlers[msg.Topic]; ok {
			err = h(msg.Key, msg.Value)
			if err == nil {
				sess.MarkMessage(msg, "")
			} else {
				fmt.Printf("kafka handle topic[%s] error: %s", msg.Topic, err.Error())
			}
		}
	}
	return nil
}

func (*groupTopicHandler) HandleError(err error) {
	fmt.Printf("kafka consumption error:%s", err.Error())
}

func (h *groupTopicHandler) RegisterTopicHandler(topic string, handler TopicHandler) {
	if _, ok := h.handlers[topic]; ok && handler == nil {
		delete(h.handlers, topic)
		return
	}
	h.handlers[topic] = handler
}

func (h *groupTopicHandler) Topics() []string {
	topics := make([]string, 0)
	for k := range h.handlers {
		topics = append(topics, k)
	}
	return topics
}
