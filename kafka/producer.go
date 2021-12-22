package kafka

import (
	"github.com/Shopify/sarama"
	"io"
)

type Producer interface {
	io.Closer
	Send(topic string, headers map[string]string, key, data []byte) (partition int32, offset int64, err error)
}

type producer struct {
	producer sarama.SyncProducer
}

func (p *producer) Send(topic string, headers map[string]string, key, data []byte) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return p.producer.SendMessage(msg)
}

func (p *producer) Close() error {
	return p.producer.Close()
}

func OpenProducer(addrs []string) (Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	p, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return &producer{
		producer: p,
	}, nil
}
