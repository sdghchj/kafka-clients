package kafka

import (
	"github.com/Shopify/sarama"
	"io"
)

type Producer interface {
	io.Closer
	Send(topic string, key, data []byte) (partition int32, offset int64, err error)
}

type producer struct {
	producer sarama.SyncProducer
}

func (p *producer) Send(topic string, key, data []byte) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}
	return p.producer.SendMessage(msg)
}

func (p *producer) Close() error {
	return p.producer.Close()
}

func OpenProducer(addrs []string) (Producer, error) {
	config := sarama.NewConfig()
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
