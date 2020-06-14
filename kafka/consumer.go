package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"io"
)

type Consumer interface {
	io.Closer
}

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	HandleError(error)
}

type groupConsumer struct {
	client  sarama.Client
	group   sarama.ConsumerGroup
	handler ConsumerGroupHandler
}

func (c *groupConsumer) Close() error {
	if c.group != nil {
		_ = c.group.Close()
	}

	if c.client != nil {
		_ = c.client.Close()
	}

	return nil
}

func OpenGroupConsumer(addrs, topics []string, consumerOffset int64, groupId string, handler ConsumerGroupHandler) (Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Consumer.Offsets.Initial = consumerOffset //sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	// Start with a client
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(groupId, client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	// Track errors
	go func() {
		for err := range group.Errors() {
			if handler != nil {
				handler.HandleError(err)
			}
		}
	}()

	// Iterate over consumer sessions.
	go func() {
		ctx := context.Background()
		for {
			err := group.Consume(ctx, topics, handler)
			if err == sarama.ErrClosedConsumerGroup {
				break
			} else if err != nil && handler != nil {
				handler.HandleError(err)
			}
		}
	}()

	return &groupConsumer{
		handler: handler,
		client:  client,
		group:   group,
	}, nil
}
