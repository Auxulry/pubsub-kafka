package amqp

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type NativeAMQP struct {
	Producer  sarama.SyncProducer
	Consumer  *NativeAMQPConsumer
	Group     sarama.ConsumerGroup
	Topics    []string
	KeepAlive bool
	Pause     bool
}

func NewAMQPService(hosts []string,
	producerConfig *sarama.Config,
	consumerConfig *sarama.Config,
	topics []string,
	assignor string,
) (*NativeAMQP, error) {
	producer, err := sarama.NewSyncProducer(hosts, producerConfig)
	if err != nil {
		return nil, err
	}

	switch assignor {
	case "range":
		consumerConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	case "roundrobin":
		consumerConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case "sticky":
		consumerConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	group, err := sarama.NewConsumerGroup(hosts, "platform-group", consumerConfig)
	if err != nil {
		return nil, err
	}

	consumer := &NativeAMQPConsumer{
		Ready: make(chan bool),
	}

	return &NativeAMQP{Producer: producer, Group: group, Topics: topics, Consumer: consumer, KeepAlive: true, Pause: false}, nil
}

func (n *NativeAMQP) Publish(topic string, message []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := n.Producer.SendMessage(msg)
	if err != nil {
		log.Println("helooo")
		log.Println(err.Error())
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}

func (n *NativeAMQP) Consume(ctx context.Context) error {
	err := n.Group.Consume(ctx, n.Topics, n.Consumer)
	if err != nil {
		return err
	}
	return nil
}

func (n *NativeAMQP) AwaitTerminate(ctx context.Context, cancel context.CancelFunc) {
	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	log.Println("terminate")
	for n.KeepAlive {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			n.KeepAlive = false
		case <-sigterm:
			log.Println("terminating: via signal")
			n.KeepAlive = false
		case <-sigusr1:
			n.OnPause()
		}
	}
	cancel()
}

func (n *NativeAMQP) Close() {
	n.Producer.Close()
	n.Group.Close()
}

func (n *NativeAMQP) OnPause() {
	if n.Pause {
		n.Group.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		n.Group.PauseAll()
		log.Println("Pausing consumption")
	}

	n.Pause = !n.Pause
}

type NativeAMQPConsumer struct {
	Ready chan bool
}

func (c *NativeAMQPConsumer) Setup(session sarama.ConsumerGroupSession) error {
	close(c.Ready)
	return nil
}

func (c *NativeAMQPConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *NativeAMQPConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}