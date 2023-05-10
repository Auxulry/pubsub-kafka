package cmd

import "github.com/MochamadAkbar/pubsub-kafka/pkg/amqp"

func publish(kafka *amqp.NativeAMQP) {
	kafka.Publish("my-topic", []byte("hello"))
}