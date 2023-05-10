package cmd

import (
	"context"
	"fmt"
	"github.com/MochamadAkbar/pubsub-kafka/configs"
	"github.com/MochamadAkbar/pubsub-kafka/pkg/amqp"
	"github.com/MochamadAkbar/pubsub-kafka/pkg/logger"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"log"
	"net/http"
	"os"
	"sync"
)

var (
	srvPort string
	rootCmd = &cobra.Command{
		Use:   "service",
		Short: "Running Pubsub Service",
		Long:  "Used to run Pubsub service rabbitMQ",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			logs := logger.NewLogger()

			_, err := configs.LoadConfig(".")
			if err != nil {
				logs.Fatalf("Failed to load env: %s", err.Error())
			}

			logs.Info("Env Successfully Loaded")

			producer := sarama.NewConfig()
			producer.Producer.Return.Successes = true
			producer.Producer.RequiredAcks = sarama.WaitForAll
			producer.Producer.Retry.Max = 5
			producer.Producer.Return.Errors = true

			consumer := sarama.NewConfig()
			consumer.Consumer.Return.Errors = true

			logs.Info("here")
			kafka, err := amqp.NewAMQPService([]string{"kafka:9092"}, producer, consumer, []string{"my-topic"}, "range")
			if err != nil {
				log.Fatalf("Failed to create Native AMQP Service: %s", err.Error())
			}
			defer kafka.Close()
			logs.Info("test")

			wg := &sync.WaitGroup{}
			wg.Add(1)

			go func() {
				defer wg.Done()
				for {
					if err := kafka.Consume(ctx); err != nil {
						logs.Panicf("Error from consumer: %v", err)
					}
					if ctx.Err() != nil {
						return
					}
					kafka.Consumer.Ready = make(chan bool)
				}
			}()
			<-kafka.Consumer.Ready
			logs.Println("Sarama consumer up and running!...")

			for i := 0; i < 10; i++ {
				publish(kafka)
			}
			kafka.AwaitTerminate(ctx, cancel)

			wg.Wait()

			s := &http.Server{
				Addr:    "127.0.0.1:4200",
				Handler: nil,
			}
			log.Fatal(s.ListenAndServe())
		},
	}
)

func Execute() {
	rootCmd.Flags().StringVarP(&srvPort, "srvport", "s", "", "define service pubsub")
	rootCmd.MarkFlagsRequiredTogether("srvport")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}