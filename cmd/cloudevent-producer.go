package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
)

const (
	qtdMessages = 60000
)

var count int = 1
var cloudeventClient client.Client

var cloudeventProducer = &cobra.Command{
	Use: "cloudevent-producer",
	Run: cloudeventProducerHandler,
}

func cloudeventProducerHandler(cmd *cobra.Command, args []string) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_0_0_0
	saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	saramaConfig.Producer.Return.Successes = false

	sender, err := kafka_sarama.NewSender([]string{"127.0.0.1:9092"}, saramaConfig, "test-cloudevent")
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	defer sender.Close(context.Background())

	c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}
	cloudeventClient = c

	workers := 10
	jobs := make(chan Message, workers)

	for w := 1; w <= workers; w++ {
		go worker(w, jobs)
	}

	for i := 1; i < qtdMessages; i++ {
		jobs <- NewRandomMessage()
	}

	close(jobs)
}

func worker(id int, jobs <-chan Message) {
	for j := range jobs {
		fmt.Println("worker", id, "started job")

		e := cloudevents.NewEvent()
		e.SetID(uuid.New().String())
		e.SetType("com.cloudevents.sample.sent")
		e.SetSource("reward-payment/request-movement")
		e.SetSubject("reserver_movement")
		_ = e.SetData(cloudevents.ApplicationJSON, j)

		if result := cloudeventClient.Send(
			// Set the producer message key
			// kafka_sarama.WithMessageKey(context.Background(), sarama.StringEncoder(e.ID())),
			context.Background(),
			e,
		); cloudevents.IsUndelivered(result) {
			log.Printf("failed to send: %v", result)
		} else {
			log.Printf("sent: %d, accepted: %t", count, cloudevents.IsACK(result))
		}
		count++
	}
}

func init() {
	rootCmd.AddCommand(cloudeventProducer)
}
