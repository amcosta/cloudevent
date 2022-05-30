package cmd

import (
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var saramaAsyncProducer = &cobra.Command{
	Use: "sarama-async-producer",
	Run: saramaAsyncProducerHandler,
}

func saramaAsyncProducerHandler(cmd *cobra.Command, args []string) {

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	defer producer.Close()

	i := 1
	for i <= 10000000 {
		message := NewRandomMessage()
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "poc-dezmil",
			Value: sarama.StringEncoder(message.SerializeJSON()),
		}
		i++
	}
}

func init() {
	rootCmd.AddCommand(saramaAsyncProducer)
}
