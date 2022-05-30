package cmd

import (
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var massProducer = &cobra.Command{
	Use: "mass-producer",
	Run: massProducerHandler,
}

func massProducerHandler(cmd *cobra.Command, args []string) {

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal     // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy // Compress messages
	// config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	// producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	defer producer.Close()

	i := 1
	for i <= 60000 {
		message := NewRandomMessage()
		producer.SendMessage(&sarama.ProducerMessage{
			Topic: "poc-cloudevent",
			Value: sarama.StringEncoder(message.SerializeJSON()),
		})
		i++
	}
}

func init() {
	rootCmd.AddCommand(massProducer)
}
