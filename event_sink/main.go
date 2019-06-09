package main

import (
	"context"
	"firewall_events/protobuf"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/tkanos/gonfig"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)



type Configuration struct {
	Brokers    []string;
	Version    string;
	Group      string;
	Topics     string;
	Oldest     bool;
	Verbose    bool;
	SaslUser   string;
	SaslPassword string;
	SaslEnable bool;
	SaslHandshake bool;
}

func init() {

}

func main() {
	log.Println("Starting a Firewall event consumer")

	jsonConfig := Configuration{}
	err := gonfig.GetConf("config.json", &jsonConfig)
	if err != nil {
		panic(err)
	}

	if jsonConfig.Verbose {

		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(jsonConfig.Version)
	if err != nil {
		panic(err)
	}

	/**
	 * Construct a new Sarama json_config.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = version

	if jsonConfig.Oldest {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	saramaConfig.Net.SASL.User = jsonConfig.SaslUser
	saramaConfig.Net.SASL.Password = jsonConfig.SaslPassword
	saramaConfig.Net.SASL.Enable = jsonConfig.SaslEnable
	saramaConfig.Net.SASL.Handshake = jsonConfig.SaslHandshake

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{}

	ctx := context.Background()
	client, err := sarama.NewConsumerGroup(jsonConfig.Brokers, jsonConfig.Group, saramaConfig)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			consumer.ready = make(chan bool, 0)
			err := client.Consume(ctx, strings.Split(jsonConfig.Topics, ","), &consumer)
			if err != nil {
				panic(err)
			}
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm // Await a sigterm signal before safely closing the consumer

	err = client.Close()
	if err != nil {
		panic(err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		event := ExchangeMessage.Event{}
		err := proto.Unmarshal(message.Value, &event)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
		}
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s",
			string(message.Value), message.Timestamp, message.Topic)
		log.Printf("Port %d %d", event.SrcPort, event.DstPort)
		session.MarkMessage(message, "")
	}

	return nil
}