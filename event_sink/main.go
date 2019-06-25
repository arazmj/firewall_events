package main

import (
	"context"
	"encoding/binary"
	"firewall_events/protobuf"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/tkanos/gonfig"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)



type Configuration struct {
	Brokers              []string;
	Version              string;
	Group                string;
	Topics               string;
	Oldest               bool;
	Verbose              bool;
	SaslUser             string;
	SaslPassword         string;
	SaslEnable           bool;
	SaslHandshake        bool;
	CassandraConsistency string;
	CassandraKeyspace    string;
	CassandraUsername    string;
	CassandraPassword    string;
}

var (
	jsonConfig = Configuration{}
	dbSession  *gocql.Session;
)

func init() {
}

func main() {
	log.Println("Starting a Firewall event consumer")

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
	 * Setup Cassandra connection
	 */
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = jsonConfig.CassandraKeyspace;
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: jsonConfig.CassandraUsername,
		Password:jsonConfig.CassandraPassword}

	cluster.Consistency = gocql.Quorum
	if jsonConfig.CassandraConsistency == "LocalOne" {
		cluster.Consistency = gocql.LocalOne
	}
	dbSession, _ = cluster.CreateSession()
	defer dbSession.Close()


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
			log.Fatal("Unmarshaling error: ", err)
		}

		/*
		*  Insert the event on Cassandra
		*/
		srcIp := make([]byte, 4)
		binary.LittleEndian.PutUint32(srcIp, event.SrcIpAddr)

		dstIp := make([]byte, 4)
		binary.LittleEndian.PutUint32(dstIp, event.DstIpAddr)

		strSrcIp := fmt.Sprintf("%d.%d.%d.%d", srcIp[0], srcIp[1], srcIp[2], srcIp[3])
		strDstIp := fmt.Sprintf("%d.%d.%d.%d", dstIp[0], dstIp[1], dstIp[2], dstIp[3])

		errDb := dbSession.Query(
			`INSERT INTO fw_events (event_id, SrcIpAddr, DstIpAddr, SrcPort, DstPort, 
				   LastUpdated, DeviceId, Action, AclRuleId) 
                   VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, ?)`,
			strSrcIp, strDstIp, event.SrcPort, event.DstPort,
			time.Now(), event.DeviceId, event.Action, event.AclRuleId).Exec();

		if  errDb != nil {
			log.Fatal("Error inserting event record: ", errDb)
		}

		if jsonConfig.Verbose {
			log.Printf("Src IP: %s, Dst IP: %s, Src Port: %d, Dst Port: %d",
				strSrcIp,
				strDstIp,
				event.SrcPort, event.DstPort)
			log.Printf("Rule ID: %d, Last Update: %s, Device ID: %d, Action ID: %d",
				event.AclRuleId, ptypes.TimestampString(event.LastUpdated), event.DeviceId, event.Action)
		}

		session.MarkMessage(message, "")
	}

	return nil
}