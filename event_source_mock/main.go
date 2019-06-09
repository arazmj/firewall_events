package main

import (
	"firewall_events/protobuf"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/tkanos/gonfig"
	"math/rand"
	"time"
)

type Configuration struct {
	BrokerList		[]string;
	Topic			string;
	MaxRetry		int;
}

func main() {

	configuration := Configuration{}
	err := gonfig.GetConf("config.json", &configuration)
	if err != nil {
		panic(err)
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = configuration.MaxRetry
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(configuration.BrokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	for true {
		event := ExchangeMessage.Event{
			DstIpAddr: rand.Uint32(),
			SrcIpAddr: rand.Uint32(),
			DstPort:   rand.Uint32() % 65535,
			SrcPort:   rand.Uint32() % 65535,
			DeviceId:  rand.Uint32(),
			Action: rand.Uint32() % 10,
			LastUpdated: ptypes.TimestampNow(),
			AclRuleId: rand.Uint32() % 4,
		}

		fmt.Println(event);
		out, _ := proto.Marshal(&event);
		fmt.Println(out);
		msg := &sarama.ProducerMessage{
			Topic: configuration.Topic,
			Value: sarama.StringEncoder(out),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", configuration.Topic, partition, offset)
		time.Sleep(1 * time.Second)
	}
}