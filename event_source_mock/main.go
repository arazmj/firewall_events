package main

import (
	"firewall_events/protobuf"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/tkanos/gonfig"
)

type Configuration struct {
	BrokerList		[]string;
	Topic			string;
	MaxRetry		int;
}

func main() {
	event := ExchangeMessage.Event{
		DstIpAddr: 192 << 24 + 168 << 16 + 0 <<  8 + 1,
		SrcIpAddr: 127 << 24 +   0 << 16 + 0 <<  8 + 1,
		DstPort: 80,
		SrcPort: 112,
	}
	fmt.Println(event);
	out, _ := proto.Marshal(&event);
	fmt.Println(out);

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
	msg := &sarama.ProducerMessage{
		Topic: configuration.Topic,
		Value: sarama.StringEncoder(out),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", configuration.Topic, partition, offset)
}