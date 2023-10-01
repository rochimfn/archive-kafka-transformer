package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/bgadrian/fastfaker/faker"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type mockEvent struct {
	FirstName string `json:"firstname"`
	LastName  string `json:"lastname"`
}

func panicOnError(err error) {
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

func GenMockEvent(c chan mockEvent, quit chan struct{}) {
	fake := faker.NewFastFaker()
	for {
		select {
		case <-quit:
			log.Println("get signal quit")
			close(c)
			return
		default:
			c <- mockEvent{
				FirstName: fake.FirstName(),
				LastName:  fake.LastName(),
			}
		}
	}
}

func ProduceEvent(c chan mockEvent, topic string, p *kafka.Producer, deliveryChannel chan kafka.Event) {
	fake := faker.NewFastFaker()
	for m := range c {
		b, err := json.Marshal(m)
		panicOnError(err)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(fake.UUID()),
			Value:          b},
			deliveryChannel,
		)
		panicOnError(err)

		p.Flush(1)

	}
}

func ack(producer *kafka.Producer) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
			} else {
				log.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		}
	}
}

func main() {
	topic := "random_topics"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
	})
	panicOnError(err)

	quit := make(chan struct{})
	events := make(chan mockEvent)

	go ack(p)
	go GenMockEvent(events, quit)
	go ProduceEvent(events, topic, p, nil)

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	log.Println("interrupted")
	quit <- struct{}{}

	p.Flush(10)
	p.Close()
}
