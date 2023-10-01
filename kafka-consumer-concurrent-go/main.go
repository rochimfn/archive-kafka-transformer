package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const sourceTopic = "random_topics"
const targetTopic = "random_topics_goroutine_transformed"

type Message struct {
	FirstName string `json:"firstname"`
	LastName  string `json:"lastname"`
	Hash      string `json:"hash"`
}

func panicOnError(err error) {
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

func Consumer(quit chan struct{}, consumerChan chan *kafka.Message) {
	log.Println("consumer started")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "consumer_goroutine"})
	panicOnError(err)
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{sourceTopic}, nil)
	panicOnError(err)

	for {
		select {
		case <-quit:
			close(consumerChan)
			return
		default:
			ev := consumer.Poll(1)
			switch e := ev.(type) {
			case *kafka.Message:
				log.Println("got message")
				consumerChan <- e
			case kafka.Error:
				panicOnError(e)
			}

		}
	}

}

func Transformer(consumerChan chan *kafka.Message, producerChan chan *kafka.Message) {
	log.Println("transformer started")
	defer close(producerChan)
	topic := targetTopic

	for m := range consumerChan {
		var message Message
		err := json.Unmarshal(m.Value, &message)
		panicOnError(err)

		message.Hash = fmt.Sprintf("%x", (md5.Sum(m.Value)))

		transformedMessage, err := json.Marshal(message)
		panicOnError(err)

		producerChan <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            m.Key,
			Value:          transformedMessage,
		}
	}

}

func producerAcked(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				panicOnError(ev.TopicPartition.Error)
			} else {
				log.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		}
	}
}

func Producer(producerChan chan *kafka.Message) {
	log.Println("producer started")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all"})
	panicOnError(err)

	go producerAcked(p)
	for m := range producerChan {
		log.Println("producing")
		err := p.Produce(m, nil)
		panicOnError(err)
	}

	unflushed := p.Flush(1000)
	log.Println("unflushed:", unflushed)
	p.Close()
}

func main() {
	var wg sync.WaitGroup
	quitChan := make(chan struct{})

	consumerChan := make(chan *kafka.Message, 100000)
	producerChan := make(chan *kafka.Message, 100000)

	wg.Add(3)
	go func() {
		Consumer(quitChan, consumerChan)
		wg.Done()
	}()

	go func() {
		Transformer(consumerChan, producerChan)
		wg.Done()
	}()

	go func() {
		Producer(producerChan)
		wg.Done()
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	quitChan <- struct{}{}

	wg.Wait()
	log.Println("done")
}
