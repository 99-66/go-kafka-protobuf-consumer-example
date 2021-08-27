package main

import (
	"github.com/99-66/go-kafka-protobuf-consumer-example/kafka"
	pb "github.com/99-66/go-kafka-protobuf-consumer-example/protos/addressbook"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"os/signal"
)

func main() {
	// Initializing Kafka Consumer
	consumer, err := kafka.Init()
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Logging Consumer Error
	go func() {
		for err := range consumer.Errors() {
			log.Printf("ERROR :%s\n", err)
		}
	}()

	// Logging Consumer Notification
	go func() {
		for noti := range consumer.Notifications() {
			log.Printf("NOTI: %v\n", noti)
		}
	}()

	// consumer에서 데이터를 읽어온다
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				// protobuf 데이터로 언마샬링한다
				decodedPerson := pb.Person{}
				err := proto.Unmarshal(msg.Value, &decodedPerson)
				if err != nil {
					log.Printf("failed unmarshaling: %s\n", err)
				}
				// consumer에서 읽은 데이터는 마킹 처리한다
				consumer.MarkOffset(msg, "")
				log.Printf("Consumed Data: %+v\n", decodedPerson.String())
			}

		case <-signals:
			log.Printf("terminated signal")
			return
		}
	}

}
