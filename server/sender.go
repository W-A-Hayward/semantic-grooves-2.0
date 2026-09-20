package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBroker = "localhost:9092"
	defaultTopic  = "reviews"
)

// ReviewMessage is the payload a later consumer (k8s job spawner) will read.
type ReviewMessage struct {
	ReviewURL string `json:"review_url"`
	Body      string `json:"body"`
}

func new_kafka_writer() *kafka.Writer {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = defaultBroker
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = defaultTopic
	}

	return &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
	}
}

func sendReview(ctx context.Context, writer *kafka.Writer, reviewURL, body string) error {
	payload, err := json.Marshal(ReviewMessage{
		ReviewURL: reviewURL,
		Body:      body,
	})
	if err != nil {
		return fmt.Errorf("marshal review %s: %w", reviewURL, err)
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(reviewURL),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("write review %s: %w", reviewURL, err)
	}
	return nil
}
