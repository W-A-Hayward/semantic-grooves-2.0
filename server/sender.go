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

// ReviewMessage is the payload a later consumer (k8s job spawner) will read:
// one message per album, with the whole review body.
type ReviewMessage struct {
	AlbumID   string   `json:"album_id"`
	ReviewURL string   `json:"review_url"`
	Artists   []string `json:"artists"`
	Title     string   `json:"title"`
	Genres    []string `json:"genres"`
	Score     float64  `json:"score"`
	PubDate   string   `json:"pub_date"`
	Body      string   `json:"body"`
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

func sendReview(ctx context.Context, writer *kafka.Writer, review Review) error {
	payload, err := json.Marshal(ReviewMessage{
		AlbumID:   review.AlbumID,
		ReviewURL: review.Url,
		Artists:   review.Artists,
		Title:     review.Album,
		Genres:    review.Genres,
		Score:     review.Score,
		PubDate:   review.PubDate,
		Body:      review.Body,
	})
	if err != nil {
		return fmt.Errorf("marshal album %s: %w", review.AlbumID, err)
	}

	// Keyed by album, so the same album always lands on the same partition.
	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(review.AlbumID),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("write album %s: %w", review.AlbumID, err)
	}
	return nil
}
