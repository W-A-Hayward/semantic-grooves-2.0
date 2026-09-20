package main

// orchestrator
// connects to db
// spawns fetcher inside goroutine
// spawns parser inside goroutine
// spawns sender inside goroutine
// closes connection to db
// handles sync before shutting down

import (
	"context"
	"database/sql"
	"log"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "../data/data.sqlite3")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	writer := new_kafka_writer()
	defer writer.Close()

	reviews, err := fetchReviews(db, 1, 30)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for _, value := range reviews {
		chunks := parse_review(value)
		for _, chunk := range chunks {
			if chunk.Body == "" {
				continue
			}
			if err := sendReview(ctx, writer, chunk.Url, chunk.Body); err != nil {
				log.Fatal(err)
			}
		}
	}
}
