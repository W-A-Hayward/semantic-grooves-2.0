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
	"strings"

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
	// One message per album: the tagger needs the whole review to judge its
	// mood, so reviews are no longer split into paragraphs here.
	for _, review := range reviews {
		if strings.TrimSpace(review.Body) == "" {
			continue
		}
		if err := sendReview(ctx, writer, review); err != nil {
			log.Fatal(err)
		}
	}
}
