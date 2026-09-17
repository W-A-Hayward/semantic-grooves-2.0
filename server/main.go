package main

// orchestrator
// connects to db
// spawns fetcher inside goroutine
// spawns parser inside goroutine
// spawns sender inside goroutine
// closes connection to db
// handles sync before shutting down

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "../data/data.sqlite3")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	reviews, err := fetch_reviews(db, 1, 2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reviews[0])
}
