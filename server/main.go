package main
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	_ "modernc.org/sqlite"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		stop()
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	db, err := openDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close()

	writer := new_kafka_writer()
	defer writer.Close()

	reviews, err := fetchReviews(db, 1, 30)
	if err != nil {
		return err
	}

	for _, review := range reviews {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(review.Body) == "" {
			continue
		}
		if err := sendReview(ctx, writer, review); err != nil {
			return err
		}
	}
	return nil
}

func openDB(ctx context.Context) (*sql.DB, error) {
	path := os.Getenv("DB_PATH")
	if path == "" {
		return nil, errors.New("DB_PATH is not set")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("DB_PATH: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return db, nil
}
