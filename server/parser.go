package main

import (
	"strings"
)

func parse_review(review Review) []Review {
	paragraphs := strings.Split(review.Body, "\n")

	var chunked_reviews []Review
	for _, value := range paragraphs {
		chunk := Review{
			Body:   value,
			Artist: review.Artist,
			Genre:  review.Genre,
			Album:  review.Album,
			Score:  review.Score,
		}

		chunked_reviews = append(chunked_reviews, chunk)
	}

	return chunked_reviews
}
