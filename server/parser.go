package main

import (
	"strings"
)

// parse_review splits a review into one Review per paragraph. The pipeline no
// longer uses it (tagging needs the whole album, see main.go); it is kept for
// a possible passage-level retrieval feature later.
func parse_review(review Review) []Review {
	paragraphs := strings.Split(review.Body, "\n")

	var chunked_reviews []Review
	for _, value := range paragraphs {
		chunk := review
		chunk.Body = value

		chunked_reviews = append(chunked_reviews, chunk)
	}

	return chunked_reviews
}
