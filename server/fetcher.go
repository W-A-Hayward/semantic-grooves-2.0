package main

import (
	"database/sql"
	"strings"
)

// Review is one album (a tombstone) together with its review. Artists and
// Genres are many-to-many in the database, so they are slices here.
type Review struct {
	AlbumID string
	Body    string
	Url     string
	Artists []string
	Genres  []string
	Album   string
	Score   float64
	PubDate string
}

// One row per tombstone. Artists and genres are folded into one column each
// with the ASCII unit separator, so joining the many-to-many tables cannot
// multiply rows (the old query returned 28,588 rows for 25,212 albums).
const reviewsQuery = `
SELECT  t.review_tombstone_id
			, r.body
			, r.review_url
			, t.title
			, t.score
			, r.pub_date
			, (SELECT group_concat(a.name, char(31))
					 FROM artist_review_map AS ar
					 JOIN artists AS a
						 ON a.artist_id = ar.artist_id
					WHERE ar.review_url = r.review_url) AS artists
			, (SELECT group_concat(gr.genre, char(31))
					 FROM genre_review_map AS gr
					WHERE gr.review_url = r.review_url) AS genres
FROM tombstones AS t
JOIN reviews AS r
	ON r.review_url = t.review_url
ORDER BY t.review_tombstone_id
LIMIT  ?
OFFSET ?`

// fetchReviews returns page pageNumber (1-based) of pageLength albums.
func fetchReviews(conn *sql.DB, pageNumber int, pageLength int) ([]Review, error) {
	if pageNumber < 1 {
		pageNumber = 1
	}
	offset := (pageNumber - 1) * pageLength

	rows, err := conn.Query(reviewsQuery, pageLength, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reviews []Review

	for rows.Next() {
		var rev Review
		var artists, genres sql.NullString
		if err := rows.Scan(&rev.AlbumID, &rev.Body, &rev.Url, &rev.Album,
			&rev.Score, &rev.PubDate, &artists, &genres); err != nil {
			return nil, err
		}
		rev.Artists = splitList(artists)
		rev.Genres = splitList(genres)

		reviews = append(reviews, rev)
	}

	if err = rows.Err(); err != nil {
		return reviews, err
	}
	return reviews, nil
}

func splitList(s sql.NullString) []string {
	if !s.Valid || s.String == "" {
		return nil
	}
	return strings.Split(s.String, "\x1f")
}
