package main

import (
	"database/sql"
)

type Review struct {
	Body   string
	Url    string
	Artist string
	Genre  string
	Album  string
	Score  float64
}

func fetchReviews(conn *sql.DB, pageNumber int, pageLength int) ([]Review, error) {
	query := `SELECT  r.body
									, r.review_url
									, a.name
									, gr.genre
									, t.title
									, t.score
						FROM reviews AS r
						
						JOIN genre_review_map AS gr
							ON r.review_url = gr.review_url
						JOIN artist_review_map AS ar
							ON r.review_url = ar.review_url
						JOIN artists AS a
							ON ar.artist_id = a.artist_id
						JOIN tombstones AS t
							ON r.review_url = t.review_url
						
						LIMIT  ?
						OFFSET ?`

	rows, err := conn.Query(query, pageLength, pageNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reviews []Review

	for rows.Next() {
		var rev Review
		if err := rows.Scan(&rev.Body, &rev.Url, &rev.Artist, &rev.Genre,
			&rev.Album, &rev.Score); err != nil {
			return nil, err
		}

		reviews = append(reviews, rev)
	}

	if err = rows.Err(); err != nil {
		return reviews, err
	}
	return reviews, nil
}
