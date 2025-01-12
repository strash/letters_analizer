package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

const (
	dsn            string = "./db.sqlite"
	links_table    string = "links"
	words_table    string = "words"
	letters_table  string = "letters_by_position"
	bigrams_table  string = "bigrams_by_position"
	trigrams_table string = "trigrams_by_position"
)

func prepareDB() (*sql.DB, error) {
	if _, err := os.Stat(dsn); err != nil {
		file, err := os.Create(dsn)
		if err != nil {
			return nil, err
		}
		file.Close()
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(fmt.Sprintf(`
	PRAGMA journal_mode=WAL;
	PRAGMA wal_checkpoint(TRUNCATE);
	PRAGMA wal_autocheckpoint=50;
	VACUUM;

	BEGIN TRANSACTION;

	-- links
	CREATE TABLE IF NOT EXISTS %[1]s (link TEXT);

	-- words
	CREATE TABLE IF NOT EXISTS %[2]s (
		value TEXT UNIQUE NOT NULL,
		count INTEGER NOT NULL DEFAULT 1
	);

	-- words length view
	CREATE VIEW IF NOT EXISTS %[2]s_view (length, count) AS
	SELECT
		LENGTH(value) AS length,
		SUM(count) AS count
	FROM %[2]s
	GROUP BY LENGTH(value)
	ORDER BY SUM(count) DESC;

	-- letters by position
	CREATE TABLE IF NOT EXISTS %[3]s (
		value    TEXT NOT NULL,
		position INTEGER NOT NULL,
		count    INTEGER NOT NULL DEFAULT 1
	);

	-- letters by position unique index
	CREATE UNIQUE INDEX IF NOT EXISTS %[3]s_idx ON %[3]s (value, position);

	-- letters view
	CREATE VIEW IF NOT EXISTS %[3]s_view (value, count) AS
	SELECT
		value,
		SUM(count) AS count
	FROM %[3]s
	GROUP BY value
	ORDER BY SUM(count) DESC;

	-- bigrams
	CREATE TABLE IF NOT EXISTS %[4]s (
		value    TEXT NOT NULL,
		position INTEGER NOT NULL,
		count    INTEGER NOT NULL DEFAULT 1
	);

	-- bigrams unique index
	CREATE UNIQUE INDEX IF NOT EXISTS %[4]s_idx ON %[4]s (value, position);

	-- bigrams view
	CREATE VIEW IF NOT EXISTS %[4]s_view (value, count) AS
	SELECT
		value,
		SUM(count) AS count
	FROM %[4]s
	GROUP BY value
	ORDER BY SUM(count) DESC;

	-- trigrams
	CREATE TABLE IF NOT EXISTS %[5]s (
		value    TEXT NOT NULL,
		position INTEGER NOT NULL,
		count    INTEGER NOT NULL DEFAULT 1
	);

	-- trigrams unique index
	CREATE UNIQUE INDEX IF NOT EXISTS %[5]s_idx ON %[5]s (value, position);

	-- trigrams view
	CREATE VIEW IF NOT EXISTS %[5]s_view (value, count) AS
	SELECT
		value,
		SUM(count) AS count
	FROM %[5]s
	GROUP BY value
	ORDER BY SUM(count) DESC;

	COMMIT;
	`,
		links_table,    // 1
		words_table,    // 2
		letters_table,  // 3
		bigrams_table,  // 4
		trigrams_table, // 5
	)); err != nil {
		return db, err
	}

	return db, nil
}

func getParsedLinks(db *sql.DB) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s;", links_table))
	if err != nil {
		return nil, err
	}
	links := make([]string, 0)
	for rows.Next() {
		var link string
		if err := rows.Scan(&link); err != nil {
			return nil, err
		}
		links = append(links, link)
	}
	return links, nil
}

func insertLink(db *sql.DB, link string) error {
	_, err := db.Exec(
		fmt.Sprintf("INSERT INTO %s (link) VALUES (?);", links_table),
		link,
	)
	return err
}

func insertWords(db *sql.DB, items []string) error {
	for i := 0; i < len(items); i += insert_batch_size {
		end := i + insert_batch_size
		if end > len(items) {
			end = len(items)
		}
		placeholder := "(?)"
		placeholders := make([]string, 0)
		values := make([]interface{}, 0)
		for _, item := range items[i:end] {
			placeholders = append(placeholders, placeholder)
			values = append(values, item)
		}
		_, err := db.Exec(fmt.Sprintf(`
		INSERT INTO %s (value) VALUES %s
		ON CONFLICT(value) DO UPDATE SET count = count + 1;
		`, words_table, strings.Join(placeholders, ",")),
			values...,
		)
		return err
	}
	return nil
}

func insertWithPosition(db *sql.DB, table string, items []entry) error {
	for i := 0; i < len(items); i += insert_batch_size {
		end := i + insert_batch_size
		if end > len(items) {
			end = len(items)
		}
		placeholder := "(?, ?)"
		placeholders := make([]string, 0)
		values := make([]interface{}, 0)
		for _, item := range items[i:end] {
			placeholders = append(placeholders, placeholder)
			values = append(values, item.Value, item.Position)
		}
		_, err := db.Exec(fmt.Sprintf(`
		INSERT INTO %s (value, position) VALUES %s
		ON CONFLICT(value, position) DO UPDATE SET count = count + 1;
		`, table, strings.Join(placeholders, ",")),
			values...,
		)
		return err
	}
	return nil
}

func cleanUp(db *sql.DB) error {
	if _, err := db.Exec(`
		PRAGMA wal_checkpoint(TRUNCATE);
		PRAGMA shrink_memory;
		PRAGMA optimize;
		VACUUM;
		`); err != nil {
		return err
	}
	return nil
}
