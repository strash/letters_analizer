package main

import (
	"database/sql"
	"regexp"
	"strings"
)

const (
	insert_batch_size int = 2000

	ru_strings_pattern string = `([«"]?[а-яА-ЯёЁ]+(?:[-–—]?[а-яА-ЯёЁ]*[»".,:;!?]?)?)`
	ru_word_pattern    string = `([а-яА-ЯёЁ]+)`
)

var (
	strings_regexp = regexp.MustCompile(ru_strings_pattern)
	word_regexp    = regexp.MustCompile(ru_word_pattern)
)

type entry struct {
	Value    string
	Position int
}

func parse(db *sql.DB, content []string) error {
	word_ch := make(chan []string)
	words := make([]string, 0)

	letter_ch := make(chan []entry)
	letters := make([]entry, 0)

	bigram_ch := make(chan []entry)
	bigrams := make([]entry, 0)

	trigram_ch := make(chan []entry)
	trigrams := make([]entry, 0)

	for _, item := range content {
		matches := strings_regexp.FindAllString(item, -1)
		if matches == nil {
			continue
		}

		for _, match := range matches {
			match = strings.ToLower(match)
			match = normalize(match)
			if len(match) == 0 {
				continue
			}
			go findWords(match, word_ch)
			go findLetters(match, letter_ch)
			go findBigrams(match, bigram_ch)
			go findTrigrams(match, trigram_ch)
			words = append(words, <-word_ch...)
			letters = append(letters, <-letter_ch...)
			bigrams = append(bigrams, <-bigram_ch...)
			trigrams = append(trigrams, <-trigram_ch...)
		}

		if err := insertWords(db, words); err != nil {
			return err
		}
		if err := insertWithPosition(db, letters_table, letters); err != nil {
			return err
		}
		if err := insertWithPosition(db, bigrams_table, bigrams); err != nil {
			return err
		}
		if err := insertWithPosition(db, trigrams_table, trigrams); err != nil {
			return err
		}
	}
	return nil
}

func normalize(item string) string {
	s := strings.ReplaceAll(item, "«", `"`)
	s = strings.ReplaceAll(s, "»", `"`)
	s = strings.ReplaceAll(s, "–", "-")
	s = strings.ReplaceAll(s, "—", "-")
	return s
}

func findWords(item string, ch chan []string) {
	matches := word_regexp.FindAllString(item, -1)
	if matches == nil {
		ch <- []string{}
		return
	}
	ch <- matches
}

func findLetters(item string, ch chan []entry) {
	items := strings.Split(item, "")
	if len(items) == 0 {
		ch <- []entry{}
		return
	}
	entries := make([]entry, 0)
	for i, value := range items {
		entries = append(entries, entry{Value: value, Position: i})
	}
	ch <- entries
}

func findBigrams(item string, ch chan []entry) {
	if len(item) < 2 {
		ch <- []entry{}
		return
	}
	items := strings.Split(item, "")
	entries := make([]entry, 0)
	for i := 0; i < len(items)-1; i++ {
		value := strings.Join(items[i:i+2], "")
		entries = append(entries, entry{Value: value, Position: i})
	}
	ch <- entries
}

func findTrigrams(item string, ch chan []entry) {
	if len(item) < 3 {
		ch <- []entry{}
		return
	}
	items := strings.Split(item, "")
	entries := make([]entry, 0)
	for i := 0; i < len(items)-2; i++ {
		value := strings.Join(items[i:i+3], "")
		entries = append(entries, entry{Value: value, Position: i})
	}
	ch <- entries
}
