package main

import (
	"fmt"
	"net/url"
	"time"
)

func main() {
	started := time.Now()
	fmt.Printf("%s Starting...\n", formatTime(&started))

	// prepare DB
	fmt.Printf("%s Prepairing DB...\n", formatTime(nil))
	db, err := prepareDB()
	defer db.Close()
	if err != nil {
		panic(err)
	}

	// prepare links
	links := make([]string, 0)
	{
		fmt.Printf("%s Prepairing links...\n", formatTime(nil))
		sourse_links, err := getSourceLinks()
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s %d links readed\n", formatTime(nil), len(sourse_links))

		// removing duplicates
		fmt.Printf("%s Searching for duplicates...\n", formatTime(nil))
		parsed_links, err := getParsedLinks(db)
		if err != nil {
			panic(err)
		}
		links, err = removeDups(parsed_links, sourse_links)
		if err != nil {
			panic(err)
		}
	}

	// scraping links
	total_count := len(links) * 2 // plus comments links
	scraped_count := 0
	content_ch := make(chan []string)
	fmt.Printf("\n%s Start scraping...\n", formatTime(nil))
	for _, link := range links {
		comments_link, err := url.JoinPath(link, "comments")
		if err != nil {
			panic(err)
		}

		go visit_safely(link, true, content_ch)
		go visit_safely(comments_link, false, content_ch)
		article := <-content_ch
		comments := <-content_ch

		if err = parse(db, article); err != nil {
			panic(err)
		}
		scraped_count++
		report(started, scraped_count, total_count)

		if err = parse(db, comments); err != nil {
			panic(err)
		}
		scraped_count++
		report(started, scraped_count, total_count)

		if err := insertLink(db, link); err != nil {
			panic(err)
		}
	}

	if err = cleanUp(db); err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)
	fmt.Printf("\n%s Done scraping!\n", formatTime(nil))
}

func formatTime(t *time.Time) string {
	tt := time.Now()
	if t != nil {
		tt = *t
	}
	return tt.Format(time.DateTime)
}

func getDuration(started time.Time) string {
	duration := time.Now().Sub(started)
	return fmt.Sprintf("%dd %02d:%02d:%02d",
		int(duration.Hours())/24,
		int(duration.Hours())%24,
		int(duration.Minutes())%60,
		int(duration.Seconds())%60,
	)
}

func report(started time.Time, scraped_count int, total_count int) {
	percent := float32(scraped_count) * 100.0 / float32(total_count)
	fmt.Printf("\r%s %.2f%% %d/%d scraped in %s",
		formatTime(nil),
		percent,
		scraped_count, total_count,
		getDuration(started),
	)
}
