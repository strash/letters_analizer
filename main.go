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
	fmt.Printf("%s Prepairing links...\n", formatTime(nil))
	links, err := getSourceLinks()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %d links readed\n", formatTime(nil), len(links))

	// removing duplicates
	fmt.Printf("%s Searching for duplicates...\n", formatTime(nil))
	parsed_links, err := getParsedLinks(db)
	if err != nil {
		panic(err)
	}
	links, err = removeDups(parsed_links, links)
	if err != nil {
		panic(err)
	}

	// scraping links
	total_count := len(links) * 2 // plus comments links
	scraped_count := 0
	fmt.Printf("\n%s Start scraping...\n", formatTime(nil))
	for _, link := range links {
		article, err := visit(link, true)
		if err != nil {
			panic(err)
		}
		if err = parse(db, article); err != nil {
			panic(err)
		}

		comments_link, err := url.JoinPath(link, "comments")
		if err != nil {
			panic(err)
		}
		comments, err := visit(comments_link, false)
		if err != nil {
			panic(err)
		}
		if err = parse(db, comments); err != nil {
			panic(err)
		}

		if err := insertLink(db, link); err != nil {
			panic(err)
		}
		scraped_count += 2
		report(started, scraped_count, total_count)
	}

	if err = cleanUp(db); err != nil {
		panic(err)
	}

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
	return fmt.Sprintf("%d days %d hours %d minutes and %d seconds",
		int(duration.Hours())/24,
		int(duration.Hours())%24,
		int(duration.Minutes())%60,
		int(duration.Seconds())%60,
	)
}

func report(started time.Time, scraped_count int, total_count int) {
	fmt.Printf("\r%s %d%% %d/%d scraped in %s",
		formatTime(nil),
		int(scraped_count*100/total_count),
		scraped_count, total_count,
		getDuration(started),
	)
}
