package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const sources_dir string = "./sources/habr/"

func getSourceLinks() ([]string, error) {
	dir, err := os.ReadDir(sources_dir)
	if err != nil {
		return nil, err
	}

	linksChan := make(chan []string)
	links := make([]string, 0)

	for _, dir_entry := range dir {
		if dir_entry.IsDir() {
			continue
		}
		name := dir_entry.Name()
		file, err := os.ReadFile(filepath.Join(sources_dir, name))
		if err != nil {
			return nil, err
		}
		go readFile(file, linksChan)
		links = append(links, <-linksChan...)
	}
	return links, nil
}

func readFile(file []uint8, linksCh chan []string) {
	all := make([]string, 0)
	for _, link := range strings.Split(string(file), "\n") {
		link = strings.Trim(link, "\r")
		link = strings.Trim(link, "\n")
		if len(link) == 0 {
			continue
		}
		all = append(all, link)
	}
	linksCh <- all
}

func removeDups(parsed_links []string, links []string) ([]string, error) {
	cleaned_links_ch := make(chan []string)
	cleaned_links := links

	dups_count_ch := make(chan int)
	dups_count := 0

	scraped_count := len(parsed_links)

	for _, target_link := range parsed_links {
		fmt.Printf("\r%s %d/%d duplicates", formatTime(nil), dups_count, scraped_count)
		go searchForDups(target_link, links, cleaned_links_ch, dups_count_ch)
		cleaned_links = <-cleaned_links_ch
		dups_count += <-dups_count_ch
	}
	return cleaned_links, nil
}

func searchForDups(target_link string, links []string, links_ch chan []string, dups_ch chan int) {
	all := links
	idx := slices.IndexFunc(links, func(link string) bool {
		l := strings.Trim(link, "\r")
		l = strings.Trim(l, "\n")
		return target_link == l
	})
	if idx != -1 {
		if idx+1 < len(links) {
			all = append(links[:idx], links[idx+1:]...)
		} else {
			all = links[:idx]
		}
		dups_ch <- 1
	} else {
		dups_ch <- 0
	}
	links_ch <- all
}
