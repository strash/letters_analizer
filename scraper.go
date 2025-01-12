package main

import (
	"net/http"
	"regexp"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

const (
	tag_query string = "(<.+?>)"
	br_query  string = "(<br.+?>)"
)

var (
	tag_regexp *regexp.Regexp = regexp.MustCompile(tag_query)
	br_regexp  *regexp.Regexp = regexp.MustCompile(br_query)
)

func visit(uri string, is_article bool) ([]string, error) {
	res, err := http.Get(uri)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, nil
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, err
	}

	content := make([]string, 0)
	ch := make(chan string)
	if is_article {
		// parse title
		title := doc.Find(".tm-title.tm-title_h1")
		if title != nil {
			if html, err := title.Html(); err == nil {
				go cleanContent(html, ch)
				res := <-ch
				content = append(content, res)
			}
		}
		// parse article
		article := doc.Find(".article-formatted-body > div")
		if article != nil && len(article.Nodes) != 0 {
			if article.Parent().HasClass("article-formatted-body_version-1") {
				if html, err := article.Html(); err == nil {
					go cleanContent(html, ch)
					res := <-ch
					content = append(content, res)
				}
			} else if article.Parent().HasClass("article-formatted-body_version-2") {
				article.Children().Each(func(i int, s *goquery.Selection) {
					findContent(s, &content, ch)
				})
			}
		}
	} else {
		// parse comments
		comments := doc.Find(".tm-comment__body-content > div")
		if comments != nil && len(comments.Nodes) != 0 {
			if comments.Parent().HasClass("tm-comment__body-content_v2") {
				comments.Children().Each(func(i int, s *goquery.Selection) {
					findContent(s, &content, ch)
				})
			} else {
				comments.Each(func(i int, s *goquery.Selection) {
					if html, err := s.Html(); err == nil {
						go cleanContent(html, ch)
						res := <-ch
						content = append(content, res)
					}
				})
			}
		}
	}
	return content, nil
}

func findContent(s *goquery.Selection, content *[]string, ch chan string) {
	if s == nil {
		return
	}
	s.Each(func(i int, s *goquery.Selection) {
		if s != nil {
			node_name := goquery.NodeName(s)
			switch node_name {
			case "h1", "h2", "h3", "h4", "h5", "h6", "p":
				html, err := s.Html()
				if err == nil {
					go cleanContent(html, ch)
					res := <-ch
					if len(res) == 0 {
						return
					}
					(*content) = append(*content, html)
				}
			default:
				children := s.Children()
				if children.Length() != 0 {
					findContent(children, content, ch)
				}
			}
		}
	})
}

func cleanContent(s string, ch chan string) {
	res := s
	res = br_regexp.ReplaceAllLiteralString(res, "\n")
	res = tag_regexp.ReplaceAllLiteralString(res, "")
	res = html.UnescapeString(res)
	ch <- res
}
