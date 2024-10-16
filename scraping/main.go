package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	url := "https://www.off.niihama-nct.ac.jp/gakuryo-a/kondate/"
	filepath := "html/ryoushoku.html"
	fileInfos, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Println("Downloading Domitory Meal HTML File...")
		err = DownloadFile(filepath, url+"ryoushoku.html")
	}
	if err != nil {
		log.Fatalln(err)
	}

	pdfUrl, err := getPDFUrl(&fileInfos, url)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(pdfUrl)

}

func DownloadFile(filepath string, url string) error {

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func makeFullPath(url string, path string) (string, bool) {
	if isUrl := strings.Contains(path, "://"); isUrl {
		return "", isUrl
	} else {
		return url + path, isUrl
	}
}

func getPDFUrl(readedFileByte *[]byte, url string) (*[]string, error) {
	stringReader := strings.NewReader(string(*readedFileByte))
	doc, err := goquery.NewDocumentFromReader(stringReader)
	if err != nil {
		return nil, err
	}
	var links []string
	doc.Find("tbody > tr").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Find("a").Attr("href")
		href = string([]rune(href))
		url, isUrl := makeFullPath(url, href)
		fmt.Println(isUrl)
		if len(href) > 0 && !isUrl {
			links = append(links, url)
		}
	})
	return &links, nil
}
