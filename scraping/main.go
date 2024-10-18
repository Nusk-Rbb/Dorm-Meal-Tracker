package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	url := "https://www.off.niihama-nct.ac.jp/gakuryo-a/kondate/"
	nowMonth := getNowManth()
	filepath := "html/ryoushoku" + nowMonth + ".html"
	fileInfos, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Println("Downloading Domitory Meal HTML File...")
		err = DownloadFile(filepath, url+"ryoushoku.html")
	}
	if err != nil {
		log.Fatalln(err)
	}
	PDFFilePath, err := getPDFFilePath(&fileInfos)
	if err != nil {
		log.Fatalln(err)
	}

	first := true
	for _, PDFPath := range PDFFilePath {
		PDFUrl, isUrl := makeFullPath(url, PDFPath)
		if isUrl {
			continue
		}
		direcoryName, err := getDirecotry(PDFPath)
		if err != nil {

		}
		if first {
			first = false
			err = makeDirecoty("PDF/" + direcoryName)
			if err != nil {
				log.Fatalln(err)
			}
		}
		//TODO: PDFファイルがすでに存在する場合を除外する
		err = DownloadFile("PDF/"+PDFPath, PDFUrl)
		if err != nil {
			log.Fatalln(err)
		}
	}

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

func getPDFFilePath(readedFile *[]byte) ([]string, error) {
	if len(*readedFile) == 0 {
		return nil, fmt.Errorf("readedFile is empty")
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(*readedFile))
	if err != nil {
		return nil, err
	}
	var links []string
	doc.Find("tbody > tr").Each(func(_ int, row *goquery.Selection) {
		path, exists := row.Find("a").Attr("href")
		if !exists {
			return
		}
		if len(path) > 0 {
			links = append(links, path)
		}
	})
	return links, nil
}

func getNowManth() string {
	return time.Now().Month().String()
}

func getDirecotry(filePath string) (string, error) {
	parts := strings.Split(filePath, "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("cannot get directory")
	}
	return parts[0], nil
}

func makeDirecoty(direcoryName string) error {
	err := os.MkdirAll(direcoryName, 0755)
	if err != nil {
		return err
	}
	return nil
}
