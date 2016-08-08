package util

import (
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

type HttpClient struct{}

func (httpClient *HttpClient) Get(url string) (string, error) {
	log.Debug(url)

	response, err := http.Get(url)
	if err != nil {
		log.Errorf("http.Get = %v", err)
		return "", err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("ioutil.ReadAll = %v", err)
		return "", err
	}
	if response.StatusCode != 200 {
		log.Errorf("response.StatusCode = %v", response.StatusCode)
		return "", err
	}

	json := string(body)
	log.Debug(json)

	return json, nil
}
