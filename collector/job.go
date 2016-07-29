package collector

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type Job struct{}

type jobMetrics struct {
	ReadBytes    int64
	WriteBytes   int64
	ReadRecords  int64
	WriteRecords int64
}

func (j *Job) GetWriteRecords(flinkJobManagerUrl string) int64 {
	jobs := j.getJobs(flinkJobManagerUrl)
	jobMetrics := j.getJobMetrics(flinkJobManagerUrl, jobs)

	var writeRecords int64
	for _, jobMetric := range jobMetrics {
		writeRecords += jobMetric.WriteRecords
	}

	log.Debugf("writeRecords = %v", writeRecords)

	return writeRecords
}

func (j *Job) getJobs(flinkJobManagerUrl string) []string {
	url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs"
	log.Debug(url)

	response, err := http.Get(url)
	if err != nil {
		log.Errorf("http.Get = %v", err)
		return []string{}
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("ioutil.ReadAll = %v", err)
		return []string{}
	}
	if response.StatusCode != 200 {
		log.Errorf("response.StatusCode = %v", response.StatusCode)
		return []string{}
	}

	jsonStr := string(body)
	log.Debug(jsonStr)

	// parse
	js, err := simpleJson.NewJson([]byte(jsonStr))
	if err != nil {
		log.Errorf("simpleJson.NewJson = %v", err)
		return []string{}
	}

	// jobs
	var jobs []string
	jobs, err = js.Get("jobs-running").StringArray()
	if err != nil {
		log.Errorf("js.Get 'jobs-running' = %v", err)
		return []string{}
	}
	log.Debugf("jobs = %v", jobs)

	return jobs
}

func (j *Job) getJobMetrics(flinkJobManagerUrl string, jobs []string) []jobMetrics {
	metrics := []jobMetrics{}
	for _, job := range jobs {
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job
		log.Debug(url)

		response, err := http.Get(url)
		if err != nil {
			log.Errorf("http.Get = %v", err)
			return []jobMetrics{}
		}
		defer response.Body.Close()

		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Errorf("ioutil.ReadAll = %v", err)
			return []jobMetrics{}
		}
		if response.StatusCode != 200 {
			log.Errorf("response.StatusCode = %v", response.StatusCode)
			return []jobMetrics{}
		}

		jsonStr := string(body)
		log.Debug(jsonStr)

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			return []jobMetrics{}
		}

		// vertices
		var vertices []interface{}
		vertices, err = js.Get("vertices").Array()
		if err != nil {
			log.Errorf("js.Get 'vertices' = %v", err)
			return []jobMetrics{}
		}
		log.Debugf("vertices = %v", vertices)

		for _, vertice := range vertices {
			vertice, _ := vertice.(map[string]interface{})
			log.Debugf("metrics = %v", vertice["metrics"])

			// only start with 'Source'
			if strings.HasPrefix(fmt.Sprint(vertice["name"]), "Source") {
				m, _ := vertice["metrics"].(map[string]interface{})
				verticesMetric := jobMetrics{}
				verticesMetric.ReadBytes, _ = strconv.ParseInt(fmt.Sprint(m["read-bytes"]), 10, 64)
				verticesMetric.WriteBytes, _ = strconv.ParseInt(fmt.Sprint(m["write-bytes"]), 10, 64)
				verticesMetric.ReadRecords, _ = strconv.ParseInt(fmt.Sprint(m["read-records"]), 10, 64)
				verticesMetric.WriteRecords, _ = strconv.ParseInt(fmt.Sprint(m["write-records"]), 10, 64)

				log.Debugf("verticesMetric = %v", verticesMetric)

				metrics = append(metrics, verticesMetric)
			}
		}
	}

	log.Debugf("metrics = %v", metrics)

	return metrics
}
