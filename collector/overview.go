package collector

import (
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"io/ioutil"
	"net/http"
	"strings"
)

type Overview struct {
	TaskManagers   int
	SlotsTotal     int
	SlotsAvailable int
	JobsRunning    int
	JobsFinished   int
	JobsCancelled  int
	JobsFailed     int
	FlinkVersion   string
}

func (o *Overview) GetOverview(flinkJobManagerUrl string) Overview {
	url := strings.Trim(flinkJobManagerUrl, "/") + "/overview"
	log.Debug(url)

	overview := Overview{
		TaskManagers:   -1,
		SlotsTotal:     -1,
		SlotsAvailable: -1,
		JobsRunning:    -1,
		JobsFinished:   -1,
		JobsCancelled:  -1,
		JobsFailed:     -1,
	}

	response, err := http.Get(url)
	if err != nil {
		log.Errorf("http.Get = %v", err)
		return overview
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("ioutil.ReadAll = %v", err)
		return overview
	}
	if response.StatusCode != 200 {
		log.Errorf("response.StatusCode = %v", response.StatusCode)
		return overview
	}

	jsonStr := string(body)
	log.Debug(jsonStr)

	// parse
	js, err := simpleJson.NewJson([]byte(jsonStr))
	if err != nil {
		log.Errorf("simpleJson.NewJson = %v", err)
		return overview
	}

	// taskmanagers
	overview.TaskManagers, err = js.Get("taskmanagers").Int()
	if err != nil {
		log.Errorf("js.Get 'taskmanagers' = %v", err)
		return overview
	}
	log.Debugf("overview.TaskManagers = %v", overview.TaskManagers)

	// slots-total
	overview.SlotsTotal, err = js.Get("slots-total").Int()
	if err != nil {
		log.Errorf("js.Get 'slots-total' = %v", err)
		return overview
	}
	log.Debugf("overview.SlotsTotal = %v", overview.SlotsTotal)

	// slots-available
	overview.SlotsAvailable, err = js.Get("slots-available").Int()
	if err != nil {
		log.Errorf("js.Get 'slots-available' = %v", err)
		return overview
	}
	log.Debugf("overview.SlotsAvailable = %v", overview.SlotsAvailable)

	// jobs-running
	overview.JobsRunning, err = js.Get("jobs-running").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-running' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsRunning = %v", overview.JobsRunning)

	// jobs-finished
	overview.JobsFinished, err = js.Get("jobs-finished").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-finished' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsFinished = %v", overview.JobsFinished)

	// jobs-cancelled
	overview.JobsCancelled, err = js.Get("jobs-cancelled").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-cancelled' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsCancelled = %v", overview.JobsCancelled)

	// jobs-failed
	overview.JobsFailed, err = js.Get("jobs-failed").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-failed' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsFailed = %v", overview.JobsFailed)

	return overview
}
