package collector

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"github.com/matsumana/flink_exporter/util"
	"strconv"
	"strings"
)

var (
	jobNames map[string]string
)

type Job struct{}

type ReadWriteMertics struct {
	JobName      string
	ReadBytes    int64
	WriteBytes   int64
	ReadRecords  int64
	WriteRecords int64
}

type ReadWriteTotalMertics struct {
	ReadBytesTotal    int64
	WriteBytesTotal   int64
	ReadRecordsTotal  int64
	WriteRecordsTotal int64
	Details           []ReadWriteMertics
}

type CheckpointMetrics struct {
	JobName  string
	Count    int64
	Duration int
	Size     int64
}

// see https://github.com/apache/flink/blob/release-1.0.3/flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobStatus.java
// TODO Must modify, After Flink version up.
type JobStatusMetrics struct {
	JobName    string
	Created    int
	Running    int
	Failing    int
	Failed     int
	Cancelling int
	Canceled   int
	Finished   int
	Restarting int
}

func (j *Job) GetMetrics(flinkJobManagerUrl string) ([]JobStatusMetrics, ReadWriteTotalMertics, []CheckpointMetrics) {
	jobNames = make(map[string]string)
	jobs := j.getJobs(flinkJobManagerUrl)
	jobStatuses := j.getJobStatus(flinkJobManagerUrl, jobs)
	readWrites := j.getReadWrite(flinkJobManagerUrl, jobs)
	checkpoints := j.getCheckpoints(flinkJobManagerUrl, jobs)
	return jobStatuses, readWrites, checkpoints
}

func (j *Job) getJobs(flinkJobManagerUrl string) []string {
	url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs"
	httpClient := util.HttpClient{}
	jsonStr, err := httpClient.Get(url)
	if err != nil {
		log.Errorf("HttpClient.Get = %v", err)
		return []string{}
	}

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

func appendJobNameMap(jobId string, jobName string) {
	jobNames[jobId] = jobName
}

func (j *Job) getJobStatus(flinkJobManagerUrl string, jobs []string) []JobStatusMetrics {
	jobStatuses := []JobStatusMetrics{}
	httpClient := util.HttpClient{}
	for _, job := range jobs {
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job
		jsonStr, err := httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			return []JobStatusMetrics{}
		}

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			return []JobStatusMetrics{}
		}

		// job name
		var jobName string
		jobName, err = js.Get("name").String()
		if err != nil {
			log.Errorf("js.Get 'name' = %v", err)
			return []JobStatusMetrics{}
		}
		log.Debugf("jobName = %v", jobName)
		appendJobNameMap(job, jobName)

		// state
		var state string
		state, err = js.Get("state").String()
		if err != nil {
			log.Errorf("js.Get 'state' = %v", err)
			return []JobStatusMetrics{}
		}
		log.Debugf("state = %v", state)

		jobStatus := JobStatusMetrics{}
		jobStatus.JobName = jobNames[job]

		switch state {
		case "CREATED":
			jobStatus.Created += 1
		case "RUNNING":
			jobStatus.Running += 1
		case "FAILING":
			jobStatus.Failing += 1
		case "FAILED":
			jobStatus.Failed += 1
		case "CANCELLING":
			jobStatus.Cancelling += 1
		case "CANCELED":
			jobStatus.Canceled += 1
		case "FINISHED":
			jobStatus.Finished += 1
		case "RESTARTING":
			jobStatus.Restarting += 1
		}

		jobStatuses = append(jobStatuses, jobStatus)
	}

	log.Debugf("jobStatuses = %v", jobStatuses)

	return jobStatuses
}

func (j *Job) getReadWrite(flinkJobManagerUrl string, jobs []string) ReadWriteTotalMertics {
	total := ReadWriteTotalMertics{}
	readWrites := []ReadWriteMertics{}
	for _, job := range jobs {
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job
		httpClient := util.HttpClient{}
		jsonStr, err := httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			return ReadWriteTotalMertics{}
		}

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			return ReadWriteTotalMertics{}
		}

		// vertices
		var vertices []interface{}
		vertices, err = js.Get("vertices").Array()
		if err != nil {
			log.Errorf("js.Get 'vertices' = %v", err)
			return ReadWriteTotalMertics{}
		}
		log.Debugf("vertices = %v", vertices)

		readWrite := ReadWriteMertics{}
		readWrite.JobName = jobNames[job]

		for _, vertice := range vertices {
			v, _ := vertice.(map[string]interface{})
			log.Debugf("metrics = %v", v["metrics"])

			metrics, _ := v["metrics"].(map[string]interface{})
			record := ReadWriteMertics{}
			if strings.HasPrefix(fmt.Sprint(v["name"]), "Source") {
				record.WriteBytes, _ = strconv.ParseInt(fmt.Sprint(metrics["write-bytes"]), 10, 64)
				record.WriteRecords, _ = strconv.ParseInt(fmt.Sprint(metrics["write-records"]), 10, 64)
				readWrite.WriteBytes += record.WriteBytes
				readWrite.WriteRecords += record.WriteRecords
			} else {
				record.ReadBytes, _ = strconv.ParseInt(fmt.Sprint(metrics["read-bytes"]), 10, 64)
				record.ReadRecords, _ = strconv.ParseInt(fmt.Sprint(metrics["read-records"]), 10, 64)
				readWrite.ReadBytes += record.ReadBytes
				readWrite.ReadRecords += record.ReadRecords
			}
		}

		total.ReadBytesTotal += readWrite.ReadBytes
		total.ReadRecordsTotal += readWrite.ReadRecords
		total.WriteBytesTotal += readWrite.WriteBytes
		total.WriteRecordsTotal += readWrite.WriteRecords

		readWrites = append(readWrites, readWrite)
	}

	log.Debugf("readWrites = %v", readWrites)

	total.Details = readWrites

	return total
}

func (j *Job) getCheckpoints(flinkJobManagerUrl string, jobs []string) []CheckpointMetrics {
	checkpoints := []CheckpointMetrics{}
	httpClient := util.HttpClient{}
	for _, job := range jobs {
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job + "/checkpoints"
		jsonStr, err := httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			return []CheckpointMetrics{}
		}

		// not exists checkpoint info
		if jsonStr == "{}" {
			continue
		}

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			return []CheckpointMetrics{}
		}

		checkpoint := CheckpointMetrics{}
		checkpoint.JobName = jobNames[job]

		// count
		checkpoint.Count, err = js.Get("count").Int64()
		if err != nil {
			log.Errorf("js.Get 'count' = %v", err)
			return []CheckpointMetrics{}
		}
		log.Debugf("count = %v", checkpoint.Count)

		// history
		var histories []interface{}
		histories, err = js.Get("history").Array()
		if err != nil {
			log.Errorf("js.Get 'history' = %v", err)
			return []CheckpointMetrics{}
		}
		log.Debugf("history = %v", histories)

		if len(histories) > 0 {
			latest, _ := histories[len(histories)-1].(map[string]interface{})
			checkpoint.Duration, _ = strconv.Atoi(fmt.Sprint(latest["duration"]))
			checkpoint.Size, _ = strconv.ParseInt(fmt.Sprint(latest["size"]), 10, 64)
		}

		checkpoints = append(checkpoints, checkpoint)
	}

	log.Debugf("checkpoints = %v", checkpoints)

	return checkpoints
}
