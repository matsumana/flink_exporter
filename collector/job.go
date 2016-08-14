package collector

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"github.com/matsumana/flink_exporter/util"
	"strconv"
	"strings"
)

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

type ExceptionMetrics struct {
	JobName string
	Count   int
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

type jobDetail struct {
	id          string
	name        string
	detail      *simpleJson.Json
	checkPoints *simpleJson.Json
	exceptions  *simpleJson.Json
}

type Job struct{}

func (j *Job) GetMetrics(flinkJobManagerUrl string) ([]JobStatusMetrics, ReadWriteTotalMertics, []CheckpointMetrics, []ExceptionMetrics) {
	jobs := j.getJobs(flinkJobManagerUrl)
	jobDetails := j.getJobDetails(flinkJobManagerUrl, jobs)
	jobStatuses := j.getJobStatus(jobDetails)
	readWrites := j.getReadWrite(jobDetails)
	checkpoints := j.getCheckpoints(jobDetails)
	exceptions := j.getExceptions(jobDetails)
	return jobStatuses, readWrites, checkpoints, exceptions
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

func (j *Job) getJobDetails(flinkJobManagerUrl string, jobs []string) map[string]jobDetail {
	httpClient := util.HttpClient{}
	details := map[string]jobDetail{}
	details = make(map[string]jobDetail)

	// collect all metrics
	for _, job := range jobs {
		// --- detail ---------------------
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job
		jsonStr, err := httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			continue
		}

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			continue
		}

		// job name
		var jobName string
		jobName, err = js.Get("name").String()
		if err != nil {
			log.Errorf("js.Get 'name' = %v", err)
			continue
		}
		log.Debugf("jobName = %v", jobName)

		detail := jobDetail{}
		detail.id = job
		detail.name = jobName
		detail.detail = js

		// --- checkpoints ---------------------
		url = strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job + "/checkpoints"
		jsonStr, err = httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			continue
		}

		// parse when exists checkpoints
		if jsonStr != "{}" {
			js, err = simpleJson.NewJson([]byte(jsonStr))
			if err != nil {
				log.Errorf("simpleJson.NewJson = %v", err)
				continue
			}
			detail.checkPoints = js
		}

		// --- exceptions ---------------------
		url = strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job + "/exceptions"
		jsonStr, err = httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			continue
		}

		// parse
		js, err = simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			continue
		}
		detail.exceptions = js

		details[detail.id] = detail
	}
	log.Debugf("jobDetails = %v", details)

	return details
}

func (j *Job) getJobStatus(jobDetails map[string]jobDetail) []JobStatusMetrics {
	jobStatuses := []JobStatusMetrics{}
	for _, jobDetail := range jobDetails {
		// state
		var state string
		state, err := jobDetail.detail.Get("state").String()
		if err != nil {
			log.Errorf("js.Get 'state' = %v", err)
			return []JobStatusMetrics{}
		}
		log.Debugf("state = %v", state)

		jobStatus := JobStatusMetrics{}
		jobStatus.JobName = jobDetail.name

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

func (j *Job) getReadWrite(jobDetails map[string]jobDetail) ReadWriteTotalMertics {
	total := ReadWriteTotalMertics{}
	readWrites := []ReadWriteMertics{}
	for _, jobDetail := range jobDetails {
		// vertices
		var vertices []interface{}
		vertices, err := jobDetail.detail.Get("vertices").Array()
		if err != nil {
			log.Errorf("js.Get 'vertices' = %v", err)
			return ReadWriteTotalMertics{}
		}
		log.Debugf("vertices = %v", vertices)

		readWrite := ReadWriteMertics{}
		readWrite.JobName = jobDetail.name

		for _, verticeTmp := range vertices {
			if vertice, okVertice := verticeTmp.(map[string]interface{}); okVertice {
				if metricsTmp, foundMetrics := vertice["metrics"]; foundMetrics {
					if metrics, okMetrics := metricsTmp.(map[string]interface{}); okMetrics {
						record := ReadWriteMertics{}
						if name, foundName := vertice["name"]; foundName {
							if strings.HasPrefix(fmt.Sprint(name), "Source") {
								record.WriteBytes = j.getValueAsInt64(metrics, "write-bytes")
								record.WriteRecords = j.getValueAsInt64(metrics, "write-records")
								readWrite.WriteBytes += record.WriteBytes
								readWrite.WriteRecords += record.WriteRecords
							} else {
								record.ReadBytes = j.getValueAsInt64(metrics, "read-bytes")
								record.ReadRecords = j.getValueAsInt64(metrics, "read-records")
								readWrite.ReadBytes += record.ReadBytes
								readWrite.ReadRecords += record.ReadRecords
							}
						}
					}
				}
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

func (j *Job) getValueAsInt64(metrics map[string]interface{}, key string) int64 {
	if value, found := metrics[key]; found {
		converted, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil {
			return 0
		}
		return converted
	} else {
		return 0
	}
}

func (j *Job) getCheckpoints(jobDetails map[string]jobDetail) []CheckpointMetrics {
	checkpoints := []CheckpointMetrics{}
	for _, jobDetail := range jobDetails {
		checkpoint := CheckpointMetrics{}
		checkpoint.JobName = jobDetail.name
		if jobDetail.checkPoints != nil {
			// count
			var count int64
			count, err := jobDetail.checkPoints.Get("count").Int64()
			if err != nil {
				log.Errorf("js.Get 'count' = %v", err)
				return []CheckpointMetrics{}
			}
			log.Debugf("count = %v", count)

			checkpoint.Count = count

			// history
			var histories []interface{}
			histories, err = jobDetail.checkPoints.Get("history").Array()
			if err != nil {
				log.Errorf("js.Get 'history' = %v", err)
				return []CheckpointMetrics{}
			}
			log.Debugf("history = %v", histories)

			if len(histories) > 0 {
				if latest, ok := histories[len(histories)-1].(map[string]interface{}); ok {
					checkpoint.Duration = int(j.getValueAsInt64(latest, "duration"))
					checkpoint.Size = j.getValueAsInt64(latest, "size")
				} else {
					checkpoint.Duration = 0
					checkpoint.Size = 0
				}
			}
		}

		checkpoints = append(checkpoints, checkpoint)
	}

	log.Debugf("checkpoints = %v", checkpoints)

	return checkpoints
}

func (j *Job) getExceptions(jobDetails map[string]jobDetail) []ExceptionMetrics {
	exceptions := []ExceptionMetrics{}
	for _, jobDetail := range jobDetails {
		// exceptions
		var allExceptions []interface{}
		allExceptions, err := jobDetail.exceptions.Get("all-exceptions").Array()
		if err != nil {
			log.Errorf("js.Get 'all-exceptions' = %v", err)
			return []ExceptionMetrics{}
		}
		log.Debugf("allExceptions = %v", allExceptions)

		exceptions = append(exceptions,
			ExceptionMetrics{
				JobName: jobDetail.name,
				Count:   len(allExceptions),
			})
	}

	log.Debugf("exceptions = %v", exceptions)

	return exceptions
}
