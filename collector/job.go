package collector

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"github.com/matsumana/flink_exporter/util"
	"strconv"
	"strings"
)

type JobMetrics struct {
	JobStatusMetrics      []JobStatusMetrics
	ReadWriteTotalMertics ReadWriteTotalMertics
	CheckpointMetrics     []CheckpointMetrics
	ExceptionMetrics      []ExceptionMetrics
}
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

// see https://github.com/apache/flink/blob/release-1.1.1/flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobStatus.java
// TODO It's maybe need modify, After Flink version up.
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
	Suspended  int
	Unknown    int
}

type jobDetail struct {
	id          string
	name        string
	detail      *simpleJson.Json
	checkPoints *simpleJson.Json
	exceptions  *simpleJson.Json
}

type Job struct{}

func (j *Job) GetMetrics(flinkJobManagerUrl string) JobMetrics {
	jobs := j.getJobs(flinkJobManagerUrl)
	jobDetails := j.getJobDetails(flinkJobManagerUrl, jobs)
	jobStatuses, runningJobs := j.getJobStatus(jobDetails)
	readWrites := j.getReadWrite(jobDetails, runningJobs)
	checkpoints := j.getCheckpoints(jobDetails, runningJobs)
	exceptions := j.getExceptions(jobDetails, runningJobs)

	return JobMetrics{
		JobStatusMetrics:      jobStatuses,
		ReadWriteTotalMertics: readWrites,
		CheckpointMetrics:     checkpoints,
		ExceptionMetrics:      exceptions,
	}
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
	var allJobs []string

	if _, ok := js.CheckGet("jobs"); ok {
		for _, job := range js.Get("jobs").MustArray() {
			jss := job.(map[string]interface{})
			allJobs = append(allJobs, jss["id"].(string))
		}
	} else {
		var jobs []string
		// jobs-running
		jobs, err = js.Get("jobs-running").StringArray()
		if err != nil {
			log.Errorf("js.Get 'jobs-running' = %v", err)
			return []string{}
		}
		log.Debugf("jobs-running = %v", jobs)
		allJobs = append(allJobs, jobs...)

		// jobs-finished
		jobs, err = js.Get("jobs-finished").StringArray()
		if err != nil {
			log.Errorf("js.Get 'jobs-finished' = %v", err)
			return []string{}
		}
		log.Debugf("jobs-finished = %v", jobs)
		allJobs = append(allJobs, jobs...)

		// jobs-cancelled
		jobs, err = js.Get("jobs-cancelled").StringArray()
		if err != nil {
			log.Errorf("js.Get 'jobs-cancelled' = %v", err)
			return []string{}
		}
		log.Debugf("jobs-cancelled = %v", jobs)
		allJobs = append(allJobs, jobs...)

		// jobs-failed
		jobs, err = js.Get("jobs-failed").StringArray()
		if err != nil {
			log.Errorf("js.Get 'jobs-failed' = %v", err)
			return []string{}
		}
		log.Debugf("jobs-failed = %v", jobs)
		allJobs = append(allJobs, jobs...)
	}

	log.Debugf("allJobs = %v", allJobs)

	return allJobs
}

func (j *Job) getJobDetails(flinkJobManagerUrl string, jobs []string) map[string]jobDetail {

	log.Debugf("jobs = %v", jobs)

	httpClient := util.HttpClient{}
	channel := make(chan jobDetail)
	for _, job := range jobs {
		go func(job string) {
			// --- detail ---------------------
			url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job
			jsonStr, err := httpClient.Get(url)
			if err != nil {
				log.Errorf("HttpClient.Get = %v", err)
				channel <- jobDetail{}
				return
			}

			// parse
			js, err := simpleJson.NewJson([]byte(jsonStr))
			if err != nil {
				log.Errorf("simpleJson.NewJson = %v", err)
				channel <- jobDetail{}
				return
			}

			// job name
			var jobName string
			jobName, err = js.Get("name").String()
			if err != nil {
				log.Errorf("js.Get 'name' = %v", err)
				channel <- jobDetail{}
				return
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
				channel <- jobDetail{}
				return
			}

			// parse when exists checkpoints
			if jsonStr != "{}" {
				js, err = simpleJson.NewJson([]byte(jsonStr))
				if err != nil {
					log.Errorf("simpleJson.NewJson = %v", err)
					channel <- jobDetail{}
					return
				}
				detail.checkPoints = js
			}

			// --- exceptions ---------------------
			url = strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job + "/exceptions"
			jsonStr, err = httpClient.Get(url)
			if err != nil {
				log.Errorf("HttpClient.Get = %v", err)
				channel <- jobDetail{}
				return
			}

			// parse
			js, err = simpleJson.NewJson([]byte(jsonStr))
			if err != nil {
				log.Errorf("simpleJson.NewJson = %v", err)
				channel <- jobDetail{}
				return
			}
			detail.exceptions = js

			channel <- detail
		}(job)
	}

	// receive from all channels
	details := make(map[string]jobDetail)
	for i := 0; i < len(jobs); i++ {
		detail := <-channel
		details[detail.id] = detail
	}

	log.Debugf("jobDetails = %v", details)

	return details
}

func (j *Job) getJobStatus(jobDetails map[string]jobDetail) ([]JobStatusMetrics, map[string]string) {
	jobStatusMetrics := make(map[string]*JobStatusMetrics)
	for _, jd := range jobDetails {
		jobStatus := new(JobStatusMetrics)
		jobStatus.JobName = jd.name
		jobStatusMetrics[jobStatus.JobName] = jobStatus
	}

	runningJobs := make(map[string]string)
	for _, jd := range jobDetails {
		// state
		var state string
		state, err := jd.detail.Get("state").String()
		if err != nil {
			log.Errorf("js.Get 'state' = %v", err)
			return []JobStatusMetrics{}, make(map[string]string)
		}
		log.Debugf("state = %v", state)

		switch state {
		case "CREATED":
			jobStatusMetrics[jd.name].Created += 1
		case "RUNNING":
			jobStatusMetrics[jd.name].Running += 1

			runningJobs[jd.name] = state
		case "FAILING":
			jobStatusMetrics[jd.name].Failing += 1
		case "FAILED":
			jobStatusMetrics[jd.name].Failed += 1
		case "CANCELLING":
			jobStatusMetrics[jd.name].Cancelling += 1
		case "CANCELED":
			jobStatusMetrics[jd.name].Canceled += 1
		case "FINISHED":
			jobStatusMetrics[jd.name].Finished += 1
		case "RESTARTING":
			jobStatusMetrics[jd.name].Restarting += 1
		case "SUSPENDED":
			jobStatusMetrics[jd.name].Suspended += 1
		default:
			jobStatusMetrics[jd.name].Unknown += 1
		}
	}

	jobStatuses := []JobStatusMetrics{}
	for _, jsm := range jobStatusMetrics {
		jobStatuses = append(jobStatuses, *jsm)
	}

	log.Debugf("jobStatuses = %v", jobStatuses)

	return jobStatuses, runningJobs
}

func (j *Job) getReadWrite(jobDetails map[string]jobDetail, runningJobs map[string]string) ReadWriteTotalMertics {
	channel := make(chan ReadWriteMertics)
	for _, jd := range jobDetails {
		go func(jd jobDetail) {
			// vertices
			var vertices []interface{}
			vertices, err := jd.detail.Get("vertices").Array()
			if err != nil {
				log.Errorf("js.Get 'vertices' = %v", err)
				channel <- ReadWriteMertics{}
				return
			}
			log.Debugf("vertices = %v", vertices)

			readWrite := ReadWriteMertics{}
			readWrite.JobName = jd.name

			// collect metrics if status is running
			// collect as 0, if status is not running
			if _, ok := runningJobs[jd.name]; ok {
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
			}

			channel <- readWrite
		}(jd)
	}

	// receive from all channels
	total := ReadWriteTotalMertics{}
	readWrites := []ReadWriteMertics{}
	for i := 0; i < len(jobDetails); i++ {
		readWrite := <-channel
		total.ReadBytesTotal += readWrite.ReadBytes
		total.ReadRecordsTotal += readWrite.ReadRecords
		total.WriteBytesTotal += readWrite.WriteBytes
		total.WriteRecordsTotal += readWrite.WriteRecords
		readWrites = append(readWrites, readWrite)
	}

	total.Details = readWrites

	log.Debugf("readWrites = %v", total)

	return total
}

func (j *Job) getCheckpoints(jobDetails map[string]jobDetail, runningJobs map[string]string) []CheckpointMetrics {
	channel := make(chan CheckpointMetrics)
	for _, jd := range jobDetails {
		go func(jd jobDetail) {
			checkpoint := CheckpointMetrics{}
			checkpoint.JobName = jd.name

			// collect metrics if status is running
			// collect as 0, if status is not running
			if _, ok := runningJobs[jd.name]; ok {
				if jd.checkPoints != nil {
					// Check json format for v1.2 or later
					var err error
					if _, ok := jd.checkPoints.CheckGet("counts"); ok {
						checkpoint, err = j.checkpointMetrics(jd)
						if err != nil {
							log.Errorf("Failed Job.checkpointMetrics = %v", err)
							channel <- CheckpointMetrics{}
							return
						}
					} else {
						// Earlier v1.1
						checkpoint, err = j.checkpointMetricsAsV11(jd)
						if err != nil {
							log.Errorf("Failed Job.checkpointMetricsAsV11 = %v", err)
							channel <- CheckpointMetrics{}
							return
						}
					}
				}
			}

			channel <- checkpoint
		}(jd)
	}

	// receive from all channels
	checkpoints := []CheckpointMetrics{}
	for i := 0; i < len(jobDetails); i++ {
		checkpoint := <-channel
		checkpoints = append(checkpoints, checkpoint)
	}

	log.Debugf("checkpoints = %v", checkpoints)

	return checkpoints
}

func (j *Job) checkpointMetrics(jd jobDetail) (CheckpointMetrics, error) {
	checkpoint := CheckpointMetrics{JobName: jd.name, Count: 0, Size: 0, Duration: 0}

	count, err := jd.checkPoints.Get("counts").Get("total").Int64()
	if err != nil {
		log.Errorf("js.Get 'counts.total' = %v", err)
		return CheckpointMetrics{}, err
	}
	checkpoint.Count = count

	// latest history
	if latest, ok := jd.checkPoints.Get("latest").CheckGet("completed"); ok {
		var err error
		checkpoint.Duration, err = latest.Get("end_to_end_duration").Int()
		if err != nil {
			log.Errorf("js.Get 'end_to_end_duration' = %v", err)
		}

		checkpoint.Size, err = latest.Get("state_size").Int64()
		if err != nil {
			log.Errorf("js.Get 'state_size' = %v", err)
		}
	} else {
		log.Errorf("js.Get 'latest.completed' = %v", err)
	}

	return checkpoint, nil
}

func (j *Job) checkpointMetricsAsV11(jd jobDetail) (CheckpointMetrics, error) {
	checkpoint := CheckpointMetrics{JobName: jd.name, Count: 0, Size: 0, Duration: 0}

	// count
	count, err := jd.checkPoints.Get("count").Int64()
	if err != nil {
		log.Errorf("js.Get 'count' = %v", err)
		return CheckpointMetrics{}, err
	}

	log.Debugf("count = %v", count)
	checkpoint.Count = count

	// history
	var histories []interface{}
	histories, err = jd.checkPoints.Get("history").Array()
	if err != nil {
		log.Errorf("js.Get 'history' = %v", err)
		return CheckpointMetrics{}, err
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
	return checkpoint, nil
}

func (j *Job) getExceptions(jobDetails map[string]jobDetail, runningJobs map[string]string) []ExceptionMetrics {
	channel := make(chan []string)
	for _, jd := range jobDetails {
		go func(jd jobDetail) {
			// exceptions
			var allExceptions []string

			// collect metrics if status is running
			// collect as 0, if status is not running
			if _, ok := runningJobs[jd.name]; ok {
				allExceptions, err := jd.exceptions.Get("all-exceptions").StringArray()
				if err != nil {
					log.Errorf("js.Get 'all-exceptions' = %v", err)
					channel <- []string{}
					return
				}
				log.Debugf("allExceptions = %v", allExceptions)
			}

			channel <- allExceptions
		}(jd)
	}

	// receive from all channels
	exceptions := []ExceptionMetrics{}
	for _, jobDetail := range jobDetails {
		allExceptions := <-channel
		exceptions = append(exceptions,
			ExceptionMetrics{
				JobName: jobDetail.name,
				Count:   len(allExceptions),
			})
	}

	log.Debugf("exceptions = %v", exceptions)

	return exceptions
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
